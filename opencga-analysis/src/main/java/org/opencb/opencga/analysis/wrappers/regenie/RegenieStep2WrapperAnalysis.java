/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.wrappers.regenie;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.variant.RegenieStep2WrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.opencb.opencga.core.api.FieldConstants.REGENIE_STEP1;
import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = RegenieStep2WrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = RegenieStep2WrapperAnalysis.DESCRIPTION)
public class RegenieStep2WrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "regenie-step2";
    public static final String DESCRIPTION = "Regenie is a program for whole genome regression modelling of large genome-wide association"
            + " studies. This performs the step1 of the regenie analysis.";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";

    private Path phenoFile;
    private Path covarFile;
    private Path predPath;

    private Path resourcePath;

    @ToolParams
    protected final RegenieStep2WrapperParams analysisParams = new RegenieStep2WrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check pheno file
        if (StringUtils.isEmpty(analysisParams.getPhenoFile())) {
            throw new ToolException("Missing phenotype file.");
        }
        File opencgaPhenoFile = getCatalogManager().getFileManager().get(study, analysisParams.getPhenoFile(), QueryOptions.empty(), token)
                .first();
        phenoFile = Paths.get(opencgaPhenoFile.getUri().getPath()).toAbsolutePath();
        if (!Files.exists(phenoFile)) {
            throw new ToolException("Phenotype file does not exit: " + phenoFile);
        }

        // Check covar file
        if (StringUtils.isNotEmpty(analysisParams.getCovarFile())) {
            File opencgaCovarFile = getCatalogManager().getFileManager().get(study, analysisParams.getCovarFile(), QueryOptions.empty(),
                    token).first();
            covarFile = Paths.get(opencgaCovarFile.getUri().getPath()).toAbsolutePath();
            if (!Files.exists(covarFile)) {
                throw new ToolException("Covariate file does not exit: " + covarFile);
            }
        }

        // Check pred path
        if (StringUtils.isEmpty(analysisParams.getPredPath())) {
            throw new ToolException("Missing prediction path generated in the " + REGENIE_STEP1 + " of regenie.");
        }
        File opencgaPredFile = getCatalogManager().getFileManager().get(study, analysisParams.getPredPath(), QueryOptions.empty(), token)
                .first();
        predPath = Paths.get(opencgaPredFile.getUri().getPath()).toAbsolutePath();
        if (!Files.exists(predPath)) {
            throw new ToolException("Prediction path does not exit: " + predPath);
        }

        // Check username and paswword
        if (StringUtils.isEmpty(analysisParams.getDockerUsername())) {
            throw new ToolException("Missing Docker Hub username.");
        }
        if (StringUtils.isEmpty(analysisParams.getDockerPassword())) {
            throw new ToolException("Missing Docker Hub password.");
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, ID, CLEAN_RESOURCES_STEP);
    }

    protected void run() throws ToolException, IOException {
        // Prepare regenie resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Run regenie
        step(ID, this::runRegenieStep2);

        // Do we have to clean the regenie resource folder
        step(CLEAN_RESOURCES_STEP, this::cleanResources);
    }

    protected void prepareResources() throws IOException {
        // Create folder where the regenie resources will be saved (within the job dir, aka outdir)
        resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_DIRNAME));

        // Copy files
        FileUtils.copyFile(phenoFile.toFile(), resourcePath.resolve(phenoFile.getFileName()).toFile());
        if (covarFile != null) {
            FileUtils.copyFile(covarFile.toFile(), resourcePath.resolve(covarFile.getFileName()).toFile());
        }
        Path step1Path = Files.createDirectories(resourcePath.resolve(REGENIE_STEP1));
        for (java.io.File file : predPath.toFile().listFiles()) {
            FileUtils.copyFile(file, step1Path.resolve(file.getName()).toFile());
        }
    }

    private void cleanResources() throws IOException {
        deleteDirectory(resourcePath.toFile());
    }

    protected void runRegenieStep2() throws ToolException, CatalogException, StorageEngineException {
        logger.info("Running regenie with parameters {}...", analysisParams);

        String dockerImage = buildDocker();

        String regenieCmd = "python3 variant_walker.py regenie_walker Regenie";
        Path regenieResults = getOutDir().resolve("regenie_results.txt");
        VariantQuery variantQuery = new VariantQuery()
                .study(getStudy())
                .includeSampleAll()
                .includeSampleData("GT")
                .unknownGenotype("./.");

        variantStorageManager.walkData(regenieResults.toString(), VariantWriterFactory.VariantOutputFormat.VCF, variantQuery,
                new QueryOptions(), dockerImage, regenieCmd, getToken());

        logger.info("Regenie done!");
    }

    public static void deleteDirectory(java.io.File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        // If it's a directory, delete contents recursively
        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) { // Not null if directory is not empty
                for (java.io.File file : files) {
                    // Recursively delete subdirectories and files
                    deleteDirectory(file);
                }
            }
        }

        // Finally, delete the directory or file itself
        Files.delete(directory.toPath());
    }

    private String buildDocker() throws ToolException {
        String dockerRepo = "regenie-walker";
        String dockerRepoVersion = Instant.now().getEpochSecond() + "-" + (new Random().nextInt(9000) + 1000);

        Path dockerBuildScript = getOpencgaHome().resolve("cloud/docker/opencga-regenie/regenie-docker-build.py");
        Path pythonUtilsPath = getOpencgaHome().resolve("cloud/docker/walker");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--step1-path", predPath.toAbsolutePath().toString(),
                "--python-path", pythonUtilsPath.toAbsolutePath().toString(),
                "--pheno-file", phenoFile.toAbsolutePath().toString(),
                "--source-image-name", "joaquintarraga/opencga-regenie:" + GitRepositoryState.getInstance().getBuildVersion(),
                "--output-dockerfile", resourcePath.resolve("Dockerfile").toString(),
                "--docker-repo", dockerRepo,
                "--docker-repo-version", dockerRepoVersion,
                "--docker-username", analysisParams.getDockerUsername(),
                "--docker-password", analysisParams.getDockerPassword()
        }, Collections.emptyMap());

        logger.info("Building and pushing the regenie docker image: {}", dockerBuild.getCommandLine()
                .replace(analysisParams.getDockerPassword(), "XXXXX"));

        dockerBuild.run();
        if (dockerBuild.getExitValue() != 0) {
            throw new ToolException("Error building and pushing the regenie docker image");
        }

        return analysisParams.getDockerUsername() + "/" + dockerRepo + ":" + dockerRepoVersion;
    }
}
