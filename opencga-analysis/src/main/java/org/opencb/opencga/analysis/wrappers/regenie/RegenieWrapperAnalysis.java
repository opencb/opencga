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
import org.opencb.opencga.core.models.variant.RegenieWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.core.api.FieldConstants.REGENIE_STEP1;
import static org.opencb.opencga.core.api.FieldConstants.REGENIE_STEP2;
import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = RegenieWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = RegenieWrapperAnalysis.DESCRIPTION)
public class RegenieWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "regenie";
    public static final String DESCRIPTION = "Program for whole genome regression modelling of large genome-wide association studies.";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";

    private Path phenoFile;
    private Path covarFile;
    private Path predPath;

    private Path resourcePath;

    @ToolParams
    protected final RegenieWrapperParams analysisParams = new RegenieWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(analysisParams.getStep())) {
            throw new ToolException("Regenie 'step' parameter is mandatory.");
        }

        if (!REGENIE_STEP2.equals(analysisParams.getStep())) {
            throw new ToolException("Unsupportyed the regenie step " + analysisParams.getStep() + ". Valid step: " + REGENIE_STEP2);
        }

        if (StringUtils.isNotEmpty(analysisParams.getWalkerDockerName())) {
            // No need to check anything else
            return;
        }

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
    }

    @Override
    protected List<String> getSteps() {
        if (StringUtils.isNotEmpty(analysisParams.getWalkerDockerName())) {
            return Arrays.asList(ID);
        } else {
            return Arrays.asList(PREPARE_RESOURCES_STEP, ID, CLEAN_RESOURCES_STEP);
        }
    }

    protected void run() throws ToolException, IOException {
        if (StringUtils.isEmpty(analysisParams.getWalkerDockerName())) {
            // Prepare regenie resource files in the job dir
            step(PREPARE_RESOURCES_STEP, this::prepareResources);
        }

        // Run regenie
        step(ID, this::runRegenie);

        if (StringUtils.isEmpty(analysisParams.getWalkerDockerName())) {
            // Do we have to clean the regenie resource folder
            step(CLEAN_RESOURCES_STEP, this::cleanResources);
        }
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

    protected void runRegenie() throws ToolException, CatalogException, StorageEngineException {
        logger.info("Running regenie with parameters {}...", analysisParams);

        String dockerImage = analysisParams.getWalkerDockerName();
        if (StringUtils.isEmpty(dockerImage)) {
            dockerImage = buildDocker();
        }

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
        String dockerImage = "local/regenie-walker:latest";

        Path dockerBuildScript = getOpencgaHome().resolve("cloud/docker/opencga-regenie/regenie-docker-build.py");
        Path pythonUtilsPath = getOpencgaHome().resolve("cloud/docker/walker");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--image-name", dockerImage,
                "--step1-path", predPath.toAbsolutePath().toString(),
                "--python-path", pythonUtilsPath.toAbsolutePath().toString(),
                "--pheno-file", phenoFile.toAbsolutePath().toString(),
                "--source-image-name", "opencb/opencga-regenie:" + GitRepositoryState.getInstance().getBuildVersion(),
                "--output-dockerfile", resourcePath.resolve("Dockerfile").toString()
        }, Collections.emptyMap());

        logger.info("Building the regenie docker image: {}", dockerBuild.getCommandLine());

        dockerBuild.run();
        if (dockerBuild.getExitValue() != 0) {
            throw new ToolException("Error building the regenie docker image");
        }
        return dockerImage;
    }
}
