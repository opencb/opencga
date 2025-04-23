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


import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.RegenieStep1WrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.analysis.AnalysisUtils.XXXXXX;
import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.*;
import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = RegenieStep1WrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = RegenieStep1WrapperAnalysis.DESCRIPTION)
public class RegenieStep1WrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "regenie-step1";
    public static final String DESCRIPTION = "Regenie is a program for whole genome regression modelling of large genome-wide association"
            + " studies. This performs the step1 of the regenie analysis.";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";
    private static final String BUILD_AND_PUSH_WALKER_DOCKER_STEP = "build-and-push-walker-docker";

    private Path vcfFile;
    private Path phenoFile;
    private Path covarFile;

    private Path resourcePath;

    @ToolParams
    protected final RegenieStep1WrapperParams analysisParams = new RegenieStep1WrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check pheno and covar files
        phenoFile = checkRegenieInputFile(analysisParams.getPhenoFile(), true, "Phenotype", getStudy(), getCatalogManager(), getToken());
        covarFile = checkRegenieInputFile(analysisParams.getCovarFile(), false, "Covariate", getStudy(), getCatalogManager(), getToken());

        // Check username and password
        checkRegenieInputParameter(analysisParams.getDockerNamespace(), true, "Docker namespace");
        checkRegenieInputParameter(analysisParams.getDockerUsername(), true, "Docker username");
        checkRegenieInputParameter(analysisParams.getDockerPassword(), true, "Docker password");
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, ID, BUILD_AND_PUSH_WALKER_DOCKER_STEP, CLEAN_RESOURCES_STEP);
    }

    protected void run() throws ToolException, IOException {
        // Prepare regenie resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Run regenie
        step(ID, this::runRegenieStep1);

        // Do we have to clean the regenie resource folder
        step(BUILD_AND_PUSH_WALKER_DOCKER_STEP, this::buildAndPushWalkerDocker);

        // Do we have to clean the regenie resource folder
        step(CLEAN_RESOURCES_STEP, this::cleanResources);
    }

    protected void prepareResources() throws ToolException {
        // Create folder where the regenie resources will be saved (within the job dir, aka outdir)
        try {
            resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_DIRNAME));

            // Copy files
            FileUtils.copyFile(phenoFile, resourcePath.resolve(PHENO_FILENAME));
            if (covarFile != null) {
                FileUtils.copyFile(covarFile, resourcePath.resolve(COVAR_FILENAME));
            }

            // Export variants
            vcfFile = resourcePath.resolve(VCF_FILENAME);
            VariantQuery variantQuery = new VariantQuery()
                    .study(getStudy())
                    .includeSampleAll()
                    .includeSampleData("GT,FT")
                    .unknownGenotype("./.");
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,studies.samples");
            getVariantStorageManager().exportData(vcfFile.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, variantQuery,
                    queryOptions, getToken());
            if (!Files.exists(vcfFile)) {
                throw new ToolException("Exported VCF file not found");
            }
        } catch (IOException | CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }
    }

    private void cleanResources() throws IOException {
        FileUtils.deleteDirectory(resourcePath);
    }

    protected void runRegenieStep1() throws ToolException {
        // Get executor
        RegenieStep1WrapperAnalysisExecutor executor = getToolExecutor(RegenieStep1WrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setStep1ScriptPath(getOpencgaHome().resolve("cloud/docker/opencga-regenie/regenie_step1.sh"))
                .setInputPath(resourcePath)
                .setOutputPath(getOutDir())
                .execute();

        logger.info("Regenie step1 done!");
    }

    private void buildAndPushWalkerDocker() throws IOException, ToolException {
        Map<String, Object> params = new HashMap<>(analysisParams.toParams());
        params.put("dockerPassword", XXXXXX);
        logger.info("Building and pushing regenie-walker docker with parameters {} ...", params);

        // Prepare regenie-step1 results to be included in the docker image
        Path dataDir = getOutDir().resolve(ID);
        if (!Files.exists(Files.createDirectories(dataDir))) {
            throw new ToolException("Could not create directory " + dataDir);
        }
        // Copy pheno and covar files
        FileUtils.copyFile(phenoFile.toFile(), dataDir.resolve(PHENO_FILENAME).toFile());
        if (covarFile != null) {
            FileUtils.copyFile(covarFile.toFile(), dataDir.resolve(COVAR_FILENAME).toFile());
        }
        // Copy Python scripts and files
        Path pythonDir = dataDir.resolve("python");
        List<String> filenames = Arrays.asList("requirements.txt", "variant_walker.py");
        for (String filename : filenames) {
            FileUtils.copyFile(getOpencgaHome().resolve("cloud/docker/walker/" + filename).toAbsolutePath(), pythonDir.resolve(filename));
        }
        FileUtils.copyFile(getOpencgaHome().resolve("cloud/docker/opencga-regenie/regenie_walker.py"),
                pythonDir.resolve("regenie_walker.py"));
        // Copy step1 results (i.e., prediction files) and update the paths within the file step1_pred.list
        Path predDir = dataDir.resolve("pred");
        if (!Files.exists(Files.createDirectories(predDir))) {
            throw new ToolException("Could not create directory " + predDir);
        }
        Path step1PredPath = getOutDir().resolve(STEP1_PRED_LIST_FILNEMANE);
        if (!Files.exists(step1PredPath)) {
            throw new ToolException("Could not find the regenie-step1 predictions file: " + STEP1_PRED_LIST_FILNEMANE);
        }
        FileUtils.copyFile(getOutDir().resolve(STEP1_PRED_LIST_FILNEMANE), predDir.resolve(STEP1_PRED_LIST_FILNEMANE));
        List<String> lines = Files.readAllLines(step1PredPath);
        try (BufferedWriter bw = FileUtils.newBufferedWriter(predDir.resolve(STEP1_PRED_LIST_FILNEMANE))) {
            for (String line : lines) {
                String[] split = line.split(" ");
                String locoFilename = Paths.get(split[1]).getFileName().toString();
                Path locoPath = getOutDir().resolve(locoFilename);
                if (!Files.exists(locoPath)) {
                    throw new ToolExecutorException("Could not find the regenie-step1 loco file: " + locoPath.getFileName());
                }
                FileUtils.copyFile(locoPath, predDir.resolve(locoFilename));
                bw.write(split[0] + " " + OPT_APP_PRED_VIRTUAL_DIR + "/" + locoFilename + "\n");
            }
        }

        String walkerDocker = buildAndPushDocker(dataDir, analysisParams.getDockerNamespace(), analysisParams.getDockerUsername(),
                analysisParams.getDockerPassword(), getOpencgaHome());

        logger.info("Regenie-walker docker image: {}", walkerDocker);
        addAttribute(OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY, walkerDocker);
        logger.info("Building and pushing regenie-walker docker done!");

        // Clean up
        FileUtils.deleteDirectory(dataDir);
    }
}
