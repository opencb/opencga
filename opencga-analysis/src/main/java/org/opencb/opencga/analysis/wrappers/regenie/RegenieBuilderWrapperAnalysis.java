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


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.regenie.RegenieBuilderWrapperParams;
import org.opencb.opencga.core.models.variant.regenie.RegenieStep1WrapperParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor.*;
import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.*;
import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = RegenieBuilderWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = RegenieBuilderWrapperAnalysis.DESCRIPTION)
public class RegenieBuilderWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "regenie-builder";
    public static final String DESCRIPTION = "Regenie is a program for whole genome regression modelling of large genome-wide association"
            + " studies. The tool regenie-builder creates the regenie-walker docker with the files from step1, to execute the step2 of the"
            + " regenie analysis in the OpenCGA framework.";


    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";
    private static final String PREPARE_WALKER_DOCKER_STEP = "build-and-push-walker-docker";

    private String dockerName = null;
    private String dockerTag = null;
    private String dockerUsername = null;
    private String dockerPassword = null;

    private String dockerBasename;

    private Path resourcePath;

    @ToolParams
    protected final RegenieBuilderWrapperParams regenieParams = new RegenieBuilderWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        // Check regenie file options
        checkRegenieFileOptions(regenieParams.getRegenieFileOptions());

        // Check doker parameters (name, username and password)
        checkDockerParameters();

        addAttribute("OPENCGA_REGENIE_BUILDER_PARAMETERS", regenieParams);
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, PREPARE_WALKER_DOCKER_STEP, CLEAN_RESOURCES_STEP);
    }

    protected void run() throws ToolException, IOException {
        // Prepare regenie resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Do we have to clean the regenie resource folder
        step(PREPARE_WALKER_DOCKER_STEP, this::prepareWalkerDocker);

        // Do we have to clean the regenie resource folder
        step(CLEAN_RESOURCES_STEP, this::cleanResources);
    }

    private void prepareResources() throws ToolException {
        // Create folder where the regenie resources will be saved (within the job dir, aka outdir)
        try {
            resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_DIRNAME));

            ObjectMap fileOptions = regenieParams.getRegenieFileOptions();

            // Copy all files to the resources; and add other options
            for (String key : fileOptions.keySet()) {
                if (ALL_FILE_OPTIONS.contains(key)) {
                    String value = fileOptions.getString(key);

                    if (key.equals(BGEN_OPTION) || key.equals(BED_OPTION) || key.equals(PGEN_OPTION)) {
                        // Ignore these options since it is used in step1 of the Regenie analysis
                        logger.info("Ignoring file option {}: it is must be used in step1 of the Regenie analysis", key);
                        continue;
                    }

                    if (key.equals(PRED_OPTION)) {
                        // Prepared prediction files that were generated from the step1 of the regenie analysis
                        Path path = checkRegenieInputFile(value, true, "Option " + key + " file ", study, catalogManager, token);
                        preparePredictionFiles(path);
                        continue;
                    }

                    Path path = checkRegenieInputFile(value, false, "Option " + key + " file ", study, catalogManager, token);
                    if (path != null) {
                        Path dest = resourcePath.resolve(path.getFileName());
                        FileUtils.copyFile(path, dest);
                        if (!Files.exists(dest)) {
                            throw new ToolException("File " + dest + " not found after copy (preparing resources)");
                        }
                    }
                } else {
                    // Discard not file options, they will not add in the regenie-walker docker
                    logger.info("Discarding option {} with value {}: only files are added in the regenie-walker docker", key,
                            fileOptions.get(key));
                }
            }
        } catch (ToolException | IOException e) {
            clean();
            throw new ToolException(e);
        }
    }

    private void preparePredictionFiles(Path predListFile) throws ToolException, IOException {
        // File containing predictions from Step 1. This is required for --step 2
        // Copy step1 results (i.e., prediction files) and update the paths within the file step1_pred.list
        Path predDir = resourcePath.resolve("pred");
        if (!Files.exists(Files.createDirectories(predDir))) {
            throw new ToolException("Could not create directory " + predDir);
        }
        Path updatedPredListFile = predDir.resolve(predListFile.getFileName());
        List<String> lines = Files.readAllLines(predListFile);
        try (BufferedWriter bw = FileUtils.newBufferedWriter(predDir.resolve(updatedPredListFile))) {
            for (String line : lines) {
                // Split the line by whitespace or tab
                String[] split = line.split("[ \t]");
                String locoFullFilename = split[1];
                logger.info("Processing regenie-step1 loco file: {}", locoFullFilename);
                String locoFilename = Paths.get(locoFullFilename).getFileName().toString();

                Path locoPath;
                if (locoFullFilename.startsWith(FILE_PREFIX)) {
                    locoPath = checkRegenieInputFile(locoFullFilename.substring(FILE_PREFIX.length()), true, "Regeine-step1 loco file ("
                            + locoFilename + ")", study, catalogManager, token);
                } else {
                    locoPath = Paths.get(locoFullFilename);
                    if (!Files.exists(locoPath)) {
                        locoPath = predListFile.getParent().resolve(locoFullFilename);
                        if (!Files.exists(locoPath)) {
                            throw new ToolExecutorException("Could not find the regenie-step1 loco file: " + locoFullFilename);
                        }
                    }
                }
                if (locoPath == null) {
                    throw new ToolExecutorException("Could not find the regenie-step1 loco file: " + locoFullFilename);
                }
                FileUtils.copyFile(locoPath, predDir.resolve(locoPath.getFileName()));
                bw.write(split[0] + " " + OPT_APP_PRED_VIRTUAL_DIR + locoPath.getFileName() + "\n");
            }
        }
    }

    private void cleanResources() throws IOException {
//        FileUtils.deleteDirectory(resourcePath);
    }

    private void checkRegenieFileOptions(ObjectMap inputOptions) throws ToolException {
        if (MapUtils.isEmpty(inputOptions)) {
            throw new ToolException("Regenie file options are mandatory.");
        }

        // Mandatory file options
        List<String> mandatoryFileOptions = Arrays.asList(PRED_OPTION, PHENO_FILE_OPTION);

        // Check other file options
        for (String key : inputOptions.keySet()) {
            if (ALL_FILE_OPTIONS.contains(key)) {
                if (key.equals(BGEN_OPTION) || key.equals(BED_OPTION) || key.equals(PGEN_OPTION)) {
                    // Ignore these options since it is used in step1 of the Regenie analysis
                    logger.info("Ignoring file option {}: it is must be used in step1 of the Regenie analysis", key);
                    continue;
                }

                String value = inputOptions.getString(key);
                if (StringUtils.isEmpty(value)) {
                    throw new ToolException(key + " is a file option, so its value must not be empty");
                }
                if (!value.startsWith(FILE_PREFIX)) {
                    throw new ToolException(key + " is a file option, so its value must start with " + FILE_PREFIX
                            + ". Current value: " + value);
                }
                value = value.substring(FILE_PREFIX.length());
                checkRegenieInputFile(value, mandatoryFileOptions.contains(key), key, study, catalogManager, token);
            }
        }
    }

    private void checkDockerParameters() throws ToolException {
        if (regenieParams.getDocker() == null) {
            throw new ToolException("Docker parameters are mandatory");
        }

        dockerName = checkRegenieInputParameter(regenieParams.getDocker().getName(), true, "Docker name");
        dockerTag = checkRegenieInputParameter(regenieParams.getDocker().getTag(), true, "Docker tag");
        dockerUsername = checkRegenieInputParameter(regenieParams.getDocker().getUsername(), true, "Docker Hub username");
        dockerPassword = checkRegenieInputParameter(regenieParams.getDocker().getPassword(), true, "Docker Hub password (or personal"
                + " access token)");

        if (!dockerName.contains("/")) {
            throw new ToolException("Invalid docker name " + dockerName + ", please, provide: namespace/repository");
        }

        // Get docker base name
        dockerBasename = configuration.getAnalysis().getOpencgaExtTools();
        if (StringUtils.isEmpty(dockerBasename)) {
            throw new ToolException("Docker base name is not set, please, check your configuration file");
        }
        if (!dockerBasename.contains(":")) {
            dockerBasename += ":" + GitRepositoryState.getInstance().getBuildVersion();
        }
    }

    private void prepareWalkerDocker() throws ToolException {
        Map<String, Object> params = new HashMap<>(regenieParams.toParams());
        logger.info("Building and pushing regenie-walker docker with parameters {} ...", params);

        try {
            // Prepare regenie-step1 results to be included in the docker image
            Path dataDir = getOutDir().resolve(ID);
            if (!Files.exists(Files.createDirectories(dataDir))) {
                throw new ToolException("Could not create directory " + dataDir);
            }

            // Copy all regenie files (e.g. pheno and covar files)
            FileUtils.copyDirectory(resourcePath, dataDir);

            // Copy Python scripts and files
            copyPythonFiles(dataDir, getOpencgaHome());

            // Create and push regenie-walker
            logger.info("Building and pushing regenie-walker ...");
            String walkerDocker = buildAndPushDocker(dataDir, dockerBasename, dockerName, dockerTag, dockerUsername, dockerPassword,
                    getOpencgaHome());

            logger.info("Regenie-walker docker image: {}", walkerDocker);
            addAttribute(OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY, walkerDocker);
            logger.info("Building and pushing regenie-walker docker done!");

            // Clean up
            FileUtils.deleteDirectory(dataDir);
        } catch (ToolException | IOException e) {
            clean();
            throw new ToolException(e);
        }
    }

    private void clean() {
//        List<Path> paths = Arrays.asList(getOutDir().resolve(ID), getOutDir().resolve(RESOURCES_DIRNAME));
//        for (Path path : paths) {
//            if (Files.exists(path)) {
//                try {
//                    logger.info("Cleaning after error: deleting directory {}", path);
//                    FileUtils.deleteDirectory(path);
//                } catch (IOException e) {
//                    logger.error("Error deleting directory {}: {}", path, e.getMessage(), e);
//                }
//            }
//        }
    }
}
