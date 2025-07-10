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


import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.regenie.RegenieStep2WrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.*;

@Tool(id = RegenieStep2WrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = RegenieStep2WrapperAnalysis.DESCRIPTION)
public class RegenieStep2WrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "regenie-step2";
    public static final String DESCRIPTION = "Regenie is a program for whole genome regression modelling of large genome-wide association"
            + " studies. This performs the step2 of the regenie analysis.";

    private String walkerDockerImage;

    @ToolParams
    protected final RegenieStep2WrapperParams regenieParams = new RegenieStep2WrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        // Check input parameters
        String dockerName = checkRegenieInputParameter(regenieParams.getDocker().getName(), false, "Docker name");
        String dockerTag = checkRegenieInputParameter(regenieParams.getDocker().getTag(), false, "Docker tag");
        String dockerUsername = checkRegenieInputParameter(regenieParams.getDocker().getUsername(), true, "Docker Hub username");
        String dockerPassword = checkRegenieInputParameter(regenieParams.getDocker().getPassword(), true, "Docker Hub password (or"
                + " personal access token)");

        if (StringUtils.isNotEmpty(dockerName) && StringUtils.isNotEmpty(dockerTag)) {
            walkerDockerImage = dockerName + ":" + dockerTag;
            logger.info("Using regenie-walker image {} to perform regenie step2", walkerDockerImage);
        } else {
            throw new ToolException("Missing regenie-walker docker parameters (name, tag)");
        }

        // Check docker image
        boolean dockerImageAvailable = RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword);
        if (!dockerImageAvailable) {
            throw new ToolException("Regenie-walker docker image name " + walkerDockerImage + " not available.");
        }

        addAttribute("OPENCGA_REGENIE_STEP2_PARAMETERS", regenieParams);
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run regenie step2
        step(ID, this::runRegenieStep2);
    }

    protected void runRegenieStep2() throws CatalogException, StorageEngineException, ToolException, IOException {
        logger.info("Running regenie step2 with regenie-walker docker image: {} ...", walkerDockerImage);

        // Create REGENIE command line from the user parameters
        StringBuilder regenieCmd = new StringBuilder("python3 /opt/app/python/variant_walker.py regenie_walker Regenie");
        if (MapUtils.isNotEmpty(regenieParams.getRegenieParams())) {
            addRegenieOptions(regenieCmd);
        }

        VariantQuery variantQuery = new VariantQuery()
                .study(getStudy())
                .includeSampleAll()
                .includeSampleData("GT")
                .unknownGenotype("./.");

        Path resultsPath = getOutDir().resolve("tmp-results-regenie-step2.txt");
        variantStorageManager.walkData(resultsPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF,
                variantQuery, new QueryOptions(), walkerDockerImage, regenieCmd.toString(), getToken());

        if (!Files.exists(resultsPath)) {
            throw new ToolException("Regenie step2 failed: the results file was not created");
        }

        // Process regenie result file by removing the header and logs lines
        boolean write = false;
        Path regenieResultsPath = getOutDir().resolve(REGENIE_RESULTS_FILENAME);
        try (BufferedReader reader = new BufferedReader(new FileReader(resultsPath.toFile()));
             BufferedWriter writer = new BufferedWriter(new FileWriter(regenieResultsPath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(REGENIE_COMMENT_PREFIX)) {
                    // Skip comment lines and log these lines
                    logger.info(line);
                } else if (write && !line.startsWith(REGENIE_HEADER_PREFIX)) {
                    writer.write(line);
                    writer.newLine(); // Write a new line character after the header line
                } else if (!write && line.startsWith(REGENIE_HEADER_PREFIX)) {
                    write = true;
                    writer.write(line);
                    writer.newLine();
                }
            }
        }
        Files.delete(resultsPath);

        logger.info("Regenie step2 done!");
    }

    private void addRegenieOptions(StringBuilder regenieCmd) {
        ObjectMap options = regenieParams.getRegenieParams();
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            if (SKIP_OPTIONS.contains(entry.getKey())) {
                continue;
            }
            if (entry.getValue() == null) {
                regenieCmd.append(" ").append(entry.getKey());
            } else if (entry.getValue() instanceof String) {
                String value = entry.getValue().toString();
                if (value.startsWith(DOCKER_FILE_PREFIX)) {
                    regenieCmd.append(" ").append(entry.getKey()).append(" ").append(value.substring(DOCKER_FILE_PREFIX.length()));
                } else if (FLAG_TRUE.equalsIgnoreCase(value)) {
                    regenieCmd.append(" ").append(entry.getKey());
                } else if (!FLAG_FALSE.equalsIgnoreCase(value)) {
                    regenieCmd.append(" ").append(entry.getKey()).append(" ").append(value);
                }
            } else {
                regenieCmd.append(" ").append(entry.getKey()).append(" ").append(entry.getValue());
            }
        }
    }
}
