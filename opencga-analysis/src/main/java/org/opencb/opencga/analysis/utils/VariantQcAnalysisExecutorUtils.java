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

package org.opencb.opencga.analysis.utils;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolExecutorException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis.VARIANT_QC_FOLDER;
import static org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis.RESOURCES_FOLDER;

public class VariantQcAnalysisExecutorUtils {

    public static String CONFIG_FILENAME = "config.json";
    public static String QC_JSON_EXTENSION = ".qc.json";

    private static String SCRIPT_VIRTUAL_FOLDER = "/script";
    private static String JOB_VIRTUAL_FOLDER = "/jobdir";

    public static void run(String qcType, LinkedList<Path> vcfPaths, LinkedList<Path> jsonPaths, Path configPath, Path scriptPath,
                           Path outDir, String dockerImage) throws ToolExecutorException {
        // Run the Python script responsible for performing the family QC analyses
        //   variant_qc.main.py --vcf-file xxx --info-json xxx --bam-file xxx --qc-type xxx --config xxx --resource-dir xxx --output-dir xxx

        // Build command line to run Python script via docker image
        try {
            // Input binding
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), SCRIPT_VIRTUAL_FOLDER));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                    JOB_VIRTUAL_FOLDER);

            String params = "python3 " + SCRIPT_VIRTUAL_FOLDER + "/variant_qc.main.py"
                    + " --vcf-file " + StringUtils.join(vcfPaths.stream().map(p -> p.toAbsolutePath().toString().replace(
                    outDir.toAbsolutePath().toString(), JOB_VIRTUAL_FOLDER)).collect(Collectors.toList()), ",")
                    + " --info-json " + StringUtils.join(jsonPaths.stream().map(p -> p.toAbsolutePath().toString().replace(
                    outDir.toAbsolutePath().toString(), JOB_VIRTUAL_FOLDER)).collect(Collectors.toList()), ",")
                    + " --config " + Paths.get(JOB_VIRTUAL_FOLDER).resolve(configPath.getFileName())
                    + " --resource-dir " + Paths.get(JOB_VIRTUAL_FOLDER).resolve(RESOURCES_FOLDER)
                    + " --output-dir " + JOB_VIRTUAL_FOLDER
                    + " " + qcType;

            // Execute Pythong script in docker
            DockerUtils.run(dockerImage, inputBindings, outputBinding, params, null);
        } catch (IOException e) {
            throw new ToolExecutorException(e);
        }
    }
}
