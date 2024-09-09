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

import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang.StringUtils;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.variant.FamilyQcAnalysisParams;
import org.opencb.opencga.core.tools.ToolParams;

import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class VariantQcAnalysisExecutorUtils {

    public static String CONFIG_FILENAME = "config.json";
    public static String QC_JSON_EXTENSION = ".qc.json";

    public static void run(LinkedList<Path> vcfPaths, LinkedList<Path> jsonPaths, Path configPath, Path outDir, Path opencgaHome)
            throws ToolExecutorException {
        // Run the Python script responsible for performing the family QC analyses
        //    variant_qc.main.py --vcf-file xxx --info-json xxx --bam-file xxx --qc-type xxx --config xxx --output-dir xxx

        // Build command line to run Python script via docker image

        try {
            // Input binding
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(opencgaHome.resolve("analysis/variant-qc").toAbsolutePath().toString(),
                    "/script"));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                    "/jobdir");

            String params = "python3 /script/variant_qc.main.py"
                    + " --vcf-file " + StringUtils.join(vcfPaths.stream().map(p -> p.toAbsolutePath().toString().replace(
                    outDir.toAbsolutePath().toString(), "/jobdir")).collect(Collectors.toList()), ",")
                    + " --info-json " + StringUtils.join(jsonPaths.stream().map(p -> p.toAbsolutePath().toString().replace(
                    outDir.toAbsolutePath().toString(), "/jobdir")).collect(Collectors.toList()), ",")
                    + " --qc-type family"
                    + " --config /jobdir/" + configPath.getFileName()
                    + " --output-dir /jobdir";


            // Execute Pythong script in docker
            String dockerImage = "opencb/opencga-ext-tools:" + GitRepositoryState.getInstance().getBuildVersion();

            DockerUtils.run(dockerImage, inputBindings, outputBinding, params, null);
        } catch (IOException e) {
            throw new ToolExecutorException(e);
        }
    }
}
