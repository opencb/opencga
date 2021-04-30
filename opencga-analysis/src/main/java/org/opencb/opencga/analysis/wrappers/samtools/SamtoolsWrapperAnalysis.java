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

package org.opencb.opencga.analysis.wrappers.samtools;

import com.google.common.base.CaseFormat;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.SamtoolsWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMANDS_SUPPORTED;
import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMAND_DESCRIPTION;

@Tool(id = SamtoolsWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = SamtoolsWrapperAnalysis.DESCRIPTION)
public class SamtoolsWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "samtools";
    public final static String DESCRIPTION = "Samtools is a program for interacting with high-throughput sequencing data in SAM, BAM"
            + " and CRAM formats. " + SAMTOOLS_COMMAND_DESCRIPTION;

    @ToolParams
    protected final SamtoolsWrapperParams analysisParams = new SamtoolsWrapperParams();

    private String inputFilePath = null;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(analysisParams.getCommand())) {
            throw new ToolException("Missing samtools command.");
        }

        if (!AnalysisUtils.isSupportedCommand(SAMTOOLS_COMMANDS_SUPPORTED)) {
            throw new ToolException("Samtools command '" + analysisParams.getCommand() + "' is not available. Supported commands are "
                    + SAMTOOLS_COMMANDS_SUPPORTED);
        }

        // Get files from catalog
        FileManager fileManager = catalogManager.getFileManager();
        if (StringUtils.isNotEmpty(analysisParams.getInputFile())) {
            inputFilePath = AnalysisUtils.getCatalogFile(analysisParams.getInputFile(), study, fileManager, token).getUri().getPath();
        }

        if (MapUtils.isNotEmpty(analysisParams.getSamtoolsParams())) {
            Set<String> fileParams = getFileParamNames(analysisParams.getCommand());

            Map<String, String> updatedMap = new HashMap<>();
            for (Map.Entry<String, String> entry : analysisParams.getSamtoolsParams().entrySet()) {
                if (fileParams.contains(entry.getKey())) {
                    updatedMap.put(entry.getKey(), AnalysisUtils.getCatalogFile(entry.getValue(), study, fileManager, token)
                            .getUri().getPath());
                }
            }
            if (MapUtils.isNotEmpty(updatedMap)) {
                analysisParams.getSamtoolsParams().putAll(updatedMap);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            if (MapUtils.isNotEmpty(analysisParams.getSamtoolsParams())) {
                executorParams.appendAll(analysisParams.getSamtoolsParams());
            }

            getToolExecutor(SamtoolsWrapperAnalysisExecutor.class)
                    .setCommand(analysisParams.getCommand())
                    .setInputFile(inputFilePath)
                    .execute();

            // Index management
            switch (analysisParams.getCommand()) {
                case "index": {
                    String indexFilePath;
                    if (executorParams.containsKey("c")) {
                        indexFilePath = inputFilePath + ".csi";
                    } else {
                        indexFilePath = inputFilePath + ".bai";
                    }
                    if (new java.io.File(indexFilePath).exists()) {
                        // Create symobolic link
                        Path target = Paths.get(indexFilePath);
                        Path symbolic = getOutDir().resolve(target.getFileName().toString());
                        Files.createSymbolicLink(symbolic, target);
                    }
                    break;
                }
                case "faidx": {
                    String indexFilePath = inputFilePath + ".fai";
                    if (new java.io.File(indexFilePath).exists()) {
                        // Create symobolic link
                        Path target = Paths.get(indexFilePath);
                        Path symbolic = getOutDir().resolve(target.getFileName().toString());
                        Files.createSymbolicLink(symbolic, target);
                    }
                    break;
                }
            }
        });
    }


    public static SamtoolsStats parseSamtoolsStats(java.io.File file, String fileId) throws IOException {
        // Create a map with the summary numbers of the statistics (samtools stats)
        Map<String, Object> map = new HashMap<>();

        int count = 0;
        for (String line : FileUtils.readLines(file, Charset.defaultCharset())) {
            // Only take into account the "SN" section (summary numbers)
            if (line.startsWith("SN")) {
                count++;
                String[] splits = line.split("\t");
                String key = splits[1].replace("(cigar)", "cigar").split("\\(")[0].trim().replace("1st", "first").replace(":", "")
                        .replace(" ", "_").replace("-", "_");
                key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                String value = splits[2].split(" ")[0];
                map.put(key, value);
            } else if (count > 0) {
                // SN (summary numbers) section has been processed
                break;
            }
        }

        // Convert map to AlignmentStats
        SamtoolsStats alignmentStats = JacksonUtils.getDefaultObjectMapper().convertValue(map, SamtoolsStats.class);

        // Set file ID
        alignmentStats.setFileId(fileId);

        return alignmentStats;
    }

    public static Set<String> getFileParamNames(String command) {
        Set<String> fileParamNames = new HashSet<>();
        switch (command) {
            case "sort":
                fileParamNames.add("reference");
                break;
            case "view":
                fileParamNames.add("U");
                fileParamNames.add("t");
                fileParamNames.add("L");
                fileParamNames.add("R");
                fileParamNames.add("T");
                fileParamNames.add("reference");
                break;
            case "stats":
                fileParamNames.add("r");
                fileParamNames.add("ref-seq");
                fileParamNames.add("reference");
                fileParamNames.add("t");
                fileParamNames.add("target-regions");
                break;
            case "depth":
                fileParamNames.add("reference");
                break;
            case "plot-bamstats":
                fileParamNames.add("r");
                fileParamNames.add("ref-stats");
                fileParamNames.add("s");
                fileParamNames.add("do-ref-stats");
                fileParamNames.add("t");
                fileParamNames.add("targets");
                break;
            default:
                break;

        }
        return  fileParamNames;
    }
}
