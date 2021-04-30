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

package org.opencb.opencga.analysis.wrappers.bwa;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.BwaWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.ParamConstants.BWA_COMMANDS_SUPPORTED;


@Tool(id = org.opencb.opencga.analysis.wrappers.bwa.BwaWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT,
        description = org.opencb.opencga.analysis.wrappers.bwa.BwaWrapperAnalysis.DESCRIPTION)
public class BwaWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "bwa";
    public final static String DESCRIPTION = "BWA is a software package for mapping low-divergent sequences against a large reference"
        + " genome.";

    @ToolParams
    protected final BwaWrapperParams analysisParams = new BwaWrapperParams();

    private String fastaFilePath = null;
    private String fastq1FilePath = null;
    private String fastq2FilePath = null;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(analysisParams.getCommand())) {
            throw new ToolException("Missing BWA command.");
        }

        if (!AnalysisUtils.isSupportedCommand(BWA_COMMANDS_SUPPORTED)) {
            throw new ToolException("BWA command '" + analysisParams.getCommand() + "' is not available. Supported commands are "
                    + BWA_COMMANDS_SUPPORTED);
        }

        // Get files from catalog
        FileManager fileManager = catalogManager.getFileManager();

        if (StringUtils.isNotEmpty(analysisParams.getFastaFile())) {
            fastaFilePath = AnalysisUtils.getCatalogFile(analysisParams.getFastaFile(), study, fileManager, token).getUri().getPath();
        }

        if (StringUtils.isNotEmpty(analysisParams.getFastq1File())) {
            fastq1FilePath = AnalysisUtils.getCatalogFile(analysisParams.getFastq1File(), study, fileManager, token).getUri().getPath();
        }

        if (StringUtils.isNotEmpty(analysisParams.getFastq2File())) {
            fastq2FilePath = AnalysisUtils.getCatalogFile(analysisParams.getFastq2File(), study, fileManager, token).getUri().getPath();
        }

        if (MapUtils.isNotEmpty(analysisParams.getBwaParams())) {
            Set<String> fileParams = getFileParamNames(analysisParams.getCommand());

            Map<String, String> updatedMap = new HashMap<>();
            for (Map.Entry<String, String> entry : analysisParams.getBwaParams().entrySet()) {
                if (fileParams.contains(entry.getKey())) {
                    updatedMap.put(entry.getKey(), AnalysisUtils.getCatalogFile(entry.getValue(), study, fileManager, token)
                            .getUri().getPath());
                }
            }
            if (MapUtils.isNotEmpty(updatedMap)) {
                analysisParams.getBwaParams().putAll(updatedMap);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            if (MapUtils.isNotEmpty(analysisParams.getBwaParams())) {
                executorParams.appendAll(analysisParams.getBwaParams());
            }

            getToolExecutor(BwaWrapperAnalysisExecutor.class)
                    .setCommand(analysisParams.getCommand())
                    .setFastaFile(fastaFilePath)
                    .setFastq1File(fastq1FilePath)
                    .setFastq2File(fastq2FilePath)
                    .execute();

            // Post-processing for the command 'index'
            if ("index".equals(analysisParams.getCommand())) {
                String name = Paths.get(fastaFilePath).getFileName().toString();
                for (Path path : Files.list(Paths.get(fastaFilePath).getParent()).collect(Collectors.toList())) {
                    if (path.getFileName().toString().startsWith(name + ".")) {
                        // Create symbolic link
                        Path symbolic = getOutDir().resolve(path.getFileName().toString());
                        Files.createSymbolicLink(symbolic, path);
                    }
                }
            }
        });
    }

    public static Set<String> getFileParamNames(String command) {
        switch (command) {
            case "mem":
                return new HashSet<>(Arrays.asList("H"));
            default:
                return null;
        }
    }
}
