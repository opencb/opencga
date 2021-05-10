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

package org.opencb.opencga.analysis.wrappers.plink;


import org.apache.commons.collections4.MapUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.PlinkWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.*;

@Tool(id = PlinkWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = PlinkWrapperAnalysis.DESCRIPTION)
public class PlinkWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "plink";
    public static final String DESCRIPTION = "Plink is a whole genome association analysis toolset, designed to perform"
            + " a range of basic, large-scale analyses.";

    public final static Set<String> FILE_PARAM_NAMES = new HashSet<>(Arrays.asList("file", "tfile", "bfile", "pheno", "within", "cov"));

    @ToolParams
    protected final PlinkWrapperParams analysisParams = new PlinkWrapperParams();

    protected void check() throws Exception {
        super.check();

        // Get files from catalog
        FileManager fileManager = catalogManager.getFileManager();
        if (MapUtils.isNotEmpty(analysisParams.getPlinkParams())) {
            Map<String, String> updatedMap = new HashMap<>();
            for (Map.Entry<String, String> entry : analysisParams.getPlinkParams().entrySet()) {
                if (FILE_PARAM_NAMES.contains(entry.getKey())) {
                    switch (entry.getKey()) {
                        case "file":
                        case "tfile":
                            // Check .ped and .map files, (throw an exception if one of them does not exist)
                            AnalysisUtils.getCatalogFile(entry.getValue() + ".ped", study, fileManager, token);
                            AnalysisUtils.getCatalogFile(entry.getValue() + ".map", study, fileManager, token);
                            break;
                        case "bfile":
                            // Check .bed, .fam and .map files, (throw an exception if one of them does not exist)
                            AnalysisUtils.getCatalogFile(entry.getValue() + ".bed", study, fileManager, token);
                            AnalysisUtils.getCatalogFile(entry.getValue() + ".fam", study, fileManager, token);
                            AnalysisUtils.getCatalogFile(entry.getValue() + ".map", study, fileManager, token);
                            break;
                        default:
                            updatedMap.put(entry.getKey(), AnalysisUtils.getCatalogFile(entry.getValue(), study, fileManager, token)
                                    .getUri().getPath());
                    }
                }
            }
            if (MapUtils.isNotEmpty(updatedMap)) {
                analysisParams.getPlinkParams().putAll(updatedMap);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            if (MapUtils.isNotEmpty(analysisParams.getPlinkParams())) {
                executorParams.appendAll(analysisParams.getPlinkParams());
            }

            getToolExecutor(PlinkWrapperAnalysisExecutor.class)
                    .execute();
        });
    }
}
