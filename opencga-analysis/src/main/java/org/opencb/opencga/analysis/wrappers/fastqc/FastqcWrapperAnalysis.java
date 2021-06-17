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

package org.opencb.opencga.analysis.wrappers.fastqc;


import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.alignment.FastqcWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.*;

@Tool(id = FastqcWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = FastqcWrapperAnalysis.DESCRIPTION)
public class FastqcWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "fastqc";
    public final static String DESCRIPTION = "A high throughput sequence QC analysis tool";

    public final static Set<String> FILE_PARAM_NAMES = new HashSet<>(Arrays.asList("l", "limits", "a", "adapters", "c", "contaminants"));

    @ToolParams
    protected final FastqcWrapperParams analysisParams = new FastqcWrapperParams();

    private String inputFilePath = null;

    protected void check() throws Exception {
        super.check();

        // Get files from catalog
        FileManager fileManager = catalogManager.getFileManager();
        if (StringUtils.isNotEmpty(analysisParams.getInputFile())) {
            inputFilePath = AnalysisUtils.getCatalogFile(analysisParams.getInputFile(), study, fileManager, token).getUri().getPath();
        }

        if (MapUtils.isNotEmpty(analysisParams.getFastqcParams())) {
            Map<String, String> updatedMap = new HashMap<>();
            for (Map.Entry<String, String> entry : analysisParams.getFastqcParams().entrySet()) {
                if (FILE_PARAM_NAMES.contains(entry.getKey())) {
                    updatedMap.put(entry.getKey(), AnalysisUtils.getCatalogFile(entry.getValue(), study, fileManager, token)
                            .getUri().getPath());
                }
            }
            if (MapUtils.isNotEmpty(updatedMap)) {
                analysisParams.getFastqcParams().putAll(updatedMap);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            if (MapUtils.isNotEmpty(analysisParams.getFastqcParams())) {
                executorParams.appendAll(analysisParams.getFastqcParams());
            }

            getToolExecutor(FastqcWrapperAnalysisExecutor.class)
                    .setInputFile(inputFilePath)
                    .execute();
        });
    }
}
