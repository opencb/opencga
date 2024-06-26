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
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.variant.PlinkWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.*;

@Tool(id = PlinkWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = PlinkWrapperAnalysis.DESCRIPTION)
public class PlinkWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "plink";
    public static final String DESCRIPTION = "Plink is a whole genome association analysis toolset, designed to perform"
            + " a range of basic, large-scale analyses.";

    public final static Set<String> FILE_PARAM_NAMES = new HashSet<>(Arrays.asList("ped", "map", "tped", "tfam", "bed", "bim", "fam", "vcf",
            "bcf", "lgen", "reference", "gen", "bgen", "sample", "23file", "read-freq", "pheno", "covar", "within", "loop-assoc", "set",
            "subset"));

    @ToolParams
    protected final PlinkWrapperParams analysisParams = new PlinkWrapperParams();

    protected void check() throws Exception {
        super.check();

        if (MapUtils.isNotEmpty(analysisParams.getPlinkParams())) {

            // Check prefix parameters
            if (analysisParams.getPlinkParams().containsKey("file")) {
                // Prefix for the .ped and .map files
                throw new ToolException("Plink --file parameter not supported. Please, use --ped and --map parameters instead of --file.");
            }
            if (analysisParams.getPlinkParams().containsKey("tfile")) {
                // Prefix for the .tped and .tfam files
                throw new ToolException("Plink --tfile parameter not supported. Please, use --tped and --tfam parameters instead of"
                        + " --tfile.");
            }
            if (analysisParams.getPlinkParams().containsKey("bfile")) {
                // Prefix for the .bed, .bim and .fam files
                throw new ToolException("Plink --bfile parameter not supported. Please, use --bed, --bim and --fam parameters instead of"
                        + " --bfile.");
            }
            if (analysisParams.getPlinkParams().containsKey("lfile")) {
                // Prefix for the .lgen file
                throw new ToolException("Plink --lfile parameter not supported. Please, use --lgen parameter instead of --lfile.");
            }
            if (analysisParams.getPlinkParams().containsKey("data")) {
                // Prefix for the .gen, .bgen and .sample files
                throw new ToolException("Plink --data parameter not supported. Please, use --gen, --bgen and --sample parameters instead"
                        + " of --data.");
            }

            // Get files from catalog
            FileManager fileManager = catalogManager.getFileManager();
            Map<String, String> updatedMap = new HashMap<>();
            for (Map.Entry<String, String> entry : analysisParams.getPlinkParams().entrySet()) {
                if (FILE_PARAM_NAMES.contains(entry.getKey())) {
                    updatedMap.put(entry.getKey(), AnalysisUtils.getCatalogFile(entry.getValue(), study, fileManager, token)
                            .getUri().getPath());
                }
            }

            // Finally update Plink parameters
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
