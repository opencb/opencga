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

package org.opencb.opencga.analysis.variant.operations;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.VariantStatsIndexParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Created by jacobo on 06/03/15.
 */
@Tool(id = VariantStatsIndexOperationTool.ID, resource = Enums.Resource.VARIANT, type = Tool.Type.OPERATION,
        description = VariantStatsIndexOperationTool.DESCRIPTION)
public class VariantStatsIndexOperationTool extends OperationTool {

    public final static String ID = "variant-stats-index";
    public static final String DESCRIPTION = "Compute variant stats for any cohort and any set of variants "
            + "and index the result in the variant storage database.";

    public static final String STATS_AGGREGATION_CATALOG = VariantStorageOptions.STATS_AGGREGATION.key().replace(".", "_");

    @ToolParams
    protected VariantStatsIndexParams toolParams = new VariantStatsIndexParams();
    private String studyFqn;

    @Override
    protected void check() throws Exception {
        super.check();
        studyFqn = getStudyFqn();

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        String aggregationMappingFile = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());
        Path mappingFilePath = null;
        if (isNotEmpty(aggregationMappingFile)) {
            mappingFilePath = getFilePath(aggregationMappingFile);
        }

        // Do not save intermediate files
        Path outputFile = getScratchDir().resolve("stats");

        if (toolParams.getCohort() == null) {
            toolParams.setCohort(new ArrayList<>());
        }

        params.put(VariantStorageOptions.STATS_OVERWRITE.key(), toolParams.isOverwriteStats());
        params.put(VariantStorageOptions.STATS_AGGREGATION.key(), toolParams.getAggregated());
        params.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), toolParams.getAggregationMappingFile());
        params.put(VariantStorageOptions.RESUME.key(), toolParams.isResume());
        params.append(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), mappingFilePath);
        params.append(DefaultVariantStatisticsManager.OUTPUT, outputFile.toAbsolutePath().toString());
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(getId());
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            Collection<String> cohorts = variantStorageManager.stats(
                    studyFqn,
                    toolParams.getCohort(),
                    /*toolParams.getRegion()*/ null,
                    params,
                    token);
            addAttribute("cohorts", cohorts);
        });
    }

    private Path getFilePath(String aggregationMapFile) throws CatalogException {
        if (Files.exists(Paths.get(aggregationMapFile))) {
            return Paths.get(aggregationMapFile).toAbsolutePath();
        } else {
            return Paths.get(getCatalogManager().getFileManager()
                    .get(studyFqn, aggregationMapFile, QueryOptions.empty(), getToken()).first().getUri());
        }
    }

}
