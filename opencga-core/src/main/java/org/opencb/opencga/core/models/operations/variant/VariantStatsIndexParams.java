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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantStatsIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Variant stats index params";

    @DataField(description = ParamConstants.VARIANT_STATS_INDEX_PARAMS_COHORT_DESCRIPTION)
    private List<String> cohort;
    @DataField(description = ParamConstants.VARIANT_STATS_INDEX_PARAMS_REGION_DESCRIPTION)
    private String region;
    @DataField(description = ParamConstants.VARIANT_STATS_INDEX_PARAMS_OVERWRITE_STATS_DESCRIPTION)
    private boolean overwriteStats;
    @DataField(description = ParamConstants.VARIANT_STATS_INDEX_PARAMS_RESUME_DESCRIPTION)
    private boolean resume;

    @DataField(description = ParamConstants.VARIANT_STATS_INDEX_PARAMS_AGGREGATED_DESCRIPTION)
    private Aggregation aggregated;
    @DataField(description = ParamConstants.VARIANT_STATS_INDEX_PARAMS_AGGREGATION_MAPPING_FILE_DESCRIPTION)
    private String aggregationMappingFile;

    public VariantStatsIndexParams() {
    }

    public VariantStatsIndexParams(List<String> cohort, String region, boolean overwriteStats,
                                   boolean resume, Aggregation aggregated, String aggregationMappingFile) {
        this.cohort = cohort;
        this.region = region;
        this.overwriteStats = overwriteStats;
        this.resume = resume;
        this.aggregated = aggregated;
        this.aggregationMappingFile = aggregationMappingFile;
    }

    public List<String> getCohort() {
        return cohort;
    }

    public VariantStatsIndexParams setCohort(List<String> cohort) {
        this.cohort = cohort;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public VariantStatsIndexParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public boolean isOverwriteStats() {
        return overwriteStats;
    }

    public VariantStatsIndexParams setOverwriteStats(boolean overwriteStats) {
        this.overwriteStats = overwriteStats;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantStatsIndexParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }

    public Aggregation getAggregated() {
        return aggregated;
    }

    public VariantStatsIndexParams setAggregated(Aggregation aggregated) {
        this.aggregated = aggregated;
        return this;
    }

    public String getAggregationMappingFile() {
        return aggregationMappingFile;
    }

    public VariantStatsIndexParams setAggregationMappingFile(String aggregationMappingFile) {
        this.aggregationMappingFile = aggregationMappingFile;
        return this;
    }
}
