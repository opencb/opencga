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

package org.opencb.opencga.core.models.variant;

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantStatsIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Variant stats params";

    private List<String> cohort;
    private String region;
    private boolean overwriteStats;
    private boolean resume;

    private Aggregation aggregated;
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
