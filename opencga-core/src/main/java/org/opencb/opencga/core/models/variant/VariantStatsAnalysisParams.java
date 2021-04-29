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

public class VariantStatsAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Variant stats params";

    public VariantStatsAnalysisParams() {
    }

    public VariantStatsAnalysisParams(List<String> cohort, List<String> samples, String region, String gene, String outdir,
                                      String outputFileName, Aggregation aggregated, String aggregationMappingFile) {
        this.cohort = cohort;
        this.samples = samples;
        this.region = region;
        this.gene = gene;
        this.outdir = outdir;
        this.outputFileName = outputFileName;
        this.aggregated = aggregated;
        this.aggregationMappingFile = aggregationMappingFile;
    }

    private List<String> cohort;
    private List<String> samples;
    private String region;
    private String gene;
    private String outdir;
    private String outputFileName;


    private Aggregation aggregated;
    private String aggregationMappingFile;

    public List<String> getCohort() {
        return cohort;
    }

    public VariantStatsAnalysisParams setCohort(List<String> cohort) {
        this.cohort = cohort;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public VariantStatsAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public VariantStatsAnalysisParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public VariantStatsAnalysisParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public VariantStatsAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public VariantStatsAnalysisParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public Aggregation getAggregated() {
        return aggregated;
    }

    public VariantStatsAnalysisParams setAggregated(Aggregation aggregated) {
        this.aggregated = aggregated;
        return this;
    }

    public String getAggregationMappingFile() {
        return aggregationMappingFile;
    }

    public VariantStatsAnalysisParams setAggregationMappingFile(String aggregationMappingFile) {
        this.aggregationMappingFile = aggregationMappingFile;
        return this;
    }
}
