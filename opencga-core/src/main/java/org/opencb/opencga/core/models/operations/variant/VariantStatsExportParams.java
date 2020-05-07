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

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantStatsExportParams extends ToolParams {
    public static final String DESCRIPTION = "Variant stats export params";

    public VariantStatsExportParams() {
    }
    public VariantStatsExportParams(List<String> cohorts, String output, String region, String gene, String outputFormat) {
        this.cohorts = cohorts;
        this.output = output;
        this.region = region;
        this.gene = gene;
        this.outputFormat = outputFormat;
    }
    private List<String> cohorts;
    private String output;
    private String region;
    private String gene;
    private String outputFormat;

    public List<String> getCohorts() {
        return cohorts;
    }

    public VariantStatsExportParams setCohorts(List<String> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public String getOutput() {
        return output;
    }

    public VariantStatsExportParams setOutput(String output) {
        this.output = output;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public VariantStatsExportParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public VariantStatsExportParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public VariantStatsExportParams setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }
}
