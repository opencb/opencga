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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantStatsExportParams extends ToolParams {
    public static final String DESCRIPTION = "Variant stats export params";

    public VariantStatsExportParams() {
    }
    public VariantStatsExportParams(List<String> cohorts, String output, String region, String gene, String outputFileFormat) {
        this.cohorts = cohorts;
        this.output = output;
        this.region = region;
        this.gene = gene;
        this.outputFileFormat = outputFileFormat;
    }
    @DataField(description = ParamConstants.VARIANT_STATS_EXPORT_PARAMS_COHORTS_DESCRIPTION)
    private List<String> cohorts;
    @DataField(description = ParamConstants.VARIANT_STATS_EXPORT_PARAMS_OUTPUT_DESCRIPTION)
    private String output;
    @DataField(description = ParamConstants.VARIANT_STATS_EXPORT_PARAMS_REGION_DESCRIPTION)
    private String region;
    @DataField(description = ParamConstants.VARIANT_STATS_EXPORT_PARAMS_GENE_DESCRIPTION)
    private String gene;
    @DataField(description = ParamConstants.VARIANT_STATS_EXPORT_PARAMS_OUTPUT_FILE_FORMAT_DESCRIPTION)
    private String outputFileFormat;

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

    public String getOutputFileFormat() {
        return outputFileFormat;
    }

    public VariantStatsExportParams setOutputFileFormat(String outputFileFormat) {
        this.outputFileFormat = outputFileFormat;
        return this;
    }
}
