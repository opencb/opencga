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

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class GenomePlotAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Genome plot analysis params to customize the plot. The configuration file includes the title, "
            + " the plot density (i.e., the number of points to display), the general query and the list of tracks. Currently, the supported "
            + "track types are: COPY-NUMBER, INDEL, REARRANGEMENT and SNV. In addition, each track can contain a specific query";

    @DataField(description = ParamConstants.GENOME_PLOT_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;
    @DataField(description = ParamConstants.GENOME_PLOT_ANALYSIS_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.GENOME_PLOT_ANALYSIS_PARAMS_CONFIG_FILE_DESCRIPTION)
    private String configFile;
    @DataField(description = ParamConstants.GENOME_PLOT_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;

    public GenomePlotAnalysisParams() {
    }

    public GenomePlotAnalysisParams(String sample, String id, String description, String configFile, String outdir) {
        this.sample = sample;
        this.id = id;
        this.description = description;
        this.configFile = configFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomePlotAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", configFile='").append(configFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public GenomePlotAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getId() {
        return id;
    }

    public GenomePlotAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public GenomePlotAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getConfigFile() {
        return configFile;
    }

    public GenomePlotAnalysisParams setConfigFile(String configFile) {
        this.configFile = configFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public GenomePlotAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
