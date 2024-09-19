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

package org.opencb.opencga.core.models.variant.qc;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcGenomePlotAnalysisParams  extends ToolParams {

    @DataField(id = "id", description = FieldConstants.SAMPLE_QC_GENOME_PLOT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.SAMPLE_QC_GENOME_PLOT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "configFile", description = FieldConstants.SAMPLE_QC_GENOME_PLOT_CONFIG_FILE_DESCRIPTION)
    private String configFile;

    public SampleQcGenomePlotAnalysisParams() {
    }

    public SampleQcGenomePlotAnalysisParams(String id, String description, String configFile) {
        this.id = id;
        this.description = description;
        this.configFile = configFile;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQcGenomePlotAnalysisParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", configFile='").append(configFile).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleQcGenomePlotAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SampleQcGenomePlotAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getConfigFile() {
        return configFile;
    }

    public SampleQcGenomePlotAnalysisParams setConfigFile(String configFile) {
        this.configFile = configFile;
        return this;
    }
}
