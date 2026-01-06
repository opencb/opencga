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

package org.opencb.opencga.core.models.clinical.interpreter;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class InterpreterAnalysisToolParams {

    @DataField(id = "name", description = "RD interpretation name")
    private String name;

    @DataField(id = "description", description = "RD interpretation description")
    private String description;

    @DataField(id = "clinicalAnalysisId", description = FieldConstants.CLINICAL_INTERPRETATION_CLINICAL_ANALYSIS_ID_DESCRIPTION, required = true)
    private String clinicalAnalysisId;

    @DataField(id = "primary", description = FieldConstants.CLINICAL_INTERPRETATION_PRIMARY_DESCRIPTION)
    private boolean primary;

    @DataField(id = "configFile", description = FieldConstants.CLINICAL_INTERPRETATION_CONFIG_FILE_DESCRIPTION)
    private String configFile;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public InterpreterAnalysisToolParams() {
    }

    public InterpreterAnalysisToolParams(String name, String description, String clinicalAnalysisId, boolean primary, String configFile, String outdir) {
        this.name = name;
        this.description = description;
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.primary = primary;
        this.configFile = configFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpreterAnalysisToolParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", primary=").append(primary);
        sb.append(", configFile='").append(configFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public InterpreterAnalysisToolParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public InterpreterAnalysisToolParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public InterpreterAnalysisToolParams setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public InterpreterAnalysisToolParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public String getConfigFile() {
        return configFile;
    }

    public InterpreterAnalysisToolParams setConfigFile(String configFile) {
        this.configFile = configFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public InterpreterAnalysisToolParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
