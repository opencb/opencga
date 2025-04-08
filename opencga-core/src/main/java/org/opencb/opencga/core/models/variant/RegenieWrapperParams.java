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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class RegenieWrapperParams extends ToolParams {

    public static final String DESCRIPTION = "Regenie parameters";


    @DataField(id = "step", description = FieldConstants.REGENIE_STEP_DESCRIPTION, required = true)
    private String step;

    @DataField(id = "phenoFile", description = FieldConstants.REGENIE_PHENO_FILE_DESCRIPTION, required = true)
    private String phenoFile;

    @DataField(id = "covarFile", description = FieldConstants.REGENIE_COVAR_FILE_DESCRIPTION)
    private String covarFile;

    @DataField(id = "predPath", description = FieldConstants.REGENIE_PRED_PATH_DESCRIPTION)
    private String predPath;

//    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
//    private String outdir;

    public RegenieWrapperParams() {
    }

    public RegenieWrapperParams(String step, String phenoFile, String covarFile, String predPath) {
        this.step = step;
        this.phenoFile = phenoFile;
        this.covarFile = covarFile;
        this.predPath = predPath;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieWrapperParams{");
        sb.append("step='").append(step).append('\'');
        sb.append(", phenoFile='").append(phenoFile).append('\'');
        sb.append(", covarFile='").append(covarFile).append('\'');
        sb.append(", predPath='").append(predPath).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStep() {
        return step;
    }

    public RegenieWrapperParams setStep(String step) {
        this.step = step;
        return this;
    }

    public String getPhenoFile() {
        return phenoFile;
    }

    public RegenieWrapperParams setPhenoFile(String phenoFile) {
        this.phenoFile = phenoFile;
        return this;
    }

    public String getCovarFile() {
        return covarFile;
    }

    public RegenieWrapperParams setCovarFile(String covarFile) {
        this.covarFile = covarFile;
        return this;
    }

    public String getPredPath() {
        return predPath;
    }

    public RegenieWrapperParams setPredPath(String predPath) {
        this.predPath = predPath;
        return this;
    }
}
