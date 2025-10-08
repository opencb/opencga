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

package org.opencb.opencga.core.models.clinical.pipeline;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class ClinicalPipelineExecuteWrapperParams extends ToolParams {

    public static final String DESCRIPTION = "Parameters to execute the clinical pipeline";

    @DataField(id = "pipelineParams", description = FieldConstants.CLINICAL_PIPELINE_EXECUTE_PARAMS_DESCRIPTION)
    private ClinicalPipelineExecuteParams pipelineParams;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public ClinicalPipelineExecuteWrapperParams() {
        this.pipelineParams = new ClinicalPipelineExecuteParams();
    }

    public ClinicalPipelineExecuteWrapperParams(ClinicalPipelineExecuteParams pipelineParams, String outdir) {
        this.pipelineParams = pipelineParams;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPipelineExecuteWrapperParams{");
        sb.append("pipelineParams=").append(pipelineParams);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public ClinicalPipelineExecuteParams getPipelineParams() {
        return pipelineParams;
    }

    public ClinicalPipelineExecuteWrapperParams setPipelineParams(ClinicalPipelineExecuteParams pipelineParams) {
        this.pipelineParams = pipelineParams;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public ClinicalPipelineExecuteWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}


