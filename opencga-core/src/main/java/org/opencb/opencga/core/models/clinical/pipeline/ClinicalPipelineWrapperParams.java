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

public class ClinicalPipelineWrapperParams extends ToolParams {

    public static final String DESCRIPTION = "Clinical pipeline parameters";

    @DataField(id = "prepareParams", description = FieldConstants.CLINICAL_PIPELINE_PREPARE_PARAMS_DESCRIPTION)
    private ClinicalPipelinePrepareParams prepareParams;

    @DataField(id = "executeParams", description = FieldConstants.CLINICAL_PIPELINE_EXECUTE_PARAMS_DESCRIPTION)
    private ClinicalPipelineExecuteParams executeParams;

    @DataField(id = "outDir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outDir;

    public ClinicalPipelineWrapperParams() {
    }

    public ClinicalPipelineWrapperParams(ClinicalPipelinePrepareParams prepareParams, ClinicalPipelineExecuteParams executeParams,
                                         String outDir) {
        this.prepareParams = prepareParams;
        this.executeParams = executeParams;
        this.outDir = outDir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPipelineWrapperParams{");
        sb.append("prepareParams=").append(prepareParams);
        sb.append(", executeParams=").append(executeParams);
        sb.append(", outDir='").append(outDir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public ClinicalPipelinePrepareParams getPrepareParams() {
        return prepareParams;
    }

    public ClinicalPipelineWrapperParams setPrepareParams(ClinicalPipelinePrepareParams prepareParams) {
        this.prepareParams = prepareParams;
        return this;
    }

    public ClinicalPipelineExecuteParams getExecuteParams() {
        return executeParams;
    }

    public ClinicalPipelineWrapperParams setExecuteParams(ClinicalPipelineExecuteParams executeParams) {
        this.executeParams = executeParams;
        return this;
    }

    public String getOutDir() {
        return outDir;
    }

    public ClinicalPipelineWrapperParams setOutDir(String outDir) {
        this.outDir = outDir;
        return this;
    }
}


