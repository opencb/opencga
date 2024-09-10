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

import java.util.List;

@Deprecated
public class InferredSexAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Inferred sex analysis params";

    @DataField(id = "individual", description = FieldConstants.INDIVIDUAL_QC_INDIVIDUAL_ID_DESCRIPTION)
    private String individual;

    @DataField(id = "sample", description = FieldConstants.INDIVIDUAL_QC_SAMPLE_ID_DESCRIPTION)
    private String sample;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public InferredSexAnalysisParams() {
    }

    public InferredSexAnalysisParams(String individual, String sample, String outdir) {
        this.individual = individual;
        this.sample = sample;
        this.outdir = outdir;
    }

    public String getIndividual() {
        return individual;
    }

    public InferredSexAnalysisParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public InferredSexAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public InferredSexAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
