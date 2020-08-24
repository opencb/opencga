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

public class IndividualQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Individual QC analysis params";
    private String individual;
    private String sample;
    private String inferredSexMethod;

    private String outdir;

    public IndividualQcAnalysisParams() {
    }

    public IndividualQcAnalysisParams(String individual, String sample, String inferredSexMethod, String outdir) {
        this.individual = individual;
        this.sample = sample;
        this.inferredSexMethod = inferredSexMethod;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQcAnalysisParams{");
        sb.append("individual='").append(individual).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", inferredSexMethod='").append(inferredSexMethod).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getIndividual() {
        return individual;
    }

    public IndividualQcAnalysisParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public IndividualQcAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getInferredSexMethod() {
        return inferredSexMethod;
    }

    public IndividualQcAnalysisParams setInferredSexMethod(String inferredSexMethod) {
        this.inferredSexMethod = inferredSexMethod;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public IndividualQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
