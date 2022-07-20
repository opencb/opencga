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

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class RelatednessAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Relatedness analysis params";
    @DataField(description = ParamConstants.RELATEDNESS_ANALYSIS_PARAMS_INDIVIDUALS_DESCRIPTION)
    private List<String> individuals;
    @DataField(description = ParamConstants.RELATEDNESS_ANALYSIS_PARAMS_SAMPLES_DESCRIPTION)
    private List<String> samples;
    @DataField(description = ParamConstants.RELATEDNESS_ANALYSIS_PARAMS_MINOR_ALLELE_FREQ_DESCRIPTION)
    private String minorAlleleFreq;
    @DataField(description = ParamConstants.RELATEDNESS_ANALYSIS_PARAMS_METHOD_DESCRIPTION)
    private String method;
    @DataField(description = ParamConstants.RELATEDNESS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;

    public RelatednessAnalysisParams() {
    }

    public RelatednessAnalysisParams(List<String> individuals, List<String> samples, String minorAlleleFreq, String method, String outdir) {
        this.individuals = individuals;
        this.samples = samples;
        this.minorAlleleFreq = minorAlleleFreq;
        this.method = method;
        this.outdir = outdir;
    }

    public List<String> getIndividuals() {
        return individuals;
    }

    public RelatednessAnalysisParams setIndividuals(List<String> individuals) {
        this.individuals = individuals;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public RelatednessAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public RelatednessAnalysisParams setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public RelatednessAnalysisParams setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public RelatednessAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
