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

public class RelatednessAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Relatedness analysis params";
    private List<String> individuals;
    private List<String> samples;
    private String minorAlleleFreq;
    private String method;
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
