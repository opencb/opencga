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

import static org.opencb.biodata.models.constants.FieldConstants.RELATEDNESS_REPORT_HAPLOID_CALL_MODE_DESCRIPTION;
import static org.opencb.biodata.models.constants.FieldConstants.RELATEDNESS_REPORT_MAF_DESCRIPTION;

public class RelatednessAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Relatedness analysis params.";

    @DataField(id = "individuals", description = "List of individuals (separated by commas)")
    private List<String> individuals;

    @DataField(id = "samples", description = "List of samples (separated by commas) to identify the individuals")
    private List<String> samples;

    @DataField(id = "minorAlleleFreq", description = RELATEDNESS_REPORT_MAF_DESCRIPTION)
    private String minorAlleleFreq;

    @DataField(id = "haploidCallMode", description = RELATEDNESS_REPORT_HAPLOID_CALL_MODE_DESCRIPTION)
    private String haploidCallMode;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public RelatednessAnalysisParams() {
    }

    public RelatednessAnalysisParams(List<String> individuals, List<String> samples, String minorAlleleFreq, String haploidCallMode, String outdir) {
        this.individuals = individuals;
        this.samples = samples;
        this.minorAlleleFreq = minorAlleleFreq;
        this.haploidCallMode = haploidCallMode;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RelatednessAnalysisParams{");
        sb.append("individuals=").append(individuals);
        sb.append(", samples=").append(samples);
        sb.append(", minorAlleleFreq='").append(minorAlleleFreq).append('\'');
        sb.append(", haploidCallMode='").append(haploidCallMode).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
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

    public String getHaploidCallMode() {
        return haploidCallMode;
    }

    public RelatednessAnalysisParams setHaploidCallMode(String haploidCallMode) {
        this.haploidCallMode = haploidCallMode;
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
