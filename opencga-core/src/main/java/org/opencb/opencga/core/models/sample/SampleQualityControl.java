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

package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.biodata.models.clinical.qc.sample.SampleQcVariantStats;
import org.opencb.biodata.models.clinical.qc.sample.Signature;
import org.opencb.opencga.core.models.variant.HsMetricsReport;
import org.opencb.opencga.core.models.variant.SamtoolsFlagStatsReport;

import java.io.Serializable;
import java.util.List;

public class SampleQualityControl implements Serializable {

    private List<SampleQcVariantStats> variantStats;
    private FastQc fastQc;
    private SamtoolsFlagStatsReport samtoolsFlagStatsReport;
    private HsMetricsReport hsMetricsReport;
    private List<GeneCoverageStats> geneCoverageStats;
    private Signature signature;

    public SampleQualityControl() {
    }

    public SampleQualityControl(List<SampleQcVariantStats> variantStats, FastQc fastQc,
                                SamtoolsFlagStatsReport samtoolsFlagStatsReport, HsMetricsReport hsMetricsReport,
                                List<GeneCoverageStats> geneCoverageStats, Signature signature) {
        this.variantStats = variantStats;
        this.fastQc = fastQc;
        this.samtoolsFlagStatsReport = samtoolsFlagStatsReport;
        this.hsMetricsReport = hsMetricsReport;
        this.geneCoverageStats = geneCoverageStats;
        this.signature = signature;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQualityControl{");
        sb.append("variantStats=").append(variantStats);
        sb.append(", fastQc=").append(fastQc);
        sb.append(", samtoolsFlagStatsReport=").append(samtoolsFlagStatsReport);
        sb.append(", hsMetricsReport=").append(hsMetricsReport);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append(", signature=").append(signature);
        sb.append('}');
        return sb.toString();
    }

    public List<SampleQcVariantStats> getVariantStats() {
        return variantStats;
    }

    public SampleQualityControl setVariantStats(List<SampleQcVariantStats> variantStats) {
        this.variantStats = variantStats;
        return this;
    }

    public FastQc getFastQc() {
        return fastQc;
    }

    public SampleQualityControl setFastQc(FastQc fastQc) {
        this.fastQc = fastQc;
        return this;
    }

    public SamtoolsFlagStatsReport getSamtoolsFlagStatsReport() {
        return samtoolsFlagStatsReport;
    }

    public SampleQualityControl setSamtoolsFlagStatsReport(SamtoolsFlagStatsReport samtoolsFlagStatsReport) {
        this.samtoolsFlagStatsReport = samtoolsFlagStatsReport;
        return this;
    }

    public HsMetricsReport getHsMetricsReport() {
        return hsMetricsReport;
    }

    public SampleQualityControl setHsMetricsReport(HsMetricsReport hsMetricsReport) {
        this.hsMetricsReport = hsMetricsReport;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public SampleQualityControl setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }

    public Signature getSignature() {
        return signature;
    }

    public SampleQualityControl setSignature(Signature signature) {
        this.signature = signature;
        return this;
    }
}
