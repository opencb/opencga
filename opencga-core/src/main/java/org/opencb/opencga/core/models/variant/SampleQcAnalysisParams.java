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

public class SampleQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample QC analysis params";
    private String sample;
    private String dictFile;
    private String baitFile;
    private String variantStatsId;
    private String variantStatsDescription;
    private AnnotationVariantQueryParams variantStatsQuery;
    private String signatureId;
    private SampleQcSignatureQueryParams signatureQuery;
    private List<String> genesForCoverageStats;

    private String outdir;

    public SampleQcAnalysisParams() {
    }

    public SampleQcAnalysisParams(String sample, String dictFile, String baitFile, String variantStatsId, String variantStatsDescription,
                                  AnnotationVariantQueryParams variantStatsQuery, String signatureId,
                                  SampleQcSignatureQueryParams signatureQuery, List<String> genesForCoverageStats, String outdir) {
        this.sample = sample;
        this.dictFile = dictFile;
        this.baitFile = baitFile;
        this.variantStatsId = variantStatsId;
        this.variantStatsDescription = variantStatsDescription;
        this.variantStatsQuery = variantStatsQuery;
        this.signatureId = signatureId;
        this.signatureQuery = signatureQuery;
        this.genesForCoverageStats = genesForCoverageStats;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQcAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", dictFile='").append(dictFile).append('\'');
        sb.append(", baitFile='").append(baitFile).append('\'');
        sb.append(", variantStatsId='").append(variantStatsId).append('\'');
        sb.append(", variantStatsDecription='").append(variantStatsDescription).append('\'');
        sb.append(", variantStatsQuery=").append(variantStatsQuery);
        sb.append(", signatureId='").append(signatureId).append('\'');
        sb.append(", signatureQuery=").append(signatureQuery);
        sb.append(", genesForCoverageStats=").append(genesForCoverageStats);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public SampleQcAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getDictFile() {
        return dictFile;
    }

    public SampleQcAnalysisParams setDictFile(String dictFile) {
        this.dictFile = dictFile;
        return this;
    }

    public String getBaitFile() {
        return baitFile;
    }

    public SampleQcAnalysisParams setBaitFile(String baitFile) {
        this.baitFile = baitFile;
        return this;
    }

    public String getVariantStatsId() {
        return variantStatsId;
    }

    public SampleQcAnalysisParams setVariantStatsId(String variantStatsId) {
        this.variantStatsId = variantStatsId;
        return this;
    }

    public String getVariantStatsDescription() {
        return variantStatsDescription;
    }

    public SampleQcAnalysisParams setVariantStatsDescription(String variantStatsDescription) {
        this.variantStatsDescription = variantStatsDescription;
        return this;
    }

    public AnnotationVariantQueryParams getVariantStatsQuery() {
        return variantStatsQuery;
    }

    public SampleQcAnalysisParams setVariantStatsQuery(AnnotationVariantQueryParams variantStatsQuery) {
        this.variantStatsQuery = variantStatsQuery;
        return this;
    }

    public String getSignatureId() {
        return signatureId;
    }

    public SampleQcAnalysisParams setSignatureId(String signatureId) {
        this.signatureId = signatureId;
        return this;
    }

    public SampleQcSignatureQueryParams getSignatureQuery() {
        return signatureQuery;
    }

    public SampleQcAnalysisParams setSignatureQuery(SampleQcSignatureQueryParams signatureQuery) {
        this.signatureQuery = signatureQuery;
        return this;
    }

    public List<String> getGenesForCoverageStats() {
        return genesForCoverageStats;
    }

    public SampleQcAnalysisParams setGenesForCoverageStats(List<String> genesForCoverageStats) {
        this.genesForCoverageStats = genesForCoverageStats;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SampleQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
