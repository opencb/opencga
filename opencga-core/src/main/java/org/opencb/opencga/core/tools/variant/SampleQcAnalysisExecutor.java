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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;
import java.util.Map;

public abstract class SampleQcAnalysisExecutor extends OpenCgaToolExecutor {

    public enum Qc {
        VARIAN_STATS, FASTQC, FLAG_STATS, HS_METRICS, GENE_COVERAGE_STATS, MUTATIONAL_SIGNATURE
    }

    private String studyId;
    private Sample sample;
    private String bamFile;
    private String fastaFile;
    private String baitFile;
    private String targetFile;
    private String variantStatsId;
    private String variantStatsDecription;
    private Map<String, String> variantStatsQuery;
    private String signatureId;
    private Map<String, String> signatureQuery;
    private List<String> genesForCoverageStats;

    private Qc qc;

    public SampleQcAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public SampleQcAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public Sample getSample() {
        return sample;
    }

    public SampleQcAnalysisExecutor setSample(Sample sample) {
        this.sample = sample;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public SampleQcAnalysisExecutor setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public SampleQcAnalysisExecutor setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getBaitFile() {
        return baitFile;
    }

    public SampleQcAnalysisExecutor setBaitFile(String baitFile) {
        this.baitFile = baitFile;
        return this;
    }

    public String getTargetFile() {
        return targetFile;
    }

    public SampleQcAnalysisExecutor setTargetFile(String targetFile) {
        this.targetFile = targetFile;
        return this;
    }

    public String getVariantStatsId() {
        return variantStatsId;
    }

    public SampleQcAnalysisExecutor setVariantStatsId(String variantStatsId) {
        this.variantStatsId = variantStatsId;
        return this;
    }

    public String getVariantStatsDecription() {
        return variantStatsDecription;
    }

    public SampleQcAnalysisExecutor setVariantStatsDecription(String variantStatsDecription) {
        this.variantStatsDecription = variantStatsDecription;
        return this;
    }

    public Map<String, String> getVariantStatsQuery() {
        return variantStatsQuery;
    }

    public SampleQcAnalysisExecutor setVariantStatsQuery(Map<String, String> variantStatsQuery) {
        this.variantStatsQuery = variantStatsQuery;
        return this;
    }

    public String getSignatureId() {
        return signatureId;
    }

    public SampleQcAnalysisExecutor setSignatureId(String signatureId) {
        this.signatureId = signatureId;
        return this;
    }

    public Map<String, String> getSignatureQuery() {
        return signatureQuery;
    }

    public SampleQcAnalysisExecutor setSignatureQuery(Map<String, String> signatureQuery) {
        this.signatureQuery = signatureQuery;
        return this;
    }

    public List<String> getGenesForCoverageStats() {
        return genesForCoverageStats;
    }

    public SampleQcAnalysisExecutor setGenesForCoverageStats(List<String> genesForCoverageStats) {
        this.genesForCoverageStats = genesForCoverageStats;
        return this;
    }

    public Qc getQc() {
        return qc;
    }

    public SampleQcAnalysisExecutor setQc(Qc qc) {
        this.qc = qc;
        return this;
    }
}
