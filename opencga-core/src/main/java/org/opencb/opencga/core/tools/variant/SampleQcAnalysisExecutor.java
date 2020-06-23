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

import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class SampleQcAnalysisExecutor extends OpenCgaToolExecutor {

    public enum Qc {
        INFERRED_SEX, RELATEDNESS, MENDELIAN_ERRORS, FASTQC, FLAG_STATS, HS_METRICS
    }

    private String studyId;
    private String familyId;
    private List<String> sampleIds;
    private Qc qc;
    private String minorAlleleFreq;
    private String relatednessMethod;

    private SampleQualityControl report;

    public SampleQcAnalysisExecutor() {
        report = new SampleQualityControl();
    }

    public String getStudyId() {
        return studyId;
    }

    public SampleQcAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public SampleQcAnalysisExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public SampleQcAnalysisExecutor setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public Qc getQc() {
        return qc;
    }

    public SampleQcAnalysisExecutor setQc(Qc qc) {
        this.qc = qc;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public SampleQcAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public SampleQcAnalysisExecutor setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public SampleQualityControl getReport() {
        return report;
    }

    public SampleQcAnalysisExecutor setReport(SampleQualityControl report) {
        this.report = report;
        return this;
    }
}
