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

import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class IndividualQcAnalysisExecutor extends OpenCgaToolExecutor {

    public enum Qc {
        INFERRED_SEX, RELATEDNESS, MENDELIAN_ERRORS
    }

    private String studyId;
    private String individualId;
    private String sampleId;
    private String familyId;
    private List<String> sampleIds;
    private Qc qc;
    private String minorAlleleFreq;
    private String relatednessMethod;

    private IndividualQualityControl report;

    public IndividualQcAnalysisExecutor() {
        IndividualQualityControl.IndividualQcMetrics metrics = new IndividualQualityControl.IndividualQcMetrics();
        metrics.setSampleId(sampleId);

        report = new IndividualQualityControl(Collections.singletonList(metrics), Collections.emptyList());
    }

    public String getStudyId() {
        return studyId;
    }

    public IndividualQcAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public IndividualQcAnalysisExecutor setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public IndividualQcAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public IndividualQcAnalysisExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public IndividualQcAnalysisExecutor setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public Qc getQc() {
        return qc;
    }

    public IndividualQcAnalysisExecutor setQc(Qc qc) {
        this.qc = qc;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public IndividualQcAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public IndividualQcAnalysisExecutor setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public IndividualQualityControl getReport() {
        return report;
    }

    public IndividualQcAnalysisExecutor setReport(IndividualQualityControl report) {
        this.report = report;
        return this;
    }
}
