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

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.Map;

public abstract class IndividualQcAnalysisExecutor extends OpenCgaToolExecutor {

    public enum QcType {
        INFERRED_SEX, RELATEDNESS, MENDELIAN_ERRORS
    }

    public static final String COVERAGE_RATIO_INFERRED_SEX_METHOD = "CoverageRatio";

    protected String studyId;
    protected Individual individual;
    protected String sampleId;
    protected String motherSampleId;
    protected String fatherSampleId;
    protected String inferredSexMethod;
    protected Map<String, Double> karyotypicSexThresholds;

    protected QcType qcType;

    protected IndividualQualityControl qualityControl;

    public IndividualQcAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public IndividualQcAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public Individual getIndividual() {
        return individual;
    }

    public IndividualQcAnalysisExecutor setIndividual(Individual individual) {
        this.individual = individual;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public IndividualQcAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getMotherSampleId() {
        return motherSampleId;
    }

    public IndividualQcAnalysisExecutor setMotherSampleId(String motherSampleId) {
        this.motherSampleId = motherSampleId;
        return this;
    }

    public String getFatherSampleId() {
        return fatherSampleId;
    }

    public IndividualQcAnalysisExecutor setFatherSampleId(String fatherSampleId) {
        this.fatherSampleId = fatherSampleId;
        return this;
    }

    public String getInferredSexMethod() {
        return inferredSexMethod;
    }

    public IndividualQcAnalysisExecutor setInferredSexMethod(String inferredSexMethod) {
        this.inferredSexMethod = inferredSexMethod;
        return this;
    }

    public Map<String, Double> getKaryotypicSexThresholds() {
        return karyotypicSexThresholds;
    }

    public IndividualQcAnalysisExecutor setKaryotypicSexThresholds(Map<String, Double> karyotypicSexThresholds) {
        this.karyotypicSexThresholds = karyotypicSexThresholds;
        return this;
    }

    public QcType getQcType() {
        return qcType;
    }

    public IndividualQcAnalysisExecutor setQcType(QcType qcType) {
        this.qcType = qcType;
        return this;
    }

    public IndividualQualityControl getQualityControl() {
        return qualityControl;
    }

    public IndividualQcAnalysisExecutor setQualityControl(IndividualQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }
}
