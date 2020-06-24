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
        FASTQC, FLAG_STATS, HS_METRICS
    }

    private String studyId;
    private String sampleId;
    private Qc qc;

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

    public String getSampleId() {
        return sampleId;
    }

    public SampleQcAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public Qc getQc() {
        return qc;
    }

    public SampleQcAnalysisExecutor setQc(Qc qc) {
        this.qc = qc;
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
