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

public class InferredSexReport {

    // Sample ID
    private String individualId;

    // Sample ID
    private String sampleId;

    // Reported values
    private String reportedSex;
    private String reportedKaryotypicSex;

    // Ratio: X-chrom / autosoma-chroms
    private double ratioX;

    // Ratio: Y-chrom / autosoma-chroms
    private double ratioY;

    // Inferred karyotypic sex
    private String inferredKaryotypicSex;

    public InferredSexReport() {
    }

    public InferredSexReport(String individualId, String sampleId, String reportedSex, String reportedKaryotypicSex, double ratioX,
                             double ratioY, String inferredKaryotypicSex) {
        this.individualId = individualId;
        this.sampleId = sampleId;
        this.reportedSex = reportedSex;
        this.reportedKaryotypicSex = reportedKaryotypicSex;
        this.ratioX = ratioX;
        this.ratioY = ratioY;
        this.inferredKaryotypicSex = inferredKaryotypicSex;
    }

    public String getIndividualId() {
        return individualId;
    }

    public InferredSexReport setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public InferredSexReport setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getReportedSex() {
        return reportedSex;
    }

    public InferredSexReport setReportedSex(String reportedSex) {
        this.reportedSex = reportedSex;
        return this;
    }

    public String getReportedKaryotypicSex() {
        return reportedKaryotypicSex;
    }

    public InferredSexReport setReportedKaryotypicSex(String reportedKaryotypicSex) {
        this.reportedKaryotypicSex = reportedKaryotypicSex;
        return this;
    }

    public double getRatioX() {
        return ratioX;
    }

    public InferredSexReport setRatioX(double ratioX) {
        this.ratioX = ratioX;
        return this;
    }

    public double getRatioY() {
        return ratioY;
    }

    public InferredSexReport setRatioY(double ratioY) {
        this.ratioY = ratioY;
        return this;
    }

    public String getInferredKaryotypicSex() {
        return inferredKaryotypicSex;
    }

    public InferredSexReport setInferredKaryotypicSex(String inferredKaryotypicSex) {
        this.inferredKaryotypicSex = inferredKaryotypicSex;
        return this;
    }
}
