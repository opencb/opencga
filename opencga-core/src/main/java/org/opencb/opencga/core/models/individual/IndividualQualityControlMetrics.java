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

package org.opencb.opencga.core.models.individual;

import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;
import org.opencb.biodata.models.clinical.qc.QualityControlFile;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;

import java.util.List;

public class IndividualQualityControlMetrics {

    /**
     * Sample of that individual
     */
    private String sampleId;

    /**
     * Inferred Sex based on sexual chromosome coverage
     */
    private List<InferredSexReport> inferredSexReport;

    /**
     * Plink-based relatedness
     */
    private RelatednessReport relatednessReport;

    /**
     * Mendelian errors
     */
    private MendelianErrorReport mendelianErrorReport;

    /**
     * List of quality control files related to the previous analysis
     */
    private List<QualityControlFile> qcFiles;

    public IndividualQualityControlMetrics() {
    }

    public IndividualQualityControlMetrics(String sampleId, List<InferredSexReport> inferredSexReport, RelatednessReport relatednessReport,
                                           MendelianErrorReport mendelianErrorReport, List<QualityControlFile> qcFiles) {
        this.sampleId = sampleId;
        this.inferredSexReport = inferredSexReport;
        this.relatednessReport = relatednessReport;
        this.mendelianErrorReport = mendelianErrorReport;
        this.qcFiles = qcFiles;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControlMetrics{");
        sb.append("sampleId='").append(sampleId).append('\'');
        sb.append(", inferredSexReport=").append(inferredSexReport);
        sb.append(", relatednessReport=").append(relatednessReport);
        sb.append(", mendelianErrorReport=").append(mendelianErrorReport);
        sb.append(", qcFiles=").append(qcFiles);
        sb.append('}');
        return sb.toString();
    }

    public String getSampleId() {
        return sampleId;
    }

    public IndividualQualityControlMetrics setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public List<InferredSexReport> getInferredSexReport() {
        return inferredSexReport;
    }

    public IndividualQualityControlMetrics setInferredSexReport(List<InferredSexReport> inferredSexReport) {
        this.inferredSexReport = inferredSexReport;
        return this;
    }

    public RelatednessReport getRelatednessReport() {
        return relatednessReport;
    }

    public IndividualQualityControlMetrics setRelatednessReport(RelatednessReport relatednessReport) {
        this.relatednessReport = relatednessReport;
        return this;
    }

    public MendelianErrorReport getMendelianErrorReport() {
        return mendelianErrorReport;
    }

    public IndividualQualityControlMetrics setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
        this.mendelianErrorReport = mendelianErrorReport;
        return this;
    }

    public List<QualityControlFile> getQcFiles() {
        return qcFiles;
    }

    public IndividualQualityControlMetrics setQcFiles(List<QualityControlFile> qcFiles) {
        this.qcFiles = qcFiles;
        return this;
    }
}
