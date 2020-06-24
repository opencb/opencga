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

import org.opencb.biodata.models.clinical.Comment;
import org.opencb.biodata.models.clinical.qc.QualityControlFile;
import org.opencb.biodata.models.clinical.qc.individual.InferredSexReport;
import org.opencb.biodata.models.clinical.qc.individual.MendelianErrorReport;
import org.opencb.biodata.models.clinical.qc.individual.RelatednessReport;

import java.util.List;

public class IndividualQualityControl {

    /**
     * List of metrics for that individual, one metrics per sample
     */
    private List<IndividualQcMetrics> metrics;

    /**
     * Comments related to the quality control
     */
    private List<Comment> comments;

    public IndividualQualityControl() {
    }

    public IndividualQualityControl(List<IndividualQcMetrics> metrics, List<Comment> comments) {
        this.metrics = metrics;
        this.comments = comments;
    }

    public static class IndividualQcMetrics {

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

        public IndividualQcMetrics() {
        }

        public IndividualQcMetrics(String sampleId, List<InferredSexReport> inferredSexReport, RelatednessReport relatednessReport,
                                   MendelianErrorReport mendelianErrorReport, List<QualityControlFile> qcFiles) {
            this.sampleId = sampleId;
            this.inferredSexReport = inferredSexReport;
            this.relatednessReport = relatednessReport;
            this.mendelianErrorReport = mendelianErrorReport;
            this.qcFiles = qcFiles;
        }

        public String getSampleId() {
            return sampleId;
        }

        public IndividualQcMetrics setSampleId(String sampleId) {
            this.sampleId = sampleId;
            return this;
        }

        public List<InferredSexReport> getInferredSexReport() {
            return inferredSexReport;
        }

        public IndividualQcMetrics setInferredSexReport(List<InferredSexReport> inferredSexReport) {
            this.inferredSexReport = inferredSexReport;
            return this;
        }

        public RelatednessReport getRelatednessReport() {
            return relatednessReport;
        }

        public IndividualQcMetrics setRelatednessReport(RelatednessReport relatednessReport) {
            this.relatednessReport = relatednessReport;
            return this;
        }

        public MendelianErrorReport getMendelianErrorReport() {
            return mendelianErrorReport;
        }

        public IndividualQcMetrics setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
            this.mendelianErrorReport = mendelianErrorReport;
            return this;
        }

        public List<QualityControlFile> getQcFiles() {
            return qcFiles;
        }

        public IndividualQcMetrics setQcFiles(List<QualityControlFile> qcFiles) {
            this.qcFiles = qcFiles;
            return this;
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControl{");
        sb.append("metrics=").append(metrics);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public List<IndividualQcMetrics> getMetrics() {
        return metrics;
    }

    public IndividualQualityControl setMetrics(List<IndividualQcMetrics> metrics) {
        this.metrics = metrics;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public IndividualQualityControl setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }
}
