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

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;

import java.util.ArrayList;
import java.util.List;

public class IndividualQualityControl {

    /**
     * Sample ID, only germinal sample and indexed
     */
    private String sampleId;

    /**
     * List of inferred sex reports, it depends on the method (currently by coverage ratio)
     */
    List<InferredSexReport> inferredSexReports;

    /**
     * Mendelian errors
     */
    private MendelianErrorReport mendelianErrorReport;

    /**
     * File IDs related to the quality control
     */
    private List<String> fileIds;

    /**
     * Comments related to the quality control
     */
    private List<ClinicalComment> comments;

    public IndividualQualityControl() {
        this(null, new ArrayList<>(), null, new ArrayList<>(), new ArrayList<>());
    }

    public IndividualQualityControl(String sampleId, List<InferredSexReport> inferredSexReports, MendelianErrorReport mendelianErrorReport,
                                    List<String> fileIds, List<ClinicalComment> comments) {
        this.sampleId = sampleId;
        this.inferredSexReports = inferredSexReports;
        this.mendelianErrorReport = mendelianErrorReport;
        this.fileIds = fileIds;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControl{");
        sb.append("sampleId='").append(sampleId).append('\'');
        sb.append(", inferredSexReports=").append(inferredSexReports);
        sb.append(", mendelianErrorReport=").append(mendelianErrorReport);
        sb.append(", fileIds=").append(fileIds);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public String getSampleId() {
        return sampleId;
    }

    public IndividualQualityControl setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public List<InferredSexReport> getInferredSexReports() {
        return inferredSexReports;
    }

    public IndividualQualityControl setInferredSexReports(List<InferredSexReport> inferredSexReports) {
        this.inferredSexReports = inferredSexReports;
        return this;
    }

    public MendelianErrorReport getMendelianErrorReport() {
        return mendelianErrorReport;
    }

    public IndividualQualityControl setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
        this.mendelianErrorReport = mendelianErrorReport;
        return this;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public IndividualQualityControl setFileIds(List<String> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public IndividualQualityControl setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }
}

