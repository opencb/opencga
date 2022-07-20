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
import org.opencb.biodata.models.clinical.qc.SampleRelatednessReport;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.ArrayList;
import java.util.List;

import org.opencb.opencga.core.api.ParamConstants;

public class IndividualQualityControl {

    /**
     * List of inferred sex reports, it depends on the method (currently by coverage ratio)
     */

    @DataField(id = "inferredSexReports", indexed = true,
            description = FieldConstants.INDIVIDUAL_QUALITY_CONTROL_INFERRED_SEX_REPORT_DESCRIPTION)
    private List<InferredSexReport> inferredSexReports;

    @DataField(id = "sampleRelatednessReport", indexed = true,
            description = FieldConstants.INDIVIDUAL_QUALITY_CONTROL_SAMPLE_RELATEDNESS_REPORT_DESCRIPTION)
    private SampleRelatednessReport sampleRelatednessReport;

    /**
     * Mendelian errors
     */
    @DataField(id = "mendelianErrorReports", indexed = true,
            description = FieldConstants.INDIVIDUAL_QUALITY_CONTROL_MENDELIAN_ERRORS_DESCRIPTION)
    private List<MendelianErrorReport> mendelianErrorReports;
    /**
     * File IDs related to the quality control
     */
    @DataField(id = "files", indexed = true,
            description = FieldConstants.QUALITY_CONTROL_FILES_DESCRIPTION)
    private List<String> files;
    /**
     * Comments related to the quality control
     */
    @DataField(id = "author", indexed = true,
            description = FieldConstants.QUALITY_CONTROL_COMMENTS_DESCRIPTION)
    private List<ClinicalComment> comments;

    public IndividualQualityControl() {
        this(new SampleRelatednessReport(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public IndividualQualityControl(SampleRelatednessReport sampleRelatednessReport, List<InferredSexReport> inferredSexReports,
                                    List<MendelianErrorReport> mendelianErrorReports, List<String> files,
                                    List<ClinicalComment> comments) {
        this.inferredSexReports = inferredSexReports;
        this.sampleRelatednessReport = sampleRelatednessReport;
        this.mendelianErrorReports = mendelianErrorReports;
        this.files = files;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControl{");
        sb.append("inferredSexReports=").append(inferredSexReports);
        sb.append(", sampleRelatednessReport=").append(sampleRelatednessReport);
        sb.append(", mendelianErrorReports=").append(mendelianErrorReports);
        sb.append(", files=").append(files);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public List<InferredSexReport> getInferredSexReports() {
        return inferredSexReports;
    }

    public IndividualQualityControl setInferredSexReports(List<InferredSexReport> inferredSexReports) {
        this.inferredSexReports = inferredSexReports;
        return this;
    }

    public List<MendelianErrorReport> getMendelianErrorReports() {
        return mendelianErrorReports;
    }

    public IndividualQualityControl setMendelianErrorReports(List<MendelianErrorReport> mendelianErrorReports) {
        this.mendelianErrorReports = mendelianErrorReports;
        return this;
    }

    public List<String> getFiles() {
        return files;
    }

    public IndividualQualityControl setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public IndividualQualityControl setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public SampleRelatednessReport getSampleRelatednessReport() {
        return sampleRelatednessReport;
    }

    public IndividualQualityControl setSampleRelatednessReport(SampleRelatednessReport sampleRelatednessReport) {
        this.sampleRelatednessReport = sampleRelatednessReport;
        return this;
    }
}

