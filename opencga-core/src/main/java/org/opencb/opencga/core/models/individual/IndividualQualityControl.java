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
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.ArrayList;
import java.util.List;

public class IndividualQualityControl {

    /**
     * List of inferred sex reports
     */
    @DataField(id = "inferredSexReports", indexed = true, description = FieldConstants.QC_INFERRED_SEX_REPORTS_DESCRIPTION)
    private List<InferredSexReport> inferredSexReports;

    /**
     * List of relatedness reports
     */
    @DataField(id = "relatednessReports", indexed = true, description = FieldConstants.QC_RELATEDNESS_REPORTS_DESCRIPTION)
    private List<RelatednessReport> relatednessReports;

    /**
     * Mendelian errors
     */
    @DataField(id = "mendelianErrorReports", indexed = true, description = FieldConstants.QC_MENDELIAN_ERROR_REPORTS_DESCRIPTION)
    private List<MendelianErrorReport> mendelianErrorReports;

    /**
     * File IDs related to the quality control
     */
    @DataField(id = "files", indexed = true, description = FieldConstants.QC_FILES_DESCRIPTION)
    private List<String> files;

    /**
     * Comments related to the quality control
     */
    @DataField(id = "author", indexed = true, description = FieldConstants.QC_COMMENTS_DESCRIPTION)
    private List<ClinicalComment> comments;

    public IndividualQualityControl() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public IndividualQualityControl(List<InferredSexReport> inferredSexReports, List<RelatednessReport> relatednessReports,
                                    List<MendelianErrorReport> mendelianErrorReports, List<String> files, List<ClinicalComment> comments) {
        this.inferredSexReports = inferredSexReports;
        this.relatednessReports = relatednessReports;
        this.mendelianErrorReports = mendelianErrorReports;
        this.files = files;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControl{");
        sb.append("inferredSexReports=").append(inferredSexReports);
        sb.append(", relatednessReports=").append(relatednessReports);
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

    public List<RelatednessReport> getRelatednessReports() {
        return relatednessReports;
    }

    public IndividualQualityControl setRelatednessReports(List<RelatednessReport> relatednessReports) {
        this.relatednessReports = relatednessReports;
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
}

