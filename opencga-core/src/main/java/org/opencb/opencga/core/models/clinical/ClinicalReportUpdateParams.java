package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileReferenceParam;

import java.util.List;
import java.util.stream.Collectors;

public class ClinicalReportUpdateParams {

    @DataField(id = "title", indexed = true, description = FieldConstants.CLINICAL_REPORT_TITLE)
    private String title;

    @DataField(id = "overview", indexed = true, description = FieldConstants.CLINICAL_REPORT_OVERVIEW)
    private String overview;

    @DataField(id = "methodology", since = "2.5.0", description = FieldConstants.CLINICAL_REPORT_METHODOLOGY)
    private String methodology;

    @DataField(id = "result", since = "2.5.0", description = FieldConstants.CLINICAL_REPORT_RESULT)
    private String result;

    @DataField(id = "discussion", indexed = true, description = FieldConstants.CLINICAL_REPORT_DISCUSSION)
    private ClinicalDiscussion discussion;

    @DataField(id = "logo", indexed = true, description = FieldConstants.CLINICAL_REPORT_LOGO)
    private String logo;

    @DataField(id = "notes", since = "2.5.0", description = FieldConstants.CLINICAL_REPORT_NOTES)
    private String notes;

    @DataField(id = "disclaimer", since = "2.5.0", description = FieldConstants.CLINICAL_REPORT_DISCLAIMER)
    private String disclaimer;

    @DataField(id = "annexes", since = "2.5.0", description = FieldConstants.CLINICAL_REPORT_ANNEXES)
    private List<FileReferenceParam> annexes;

    public ClinicalReportUpdateParams() {
    }

    public ClinicalReportUpdateParams(String title, String overview, String methodology, String result, ClinicalDiscussion discussion,
                                      String logo, String notes, String disclaimer, List<FileReferenceParam> annexes) {
        this.title = title;
        this.overview = overview;
        this.methodology = methodology;
        this.result = result;
        this.discussion = discussion;
        this.logo = logo;
        this.notes = notes;
        this.disclaimer = disclaimer;
        this.annexes = annexes;
    }

    public ClinicalReport toClinicalReport() {
        List<File> fileAnnexes = annexes != null
                ? annexes.stream().map(FileReferenceParam::toFile).collect(Collectors.toList())
                : null;
        return new ClinicalReport(title, overview, methodology, result, discussion, logo, notes, disclaimer, fileAnnexes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalReportUpdateParams{");
        sb.append("title='").append(title).append('\'');
        sb.append(", overview='").append(overview).append('\'');
        sb.append(", methodology='").append(methodology).append('\'');
        sb.append(", result='").append(result).append('\'');
        sb.append(", discussion=").append(discussion);
        sb.append(", logo='").append(logo).append('\'');
        sb.append(", notes='").append(notes).append('\'');
        sb.append(", disclaimer='").append(disclaimer).append('\'');
        sb.append(", annexes=").append(annexes);
        sb.append('}');
        return sb.toString();
    }

    public String getTitle() {
        return title;
    }

    public ClinicalReportUpdateParams setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getOverview() {
        return overview;
    }

    public ClinicalReportUpdateParams setOverview(String overview) {
        this.overview = overview;
        return this;
    }

    public String getMethodology() {
        return methodology;
    }

    public ClinicalReportUpdateParams setMethodology(String methodology) {
        this.methodology = methodology;
        return this;
    }

    public String getResult() {
        return result;
    }

    public ClinicalReportUpdateParams setResult(String result) {
        this.result = result;
        return this;
    }

    public ClinicalDiscussion getDiscussion() {
        return discussion;
    }

    public ClinicalReportUpdateParams setDiscussion(ClinicalDiscussion discussion) {
        this.discussion = discussion;
        return this;
    }

    public String getLogo() {
        return logo;
    }

    public ClinicalReportUpdateParams setLogo(String logo) {
        this.logo = logo;
        return this;
    }

    public String getNotes() {
        return notes;
    }

    public ClinicalReportUpdateParams setNotes(String notes) {
        this.notes = notes;
        return this;
    }

    public String getDisclaimer() {
        return disclaimer;
    }

    public ClinicalReportUpdateParams setDisclaimer(String disclaimer) {
        this.disclaimer = disclaimer;
        return this;
    }

    public List<FileReferenceParam> getAnnexes() {
        return annexes;
    }

    public ClinicalReportUpdateParams setAnnexes(List<FileReferenceParam> annexes) {
        this.annexes = annexes;
        return this;
    }
}
