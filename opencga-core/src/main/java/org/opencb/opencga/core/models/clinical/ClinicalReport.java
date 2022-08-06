package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalReport {

    @DataField(id = "title", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_TITLE)
    private String title;

    @DataField(id = "overview", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_OVERVIEW)
    private String overview;

    @DataField(id = "discussion", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_DISCUSSION)
    private ClinicalDiscussion discussion;

    @DataField(id = "logo", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_LOGO)
    private String logo;

    @DataField(id = "signedBy", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_SIGNED_BY)
    private String signedBy;

    @DataField(id = "signature", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_SIGNATURE)
    private String signature;

    @DataField(id = "date", indexed = true,
            description = FieldConstants.CLINICAL_REPORT_DATE)
    private String date;

    public ClinicalReport() {
    }

    public ClinicalReport(String title, String overview, ClinicalDiscussion discussion, String logo, String signedBy,
                          String signature, String date) {
        this.title = title;
        this.overview = overview;
        this.discussion = discussion;
        this.logo = logo;
        this.signedBy = signedBy;
        this.signature = signature;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalReport{");
        sb.append("title='").append(title).append('\'');
        sb.append(", overview='").append(overview).append('\'');
        sb.append(", discussion='").append(discussion).append('\'');
        sb.append(", logo='").append(logo).append('\'');
        sb.append(", signedBy='").append(signedBy).append('\'');
        sb.append(", signature='").append(signature).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getTitle() {
        return title;
    }

    public ClinicalReport setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getOverview() {
        return overview;
    }

    public ClinicalReport setOverview(String overview) {
        this.overview = overview;
        return this;
    }

    public ClinicalDiscussion getDiscussion() {
        return discussion;
    }

    public ClinicalReport setDiscussion(ClinicalDiscussion discussion) {
        this.discussion = discussion;
        return this;
    }

    public String getLogo() {
        return logo;
    }

    public ClinicalReport setLogo(String logo) {
        this.logo = logo;
        return this;
    }

    public String getSignedBy() {
        return signedBy;
    }

    public ClinicalReport setSignedBy(String signedBy) {
        this.signedBy = signedBy;
        return this;
    }

    public String getSignature() {
        return signature;
    }

    public ClinicalReport setSignature(String signature) {
        this.signature = signature;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalReport setDate(String date) {
        this.date = date;
        return this;
    }
}
