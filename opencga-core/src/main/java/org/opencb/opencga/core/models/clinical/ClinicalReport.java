package org.opencb.opencga.core.models.clinical;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.interpretation.MiniPubmed;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.file.File;

import java.util.List;
import java.util.Map;

public class ClinicalReport {

    @DataField(id = "overview", description = FieldConstants.CLINICAL_REPORT_OVERVIEW)
    private String overview;

    @DataField(id = "discussion", description = FieldConstants.CLINICAL_REPORT_DISCUSSION)
    private ClinicalDiscussion discussion;

    @DataField(id = "recommendation", description = "Recommendation for the report")
    private String recommendation;

    @DataField(id = "methodology", description = "Methodology used to generate the report")
    private String methodology;

    @DataField(id = "limitations", description = "Limitations of the report")
    private String limitations;

    @DataField(id = "signatures", description = "Signatures of the report")
    private List<Signature> signatures;

    @DataField(id = "date", description = FieldConstants.CLINICAL_REPORT_DATE)
    private String date;

    @DataField(id = "comments", description = FieldConstants.CLINICAL_REPORT_COMMENTS, since = "2.12.0")
    private List<ClinicalComment> comments;

    @DataField(id = "references", description = "References to PubMed articles supporting the report")
    private List<MiniPubmed> references;

    @DataField(id = "images", description = "Images related to the report")
    private List<String> images;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    // Deprecated fields
    @Deprecated
    @DataField(id = "title", description = FieldConstants.CLINICAL_REPORT_TITLE, deprecated = true)
    private String title;

    @Deprecated
    @DataField(id = "logo", description = FieldConstants.CLINICAL_REPORT_LOGO, deprecated = true)
    private String logo;

    @Deprecated
    @DataField(id = "signedBy", description = FieldConstants.CLINICAL_REPORT_SIGNED_BY, deprecated = true)
    private String signedBy;

    @Deprecated
    @DataField(id = "signature", description = FieldConstants.CLINICAL_REPORT_SIGNATURE, deprecated = true)
    private String signature;

    @Deprecated
    @DataField(id = "supportingEvidences", description = FieldConstants.CLINICAL_REPORT_SUPPORTING_EVIDENCES, since = "2.12.0",
            deprecated = true)
    private List<File> supportingEvidences;

    @Deprecated
    @DataField(id = "files", description = FieldConstants.CLINICAL_REPORT_FILES, since = "2.12.0", deprecated = true)
    private List<File> files;

    public ClinicalReport() {
    }

    public ClinicalReport(String overview, ClinicalDiscussion discussion, String recommendation, String methodology, String limitations,
                          List<Signature> signatures, String date, List<ClinicalComment> comments, List<MiniPubmed> references,
                          List<String> images, Map<String, Object> attributes) {
        this.overview = overview;
        this.discussion = discussion;
        this.recommendation = recommendation;
        this.methodology = methodology;
        this.limitations = limitations;
        this.signatures = signatures;
        this.date = date;
        this.comments = comments;
        this.references = references;
        this.images = images;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalReport{");
        sb.append("overview='").append(overview).append('\'');
        sb.append(", discussion=").append(discussion);
        sb.append(", recommendation='").append(recommendation).append('\'');
        sb.append(", methodology='").append(methodology).append('\'');
        sb.append(", limitations='").append(limitations).append('\'');
        sb.append(", signatures=").append(signatures);
        sb.append(", date='").append(date).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", references=").append(references);
        sb.append(", images=").append(images);
        sb.append(", attributes=").append(attributes);
        sb.append(", title='").append(title).append('\'');
        sb.append(", logo='").append(logo).append('\'');
        sb.append(", signedBy='").append(signedBy).append('\'');
        sb.append(", signature='").append(signature).append('\'');
        sb.append(", supportingEvidences=").append(supportingEvidences);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
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

    public String getRecommendation() {
        return recommendation;
    }

    public ClinicalReport setRecommendation(String recommendation) {
        this.recommendation = recommendation;
        return this;
    }

    public String getMethodology() {
        return methodology;
    }

    public ClinicalReport setMethodology(String methodology) {
        this.methodology = methodology;
        return this;
    }

    public String getLimitations() {
        return limitations;
    }

    public ClinicalReport setLimitations(String limitations) {
        this.limitations = limitations;
        return this;
    }

    public List<Signature> getSignatures() {
        return signatures;
    }

    public ClinicalReport setSignatures(List<Signature> signatures) {
        this.signatures = signatures;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalReport setDate(String date) {
        this.date = date;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public ClinicalReport setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public List<MiniPubmed> getReferences() {
        return references;
    }

    public ClinicalReport setReferences(List<MiniPubmed> references) {
        this.references = references;
        return this;
    }

    public List<String> getImages() {
        return images;
    }

    public ClinicalReport setImages(List<String> images) {
        this.images = images;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalReport setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    @Deprecated
    public String getTitle() {
        return title;
    }

    @Deprecated
    public ClinicalReport setTitle(String title) {
        throw new UnsupportedOperationException("Title field is deprecated. Add title to Study clinical report configuration instead.");
    }

    @Deprecated
    public String getLogo() {
        return logo;
    }

    @Deprecated
    public ClinicalReport setLogo(String logo) {
        throw new UnsupportedOperationException("Logo field is deprecated. Add logo to Study clinical report configuration instead.");
    }

    @Deprecated
    public String getSignedBy() {
        return CollectionUtils.isNotEmpty(signatures) ? signatures.get(0).getSignedBy() : signedBy;
    }

    @Deprecated
    public ClinicalReport setSignedBy(String signedBy) {
        throw new UnsupportedOperationException("SignedBy field is deprecated. Use array of signatures instead.");
    }

    @Deprecated
    public String getSignature() {
        return CollectionUtils.isNotEmpty(signatures) ? signatures.get(0).getSignature() : signature;
    }

    @Deprecated
    public ClinicalReport setSignature(String signature) {
        throw new UnsupportedOperationException("Signature field is deprecated. Use array of signatures instead.");
    }

    @Deprecated
    public List<File> getSupportingEvidences() {
        return supportingEvidences;
    }

    @Deprecated
    public ClinicalReport setSupportingEvidences(List<File> supportingEvidences) {
        throw new UnsupportedOperationException("Supporting evidences field is deprecated. Use references and images instead.");
    }

    @Deprecated
    public List<File> getFiles() {
        return files;
    }

    @Deprecated
    public ClinicalReport setFiles(List<File> files) {
        throw new UnsupportedOperationException("Files field is deprecated. Moved to ClinicalAnalysis.reportedFiles.");
    }
}
