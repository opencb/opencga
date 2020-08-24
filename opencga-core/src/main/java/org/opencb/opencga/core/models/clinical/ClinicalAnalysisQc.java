package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.Comment;
import org.opencb.biodata.models.clinical.interpretation.Analyst;

import java.util.List;

public class ClinicalAnalysisQc {

    private Quality quality;
    private ClinicalAnalysisVariantQc variant;
    private ClinicalAnalysisAlignmentQc alignment;
    private Analyst analyst;
    private List<Comment> comments;
    private String date;

    public ClinicalAnalysisQc() {
    }

    public ClinicalAnalysisQc(Quality quality, ClinicalAnalysisVariantQc variant, ClinicalAnalysisAlignmentQc alignment, Analyst analyst,
                              List<Comment> comments, String date) {
        this.quality = quality;
        this.variant = variant;
        this.alignment = alignment;
        this.analyst = analyst;
        this.comments = comments;
        this.date = date;
    }

    public enum Quality {
        HIGH,
        MEDIUM,
        LOW,
        UNKNOWN
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQc{");
        sb.append("quality=").append(quality);
        sb.append(", variant=").append(variant);
        sb.append(", alignment=").append(alignment);
        sb.append(", analyst=").append(analyst);
        sb.append(", comments=").append(comments);
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Quality getQuality() {
        return quality;
    }

    public ClinicalAnalysisQc setQuality(Quality quality) {
        this.quality = quality;
        return this;
    }

    public ClinicalAnalysisVariantQc getVariant() {
        return variant;
    }

    public ClinicalAnalysisQc setVariant(ClinicalAnalysisVariantQc variant) {
        this.variant = variant;
        return this;
    }

    public ClinicalAnalysisAlignmentQc getAlignment() {
        return alignment;
    }

    public ClinicalAnalysisQc setAlignment(ClinicalAnalysisAlignmentQc alignment) {
        this.alignment = alignment;
        return this;
    }

    public Analyst getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisQc setAnalyst(Analyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalAnalysisQc setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalAnalysisQc setDate(String date) {
        this.date = date;
        return this;
    }
}
