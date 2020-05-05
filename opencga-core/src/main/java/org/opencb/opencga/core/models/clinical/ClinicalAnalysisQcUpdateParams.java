package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.interpretation.Analyst;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ClinicalAnalysisQcUpdateParams {

    private ClinicalAnalysisQc.Quality quality;
    private ClinicalAnalysisVariantQc variant;
    private ClinicalAnalysisAlignmentQc alignment;
    private Analyst analyst;
    private List<Comment> comments;

    public ClinicalAnalysisQcUpdateParams() {
    }

    public ClinicalAnalysisQcUpdateParams(ClinicalAnalysisQc.Quality quality, ClinicalAnalysisVariantQc variant,
                                          ClinicalAnalysisAlignmentQc alignment, Analyst analyst, List<Comment> comments) {
        this.quality = quality;
        this.variant = variant;
        this.alignment = alignment;
        this.analyst = analyst;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQcUpdateParams{");
        sb.append("quality=").append(quality);
        sb.append(", variant=").append(variant);
        sb.append(", alignment=").append(alignment);
        sb.append(", analyst=").append(analyst);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    public ClinicalAnalysisQc.Quality getQuality() {
        return quality;
    }

    public ClinicalAnalysisQcUpdateParams setQuality(ClinicalAnalysisQc.Quality quality) {
        this.quality = quality;
        return this;
    }

    public ClinicalAnalysisVariantQc getVariant() {
        return variant;
    }

    public ClinicalAnalysisQcUpdateParams setVariant(ClinicalAnalysisVariantQc variant) {
        this.variant = variant;
        return this;
    }

    public ClinicalAnalysisAlignmentQc getAlignment() {
        return alignment;
    }

    public ClinicalAnalysisQcUpdateParams setAlignment(ClinicalAnalysisAlignmentQc alignment) {
        this.alignment = alignment;
        return this;
    }

    public Analyst getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisQcUpdateParams setAnalyst(Analyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalAnalysisQcUpdateParams setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }
}
