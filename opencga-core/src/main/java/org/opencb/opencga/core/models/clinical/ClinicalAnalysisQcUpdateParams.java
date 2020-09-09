package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ClinicalAnalysisQcUpdateParams {

    private ClinicalAnalysisQc.Quality quality;
    private ClinicalAnalysisVariantQc variant;
    private ClinicalAnalysisAlignmentQc alignment;
    private ClinicalAnalyst analyst;
    private List<ClinicalComment> comments;

    public ClinicalAnalysisQcUpdateParams() {
    }

    public ClinicalAnalysisQcUpdateParams(ClinicalAnalysisQc.Quality quality, ClinicalAnalysisVariantQc variant,
                                          ClinicalAnalysisAlignmentQc alignment, ClinicalAnalyst analyst, List<ClinicalComment> comments) {
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

    public ClinicalAnalyst getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisQcUpdateParams setAnalyst(ClinicalAnalyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public ClinicalAnalysisQcUpdateParams setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }
}
