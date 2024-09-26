package org.opencb.opencga.core.models.family;

import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.InternalQualityControl;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;

import java.util.List;
import java.util.Map;

public class FamilyInternalUpdateParams extends FamilyUpdateParams {

    private FamilyQualityControl qualityControl;
    private InternalQualityControl internal;

    public FamilyInternalUpdateParams() {
        super();
    }

    public FamilyInternalUpdateParams(FamilyQualityControl qualityControl, InternalQualityControl internal) {
        this.qualityControl = qualityControl;
        this.internal = internal;
    }

    public FamilyInternalUpdateParams(String id, String name, String description, String creationDate, String modificationDate,
                                      List<IndividualReferenceParam> members, Integer expectedSize, StatusParams status,
                                      List<AnnotationSet> annotationSets, Map<String, Object> attributes,
                                      FamilyQualityControl qualityControl, InternalQualityControl internal) {
        super(id, name, description, creationDate, modificationDate, members, expectedSize, status, annotationSets, attributes);
        this.qualityControl = qualityControl;
        this.internal = internal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyInternalUpdateParams{");
        sb.append("qualityControl=").append(qualityControl);
        sb.append(", internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    public FamilyQualityControl getQualityControl() {
        return qualityControl;
    }

    public FamilyInternalUpdateParams setQualityControl(FamilyQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public InternalQualityControl getInternal() {
        return internal;
    }

    public FamilyInternalUpdateParams setInternal(InternalQualityControl internal) {
        this.internal = internal;
        return this;
    }
}
