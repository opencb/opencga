package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.StatusParams;

import java.util.List;
import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CohortGenerateParams {

    @DataField(description = ParamConstants.COHORT_GENERATE_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.COHORT_GENERATE_PARAMS_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.COHORT_GENERATE_PARAMS_TYPE_DESCRIPTION)
    private Enums.CohortType type;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;
    @DataField(description = ParamConstants.COHORT_GENERATE_PARAMS_ANNOTATION_SETS_DESCRIPTION)
    private List<AnnotationSet> annotationSets;
    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private StatusParams status;
    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public CohortGenerateParams() {
    }

    public CohortGenerateParams(String id, String name, Enums.CohortType type, String description, String creationDate,
                                String modificationDate, List<AnnotationSet> annotationSets, StatusParams status,
                                Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.annotationSets = annotationSets;
        this.status = status;
        this.attributes = attributes;
    }

    public Cohort toCohort() {
        return new Cohort(id, name, type, creationDate, modificationDate, description, null, 0, annotationSets,
                0, status != null ? status.toStatus() : null, null, attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortGenerateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name=").append(name);
        sb.append(", type=").append(type);
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", status=").append(status);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CohortGenerateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public CohortGenerateParams setName(String name) {
        this.name = name;
        return this;
    }

    public Enums.CohortType getType() {
        return type;
    }

    public CohortGenerateParams setType(Enums.CohortType type) {
        this.type = type;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CohortGenerateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public CohortGenerateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public CohortGenerateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public CohortGenerateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public StatusParams getStatus() {
        return status;
    }

    public CohortGenerateParams setStatus(StatusParams status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public CohortGenerateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
