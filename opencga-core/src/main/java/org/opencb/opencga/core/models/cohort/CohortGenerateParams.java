package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;
import java.util.Map;

public class CohortGenerateParams {

    private String id;
    private Enums.CohortType type;
    private String description;
    private List<AnnotationSet> annotationSets;
    private CustomStatusParams status;
    private Map<String, Object> attributes;

    public CohortGenerateParams() {
    }

    public CohortGenerateParams(String id, Enums.CohortType type, String description, List<AnnotationSet> annotationSets,
                                CustomStatusParams status, Map<String, Object> attributes) {
        this.id = id;
        this.type = type;
        this.description = description;
        this.annotationSets = annotationSets;
        this.status = status;
        this.attributes = attributes;
    }

    public Cohort toCohort() {
        return new Cohort(id, type, null, description, null, annotationSets, 0, status != null ? status.toCustomStatus() : null, null,
                attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortGenerateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type=").append(type);
        sb.append(", description='").append(description).append('\'');
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

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public CohortGenerateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public CohortGenerateParams setStatus(CustomStatusParams status) {
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
