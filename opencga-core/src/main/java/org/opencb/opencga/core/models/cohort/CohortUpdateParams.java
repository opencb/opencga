package org.opencb.opencga.core.models.cohort;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class CohortUpdateParams {

    private String id;
    private String description;
    private List<String> samples;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public CohortUpdateParams() {
    }

    public CohortUpdateParams(String id, String description, List<String> samples, List<AnnotationSet> annotationSets,
                              Map<String, Object> attributes, CustomStatusParams status) {
        this.id = id;
        this.description = description;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
        this.status = status;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        List<AnnotationSet> annotationSetList = this.annotationSets;
        this.annotationSets = null;

        ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));

        this.annotationSets = annotationSetList;
        if (this.annotationSets != null) {
            // We leave annotation sets as is so we don't need to make any more castings
            params.put("annotationSets", this.annotationSets);
        }

        return params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CohortUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CohortUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public CohortUpdateParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public CohortUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public CohortUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public CohortUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
