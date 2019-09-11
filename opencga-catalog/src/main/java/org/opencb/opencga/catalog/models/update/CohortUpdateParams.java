package org.opencb.opencga.catalog.models.update;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.AnnotationSet;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class CohortUpdateParams {

    private String id;
    private String description;
    private List<String> samples;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    public CohortUpdateParams() {
    }

    public ObjectMap getUpdateMap() throws CatalogException {
        try {
            List<AnnotationSet> annotationSetList = this.annotationSets;
            this.annotationSets = null;

            ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));

            this.annotationSets = annotationSetList;
            if (this.annotationSets != null) {
                // We leave annotation sets as is so we don't need to make any more castings
                params.put("annotationSets", this.annotationSets);
            }

            return params;
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples='").append(samples).append('\'');
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public CohortUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
