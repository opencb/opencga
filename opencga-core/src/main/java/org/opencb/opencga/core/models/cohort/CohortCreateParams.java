package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CohortCreateParams {

    public String id;
    public Enums.CohortType type;
    public String description;
    public List<String> samples;
    public List<AnnotationSet> annotationSets;
    public Map<String, Object> attributes;

    public CohortCreateParams() {
    }

    public CohortCreateParams(String id, Enums.CohortType type, String description, List<String> samples,
                              List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.type = type;
        this.description = description;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    public static CohortCreateParams of(Cohort cohort) {
        return new CohortCreateParams(cohort.getId(), cohort.getType(), cohort.getDescription(),
                cohort.getSamples() != null
                        ? cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                        : Collections.emptyList(),
                cohort.getAnnotationSets(), cohort.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type=").append(type);
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public CohortCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public Enums.CohortType getType() {
        return type;
    }

    public CohortCreateParams setType(Enums.CohortType type) {
        this.type = type;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CohortCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public CohortCreateParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public CohortCreateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public CohortCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
