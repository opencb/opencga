/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CohortCreateParams {

    private String id;
    private Enums.CohortType type;
    private String description;
    private List<SampleReferenceParam> samples;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public CohortCreateParams() {
    }

    public CohortCreateParams(String id, Enums.CohortType type, String description, List<SampleReferenceParam> samples,
                              List<AnnotationSet> annotationSets, Map<String, Object> attributes, CustomStatusParams status) {
        this.id = id;
        this.type = type;
        this.description = description;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
        this.status = status;
    }

    public static CohortCreateParams of(Cohort cohort) {
        return new CohortCreateParams(cohort.getId(), cohort.getType(), cohort.getDescription(),
                cohort.getSamples() != null
                        ? cohort.getSamples().stream().map(s -> new SampleReferenceParam(s.getId(), s.getUuid()))
                        .collect(Collectors.toList())
                        : Collections.emptyList(),
                cohort.getAnnotationSets(), cohort.getAttributes(), CustomStatusParams.of(cohort.getStatus()));
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
        sb.append(", status=").append(status);
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

    public List<SampleReferenceParam> getSamples() {
        return samples;
    }

    public CohortCreateParams setSamples(List<SampleReferenceParam> samples) {
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

    public CustomStatusParams getStatus() {
        return status;
    }

    public CohortCreateParams setStatus(CustomStatusParams status) {
        this.status = status;
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
