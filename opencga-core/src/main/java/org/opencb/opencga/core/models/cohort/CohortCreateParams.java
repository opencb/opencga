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
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CohortCreateParams {

    @DataField(description = ParamConstants.COHORT_CREATE_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.COHORT_CREATE_PARAMS_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.COHORT_CREATE_PARAMS_TYPE_DESCRIPTION)
    private Enums.CohortType type;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;
    @DataField(description = ParamConstants.COHORT_CREATE_PARAMS_SAMPLES_DESCRIPTION)
    private List<SampleReferenceParam> samples;
    @DataField(description = ParamConstants.COHORT_CREATE_PARAMS_ANNOTATION_SETS_DESCRIPTION)
    private List<AnnotationSet> annotationSets;
    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;
    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private StatusParams status;

    public CohortCreateParams() {
    }

    public CohortCreateParams(String id, String name, Enums.CohortType type, String description, String creationDate,
                              String modificationDate, List<SampleReferenceParam> samples, List<AnnotationSet> annotationSets,
                              Map<String, Object> attributes, StatusParams status) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
        this.status = status;
    }

    public static CohortCreateParams of(Cohort cohort) {
        return new CohortCreateParams(cohort.getId(), "", cohort.getType(), cohort.getDescription(), cohort.getCreationDate(),
                cohort.getModificationDate(), cohort.getSamples() != null
                ? cohort.getSamples().stream().map(s -> new SampleReferenceParam(s.getId(), s.getUuid()))
                .collect(Collectors.toList())
                : Collections.emptyList(),
                cohort.getAnnotationSets(), cohort.getAttributes(), StatusParams.of(cohort.getStatus()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name=").append(name);
        sb.append(", type=").append(type);
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
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

    public String getName() {
        return name;
    }

    public CohortCreateParams setName(String name) {
        this.name = name;
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

    public String getCreationDate() {
        return creationDate;
    }

    public CohortCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public CohortCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
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

    public StatusParams getStatus() {
        return status;
    }

    public CohortCreateParams setStatus(StatusParams status) {
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
