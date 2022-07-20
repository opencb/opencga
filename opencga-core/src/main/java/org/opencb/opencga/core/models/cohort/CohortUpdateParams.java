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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CohortUpdateParams {

    @DataField(description = ParamConstants.COHORT_UPDATE_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.COHORT_UPDATE_PARAMS_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.COHORT_UPDATE_PARAMS_TYPE_DESCRIPTION)
    private Enums.CohortType type;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;
    @DataField(description = ParamConstants.COHORT_UPDATE_PARAMS_SAMPLES_DESCRIPTION)
    private List<SampleReferenceParam> samples;
    @DataField(description = ParamConstants.COHORT_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION)
    private List<AnnotationSet> annotationSets;
    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;
    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private StatusParams status;

    public CohortUpdateParams() {
    }

    public CohortUpdateParams(String id, String name, Enums.CohortType type, String description, String creationDate,
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

    public Cohort toCohort() {
        return new Cohort(id, name, type, creationDate, modificationDate,
                description,
                samples != null ?
                        samples.stream().map(s -> new Sample().setId(s.getId()).setUuid(s.getUuid())).collect(Collectors.toList())
                        : null, samples != null ? samples.size() : 0, annotationSets, 1, status.toStatus(), new CohortInternal(), attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortUpdateParams{");
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

    public CohortUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public CohortUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public Enums.CohortType getType() {
        return type;
    }

    public CohortUpdateParams setType(Enums.CohortType type) {
        this.type = type;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CohortUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public CohortUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public CohortUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<SampleReferenceParam> getSamples() {
        return samples;
    }

    public CohortUpdateParams setSamples(List<SampleReferenceParam> samples) {
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

    public StatusParams getStatus() {
        return status;
    }

    public CohortUpdateParams setStatus(StatusParams status) {
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
