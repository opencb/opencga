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

package org.opencb.opencga.core.models.family;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class FamilyUpdateParams {
    private String id;
    private String name;
    private String description;
    private String creationDate;
    private String modificationDate;
    private List<IndividualReferenceParam> members;
    private Integer expectedSize;
    private FamilyQualityControl qualityControl;
    private CustomStatusParams status;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    public FamilyUpdateParams() {
    }

    public FamilyUpdateParams(String id, String name, String description, String creationDate, String modificationDate,
                              List<IndividualReferenceParam> members, Integer expectedSize, CustomStatusParams status,
                              FamilyQualityControl qualityControl, List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.members = members;
        this.expectedSize = expectedSize;
        this.status = status;
        this.qualityControl = qualityControl;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
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

    public Family toFamily() {
        return new Family(id, name, null, null,
                members != null
                        ? members.stream().map(m -> new Individual().setId(m.getId()).setUuid(m.getUuid())).collect(Collectors.toList())
                        : null,
                creationDate, modificationDate, description, members != null ? members.size() : 0, 1, 1, annotationSets,
                status != null ? status.toCustomStatus() : null, new FamilyInternal(), Collections.emptyMap(), attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", members=").append(members);
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", status=").append(status);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FamilyUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public FamilyUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FamilyUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FamilyUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public FamilyUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<IndividualReferenceParam> getMembers() {
        return members;
    }

    public FamilyUpdateParams setMembers(List<IndividualReferenceParam> members) {
        this.members = members;
        return this;
    }

    public Integer getExpectedSize() {
        return expectedSize;
    }

    public FamilyUpdateParams setExpectedSize(Integer expectedSize) {
        this.expectedSize = expectedSize;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public FamilyUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public FamilyQualityControl getQualityControl() {
        return qualityControl;
    }

    public FamilyUpdateParams setQualityControl(FamilyQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public FamilyUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FamilyUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
