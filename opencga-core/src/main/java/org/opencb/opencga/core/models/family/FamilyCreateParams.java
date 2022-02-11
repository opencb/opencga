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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.individual.Individual;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FamilyCreateParams {

    private String id;
    private String name;
    private String description;
    private String creationDate;
    private String modificationDate;
    private List<IndividualCreateParams> members;

    private Integer expectedSize;

    private StatusParams status;
    private Map<String, Object> attributes;
    private List<AnnotationSet> annotationSets;

    public FamilyCreateParams() {
    }

    public FamilyCreateParams(String id, String name, String description, String creationDate, String modificationDate,
                              List<IndividualCreateParams> members, Integer expectedSize, Map<String, Object> attributes,
                              List<AnnotationSet> annotationSets, StatusParams status) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.members = members;
        this.expectedSize = expectedSize;
        this.attributes = attributes;
        this.annotationSets = annotationSets;
        this.status = status;
    }

    public static FamilyCreateParams of(Family family) {
        return new FamilyCreateParams(family.getId(), family.getName(), family.getDescription(), family.getCreationDate(),
                family.getModificationDate(),
                family.getMembers() != null
                        ? family.getMembers().stream().map(IndividualCreateParams::of).collect(Collectors.toList())
                        : Collections.emptyList(),
                family.getExpectedSize(), family.getAttributes(), family.getAnnotationSets(), StatusParams.of(family.getStatus()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", members=").append(members);
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", status=").append(status);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    public Family toFamily() {

        List<Individual> relatives = null;
        if (members != null) {
            relatives = new ArrayList<>(members.size());
            for (IndividualCreateParams member : members) {
                relatives.add(member.toIndividual());
            }
        }

        String familyId = StringUtils.isEmpty(id) ? name : id;
        String familyName = StringUtils.isEmpty(name) ? familyId : name;
        int familyExpectedSize = expectedSize != null ? expectedSize : -1;
        return new Family(familyId, familyName, null, null, relatives, creationDate, modificationDate, description, familyExpectedSize, 1,
                1, annotationSets, status != null ? status.toStatus() : null, null, null, attributes);
    }

    public String getId() {
        return id;
    }

    public FamilyCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public FamilyCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FamilyCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FamilyCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public FamilyCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<IndividualCreateParams> getMembers() {
        return members;
    }

    public FamilyCreateParams setMembers(List<IndividualCreateParams> members) {
        this.members = members;
        return this;
    }

    public Integer getExpectedSize() {
        return expectedSize;
    }

    public FamilyCreateParams setExpectedSize(Integer expectedSize) {
        this.expectedSize = expectedSize;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FamilyCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public StatusParams getStatus() {
        return status;
    }

    public FamilyCreateParams setStatus(StatusParams status) {
        this.status = status;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public FamilyCreateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }
}
