
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

import org.apache.commons.lang3.ObjectUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.individual.Individual;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 02/05/17.
 */
public class Family extends Annotable {

    private String id;
    private String name;
    private String uuid;
    private List<Individual> members;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;

    private FamilyQualityControl qualityControl;

    private String creationDate;
    private String modificationDate;
    private int expectedSize;
    private String description;

    private int release;
    private int version;
    private CustomStatus status;
    private FamilyInternal internal;
    private Map<String, Map<String, FamiliarRelationship>> roles;
    private Map<String, Object> attributes;

    public enum FamiliarRelationship {
        MOTHER("", "mother"),
        FATHER("", "father"),
        STEP_MOTHER("", "mother"),
        STEP_FATHER("", "father"),
        IDENTICAL_TWIN("", "twin"),
        FRATERNAL_TWIN("", "twin"),
        FULL_SIBLING("", "sibling"),
        HALF_SIBLING("", "sibling"),
        STEP_SIBLING("", "sibling"),
        SISTER("", "sister"),
        BROTHER("", "brother"),
        STEP_SISTER("", "sister"),
        STEP_BROTHER("", "brother"),
        SON("", "son"),
        DAUGHTER("", "daughter"),

        CHILD_OF_UNKNOWN_SEX("", "child"),

        UNCLE("", "uncle"),
        AUNT("", "aunt"),
        MATERNAL_AUNT("", "aunt"),
        MATERNAL_UNCLE("", "uncle"),
        PATERNAL_AUNT("", "aunt"),
        PATERNAL_UNCLE("", "uncle"),
        NEPHEW("", "nephew"),
        NIECE("", "niece"),
        GRANDSON("", "grandson"),
        GRANDCHILD("", "grandchild"),
        GRANDDAUGHTER("", "granddaughter"),
        GRANDFATHER("", "grandfather"),
        GRANDMOTHER("", "grandmother"),
        MATERNAL_GRANDMOTHER("", "grandmother"),
        PATERNAL_GRANDMOTHER("", "grandmother"),
        MATERNAL_GRANDFATHER("", "grandfather"),
        PATERNAL_GRANDFATHER("", "grandfather"),
        GREAT_GRANDFATHER("", "great-grandfather"),
        GREAT_GRANDMOTHER("", "great-grandmother"),
        DOUBLE_FIRST_COUSING("", "cousin"),
        COUSIN("", "cousin"),
        MALE_COUSIN("", "cousin"),
        FEMALE_COUSIN("", "cousin"),
        SECOND_COUSIN("", "cousin"),
        MALE_SECOND_COUSIN("", "cousin"),
        FEMALE_SECOND_COUSIN("", "cousin"),
        SPOUSE("", "spouse"),
        HUSBAND("", "husband"),
        OTHER("", "other"),
        UNKNOWN("", "unknown"),
        UNRELATED("", "unrelated"),

        PROBAND("", "proband");

        private final String snomedCtId;
        private final String isA;

        FamiliarRelationship(String snomedCtId, String isA) {
            this.snomedCtId = snomedCtId;
            this.isA = isA;
        }

        public String getId() {
            return this.name();
        }

        public String getSnomedCtId() {
            return snomedCtId;
        }

        public String getIsA() {
            return isA;
        }
    }

    public Family() {
    }

    public Family(String id, String name, List<Phenotype> phenotypes, List<Disorder> disorders, List<Individual> members,
                  String description, int expectedSize, List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this(id, name, phenotypes, disorders, members, TimeUtils.getTime(), description, expectedSize, -1, 1, annotationSets,
                new CustomStatus(), null, null, attributes);
    }

    public Family(String id, String name, List<Phenotype> phenotypes, List<Disorder> disorders, List<Individual> members,
                  String creationDate, String description, int expectedSize, int release, int version, List<AnnotationSet> annotationSets,
                  CustomStatus status, FamilyInternal internal, Map<String, Map<String, FamiliarRelationship>> roles,
                  Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.phenotypes = ObjectUtils.defaultIfNull(phenotypes, new ArrayList<>());
        this.disorders = ObjectUtils.defaultIfNull(disorders, new ArrayList<>());
        this.members = ObjectUtils.defaultIfNull(members, new ArrayList<>());
        this.creationDate = ObjectUtils.defaultIfNull(creationDate, TimeUtils.getTime());
        this.expectedSize = expectedSize;
        this.description = description;
        this.release = release;
        this.version = version;
        this.annotationSets = ObjectUtils.defaultIfNull(annotationSets, new ArrayList<>());
        this.status = status;
        this.internal = internal;
        this.roles = roles;
        this.attributes = ObjectUtils.defaultIfNull(attributes, new HashMap<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Family{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", members=").append(members);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", description='").append(description).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
        sb.append(", roles=").append(roles);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Family setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public Family setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Family setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Family setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Family setName(String name) {
        this.name = name;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public Family setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public Family setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public List<Individual> getMembers() {
        return members;
    }

    public Family setMembers(List<Individual> members) {
        this.members = members;
        return this;
    }

    public FamilyQualityControl getQualityControl() {
        return qualityControl;
    }

    public Family setQualityControl(FamilyQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Family setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Family setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public FamilyInternal getInternal() {
        return internal;
    }

    public Family setInternal(FamilyInternal internal) {
        this.internal = internal;
        return this;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public Family setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Family setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Family setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Family setVersion(int version) {
        this.version = version;
        return this;
    }

    public CustomStatus getStatus() {
        return status;
    }

    public Family setStatus(CustomStatus status) {
        this.status = status;
        return this;
    }

    public Map<String, Map<String, FamiliarRelationship>> getRoles() {
        return roles;
    }

    public Family setRoles(Map<String, Map<String, FamiliarRelationship>> roles) {
        this.roles = roles;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Family setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
