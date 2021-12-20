
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
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
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

    /**
     * Family is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.FAMILY_ID_DESCRIPTION)
    private String id;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the Family creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */
    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "name", indexed = true,
            description = FieldConstants.FAMILY_NAME)
    private String name;

    @DataField(id = "members", indexed = true,
            description = FieldConstants.FAMILY_MEMBERS)
    private List<Individual> members;

    @DataField(id = "phenotypes", indexed = true,
            description = FieldConstants.GENERIC_PHENOTYPES_DESCRIPTION)
    private List<Phenotype> phenotypes;

    @DataField(id = "disorders", indexed = true,
            description = FieldConstants.FAMILY_DISORDERS)
    private List<Disorder> disorders;

    @DataField(id = "qualityControl", indexed = true,
            description = FieldConstants.GENERIC_QUALITY_CONTROL)
    private FamilyQualityControl qualityControl;

    /**
     * String representing when the Family was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "creationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    /**
     * String representing when was the last time the Family was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.FAMILY_EXPECTED_SIZE)
    private int expectedSize;

    /**
     * An string to describe the properties of the Family.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;


    /**
     * An integer describing the current data release.
     *
     * @apiNote Internal
     */
    @DataField(id = "release", managed = true, indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    /**
     * An integer describing the current version.
     *
     * @apiNote Internal
     */
    @DataField(id = "version", managed = true, indexed = true,
            description = FieldConstants.GENERIC_VERSION_DESCRIPTION)
    private int version;

    /**
     * An object describing the status of the Family.
     *
     * @apiNote
     */
    @DataField(id = "status", since = "2.0",
            description = FieldConstants.GENERIC_CUSTOM_STATUS)
    private CustomStatus status;


    /**
     * An object describing the internal information of the Family. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "internal", since = "2.0", description = FieldConstants.GENERIC_INTERNAL, navigate = false)
    private FamilyInternal internal;

    @DataField(id = "roles", description = FieldConstants.FAMILY_ROLES)
    private Map<String, Map<String, FamiliarRelationship>> roles;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    @DataField(id = "attributes", since = "1.0",
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Family() {
    }

    public Family(String id, String name, List<Phenotype> phenotypes, List<Disorder> disorders, List<Individual> members,
                  String description, int expectedSize, List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this(id, name, phenotypes, disorders, members, TimeUtils.getTime(), TimeUtils.getTime(), description, expectedSize, -1, 1,
                annotationSets, new CustomStatus(), null, null, attributes);
    }

    public Family(String id, String name, List<Phenotype> phenotypes, List<Disorder> disorders, List<Individual> members,
                  String creationDate, String modificationDate, String description, int expectedSize, int release, int version,
                  List<AnnotationSet> annotationSets, CustomStatus status, FamilyInternal internal,
                  Map<String, Map<String, FamiliarRelationship>> roles, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.phenotypes = ObjectUtils.defaultIfNull(phenotypes, new ArrayList<>());
        this.disorders = ObjectUtils.defaultIfNull(disorders, new ArrayList<>());
        this.members = ObjectUtils.defaultIfNull(members, new ArrayList<>());
        this.creationDate = ObjectUtils.defaultIfNull(creationDate, TimeUtils.getTime());
        this.modificationDate = modificationDate;
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

    @Override
    public String getUuid() {
        return uuid;
    }

    public Family setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
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
            return name();
        }

        public String getSnomedCtId() {
            return snomedCtId;
        }

        public String getIsA() {
            return isA;
        }
    }
}
