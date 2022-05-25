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


import org.opencb.biodata.models.common.Status;
import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@DataClass(id = "Cohort", since = "1.0",
        description = "Cohort data model hosts information about any cohort.")
public class Cohort extends Annotable {

    /**
     * Cohort ID is a mandatory parameter when creating a new Cohort, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    /**
     * Cohort name parameter.
     *
     */
    private String name;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the Cohort creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */
    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "type", indexed = true,
            description = FieldConstants.COHORT_TYPE)
    private Enums.CohortType type;

    /**
     * String representing when the Cohort was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "creationDate", indexed = true,
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    /**
     * String representing when was the last time the Cohort was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    /**
     * An string to describe the properties of the Cohort.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "samples", indexed = true,
            description = FieldConstants.COHORT_SAMPLES)
    private List<Sample> samples;

    @DataField(id = "numSamples", indexed = true,
            description = FieldConstants.COHORT_NUM_SAMPLES)
    private int numSamples;


    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    /**
     * An object describing the status of the Sample.
     *
     * @apiNote
     */

    @DataField(id = "status", indexed = true,
            description = FieldConstants.GENERIC_CUSTOM_STATUS)
    private Status status;


    /**
     * An object describing the internal information of the Sample. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */

    @DataField(id = "internal", indexed = true,
            description = FieldConstants.GENERIC_INTERNAL)
    private CohortInternal internal;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Cohort() {
    }

    public Cohort(String id, String name, Enums.CohortType type, String creationDate, String modificationDate, String description,
                  List<Sample> samples, int release, Map<String, Object> attributes) {
        this(id, name, type, creationDate, modificationDate, description, samples, 0, Collections.emptyList(), release, new Status(),
                null, attributes);
    }

    public Cohort(String id, String name, Enums.CohortType type, String creationDate, String modificationDate, String description,
                  List<Sample> samples, List<AnnotationSet> annotationSetList, int release, Map<String, Object> attributes) {
        this(id, name, type, creationDate, modificationDate, description, samples, 0, annotationSetList, release, new Status(),
                null, attributes);
    }

    public Cohort(String id, String name, Enums.CohortType type, String creationDate, String modificationDate, String description,
                  List<Sample> samples, int numSamples, List<AnnotationSet> annotationSets, int release, Status status,
                  CohortInternal internal, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.samples = samples;
        this.numSamples = numSamples;
        this.annotationSets = annotationSets;
        this.release = release;
        this.status = status;
        this.internal = internal;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Cohort{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", numSamples=").append(numSamples);
        sb.append(", release=").append(release);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Cohort)) {
            return false;
        }
        Cohort cohort = (Cohort) o;
        return release == cohort.release
                && Objects.equals(uuid, cohort.uuid)
                && Objects.equals(id, cohort.id)
                && type == cohort.type
                && Objects.equals(creationDate, cohort.creationDate)
                && Objects.equals(internal, cohort.internal)
                && Objects.equals(description, cohort.description)
                && Objects.equals(samples, cohort.samples)
                && Objects.equals(numSamples, cohort.numSamples)
                && Objects.equals(status, cohort.status)
                && Objects.equals(attributes, cohort.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, id, type, creationDate, internal, description, samples, numSamples, status, release, attributes);
    }

    @Override
    public Cohort setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public Cohort setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Cohort setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Cohort setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Cohort setName(String name) {
        this.name = name;
        return this;
    }

    public Enums.CohortType getType() {
        return type;
    }

    public Cohort setType(Enums.CohortType type) {
        this.type = type;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Cohort setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Cohort setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public CohortInternal getInternal() {
        return internal;
    }

    public Cohort setInternal(CohortInternal internal) {
        this.internal = internal;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Cohort setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public Cohort setSamples(List<Sample> samples) {
        this.samples = samples;
        return this;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public Cohort setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Cohort setRelease(int release) {
        this.release = release;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Cohort setStatus(Status status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Cohort setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
