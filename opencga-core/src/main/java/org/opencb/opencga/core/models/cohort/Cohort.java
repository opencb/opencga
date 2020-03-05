/*
 * Copyright 2015-2017 OpenCB
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

import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class Cohort extends Annotable {

    private String id;
    private String uuid;
    private Enums.CohortType type;
    private String creationDate;
    private String modificationDate;
    private String description;
    private List<Sample> samples;

    private int release;
    private CohortInternal internal;
    private Map<String, Object> attributes;

    public Cohort() {
    }

    public Cohort(String id, Enums.CohortType type, String creationDate, String description, List<Sample> samples, int release,
                  Map<String, Object> attributes) {
        this(id, type, creationDate, description, samples, Collections.emptyList(), release, null, attributes);
    }

    public Cohort(String id, Enums.CohortType type, String creationDate, String description, List<Sample> samples,
                  List<AnnotationSet> annotationSetList, int release, Map<String, Object> attributes) {
        this(id, type, creationDate, description, samples, annotationSetList, release, null, attributes);
    }

    public Cohort(String id, Enums.CohortType type, String creationDate, String description, List<Sample> samples,
                  List<AnnotationSet> annotationSets, int release, CohortInternal internal, Map<String, Object> attributes) {
        this.id = id;
        this.type = type;
        this.creationDate = creationDate;
        this.description = description;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.release = release;
        this.internal = internal;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Cohort{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", release=").append(release);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", internal=").append(internal);
        sb.append(", attributes=").append(attributes);
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
                && Objects.equals(attributes, cohort.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, id, type, creationDate, internal, description, samples, release, attributes);
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

    public String getUuid() {
        return uuid;
    }

    public Cohort setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Cohort setId(String id) {
        this.id = id;
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

    public int getRelease() {
        return release;
    }

    public Cohort setRelease(int release) {
        this.release = release;
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
