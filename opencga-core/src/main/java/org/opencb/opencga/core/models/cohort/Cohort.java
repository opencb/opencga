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
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 *         <p>
 *         Set of samples grouped according to criteria
 */
public class Cohort extends Annotable {

    private String id;
    @Deprecated
    private String name;
    private String uuid;
    private Enums.CohortType type;
    private String creationDate;
    private String modificationDate;
    private CohortStatus status;
    private String description;

    private List<Sample> samples;

    @Deprecated
    private Map<String, Object> stats;
    private int release;
    private Map<String, Object> attributes;


    public Cohort() {
    }

    public Cohort(String id, Enums.CohortType type, String creationDate, String description, List<Sample> samples, int release,
                  Map<String, Object> attributes) {
        this(id, type, creationDate, new CohortStatus(), description, samples, Collections.emptyList(), Collections.emptyMap(), release,
                attributes);
    }

    public Cohort(String id, Enums.CohortType type, String creationDate, String description, List<Sample> samples,
                  List<AnnotationSet> annotationSetList, int release, Map<String, Object> attributes) {
        this(id, type, creationDate, new CohortStatus(), description, samples, annotationSetList, Collections.emptyMap(), release,
                attributes);
    }

    public Cohort(String id, Enums.CohortType type, String creationDate, CohortStatus status, String description, List<Sample> samples,
                  List<AnnotationSet> annotationSets, Map<String, Object> stats, int release, Map<String, Object> attributes) {
        this.id = id;
        this.type = type;
        this.creationDate = creationDate;
        this.status = status;
        this.description = description;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.release = release;
        this.stats = stats;
        this.attributes = attributes;
    }

    public static class CohortStatus extends Status {

        public static final String NONE = "NONE";
        public static final String CALCULATING = "CALCULATING";
        public static final String INVALID = "INVALID";

        public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NONE, CALCULATING, INVALID);

        public CohortStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public CohortStatus(String status) {
            this(status, "");
        }

        public CohortStatus() {
            this(NONE, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(NONE) || status.equals(CALCULATING) || status.equals(INVALID))) {
                return true;
            }
            return false;
        }
    }

    public class Family {

        private String id;
        private List<Long> probands;

        public Family(String id, List<Long> probands) {
            this.id = id;
            this.probands = probands;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Family{");
            sb.append("id='").append(id).append('\'');
            sb.append(", probands=").append(probands);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Family setId(String id) {
            this.id = id;
            return this;
        }

        public List<Long> getProbands() {
            return probands;
        }

        public Family setProbands(List<Long> probands) {
            this.probands = probands;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Family)) {
                return false;
            }
            Family family = (Family) o;
            return Objects.equals(id, family.id)
                    && Objects.equals(probands, family.probands);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, probands);
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Cohort{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", stats=").append(stats);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
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

    @Deprecated
    public String getName() {
        return name;
    }

    @Deprecated
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

    public CohortStatus getStatus() {
        return status;
    }

    public Cohort setStatus(CohortStatus status) {
        this.status = status;
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

    public Map<String, Object> getStats() {
        return stats;
    }

    public Cohort setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Cohort setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
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
                && Objects.equals(status, cohort.status)
                && Objects.equals(description, cohort.description)
                && Objects.equals(samples, cohort.samples)
                && Objects.equals(stats, cohort.stats)
                && Objects.equals(attributes, cohort.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, id, type, creationDate, status, description, samples, stats, release, attributes);
    }

}
