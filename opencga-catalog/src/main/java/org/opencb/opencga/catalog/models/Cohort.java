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

package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.permissions.CohortAclEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 *         <p>
 *         Set of samples grouped according to criteria
 */
public class Cohort extends Annotable<CohortAclEntry> {

    private long id;
    private String name;
    private Study.Type type;
    private String creationDate;
    private CohortStatus status;
    private String description;

    private List<Sample> samples;
    private Family family;

    private Map<String, Object> stats;
    private int release;
    private Map<String, Object> attributes;


    public Cohort() {
    }

    public Cohort(String name, Study.Type type, String creationDate, String description, List<Sample> samples, int release,
                  Map<String, Object> attributes) {
        this(-1, name, type, creationDate, new CohortStatus(), description, samples, null, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), release, attributes);
    }

    public Cohort(String name, Study.Type type, String creationDate, String description, List<Sample> samples,
                  List<AnnotationSet> annotationSetList, int release, Map<String, Object> attributes) {
        this(-1, name, type, creationDate, new CohortStatus(), description, samples, null, Collections.emptyList(),
                annotationSetList, Collections.emptyMap(), release, attributes);
    }

    public Cohort(long id, String name, Study.Type type, String creationDate, CohortStatus status, String description,
                  List<Sample> samples, Family family, List<CohortAclEntry> acl, List<AnnotationSet> annotationSets,
                  Map<String, Object> stats, int release, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.creationDate = creationDate;
        this.status = status;
        this.description = description;
        this.samples = samples;
        this.family = family;
        this.acl = acl;
        this.annotationSets = annotationSets;
        this.release = release;
        this.stats = stats;
        this.attributes = attributes;
    }

    public static class CohortStatus extends Status {

        public static final String NONE = "NONE";
        public static final String CALCULATING = "CALCULATING";
        public static final String INVALID = "INVALID";

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
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", family=").append(family);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", stats=").append(stats);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Cohort setId(long id) {
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

    public Study.Type getType() {
        return type;
    }

    public Cohort setType(Study.Type type) {
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

    public Family getFamily() {
        return family;
    }

    public Cohort setFamily(Family family) {
        this.family = family;
        return this;
    }

    public Cohort setAcl(List<CohortAclEntry> acl) {
        this.acl = acl;
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
        return id == cohort.id
                && Objects.equals(name, cohort.name)
                && type == cohort.type
                && Objects.equals(creationDate, cohort.creationDate)
                && Objects.equals(status, cohort.status)
                && Objects.equals(description, cohort.description)
                && Objects.equals(samples, cohort.samples)
                && Objects.equals(family, cohort.family)
                && Objects.equals(stats, cohort.stats)
                && Objects.equals(attributes, cohort.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, creationDate, status, description, samples, family, stats, attributes);
    }

}
