/*
 * Copyright 2015 OpenCB
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

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.CohortAcl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 *         <p>
 *         Set of samples grouped according to criteria
 */
public class Cohort {

    private long id;
    private String name;
    private Type type;
    private String creationDate;
    private CohortStatus status;
    private String description;

    private List<Long> samples;
    private List<CohortAcl> acls;
    private List<AnnotationSet> annotationSets;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public Cohort() {
    }

    public Cohort(String name, Type type, String creationDate, String description, List<Long> samples,
                  Map<String, Object> attributes) throws CatalogException {
        this(-1, name, type, creationDate, new CohortStatus(), description, samples, Collections.emptyMap(), attributes);
    }

    public Cohort(int id, String name, Type type, String creationDate, CohortStatus cohortStatus, String description, List<Long> samples,
                  Map<String, Object> stats, Map<String, Object> attributes) throws CatalogException {
        this.id = id;
        this.name = name;
        this.type = type;
        this.creationDate = creationDate;
        this.status = cohortStatus;
        this.description = description;
        this.samples = samples;
        this.acls = Collections.emptyList();
        this.annotationSets = Collections.emptyList();
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

    //Represents the criteria of grouping samples in the cohort
    @Deprecated
    public enum Type {
        CASE_CONTROL,
        CASE_SET,
        CONTROL_SET,
        PAIRED,
        PAIRED_TUMOR,
        FAMILY,
        TRIO,
        COLLECTION
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
        sb.append(", acls=").append(acls);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", stats=").append(stats);
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

    public Type getType() {
        return type;
    }

    public Cohort setType(Type type) {
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

    public List<Long> getSamples() {
        return samples;
    }

    public Cohort setSamples(List<Long> samples) {
        this.samples = samples;
        return this;
    }

    public List<CohortAcl> getAcls() {
        return acls;
    }

    public Cohort setAcls(List<CohortAcl> acls) {
        this.acls = acls;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Cohort setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
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

}
