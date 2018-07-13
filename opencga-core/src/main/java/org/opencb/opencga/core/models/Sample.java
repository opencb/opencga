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

package org.opencb.opencga.core.models;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.acls.AclParams;

import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Sample extends Annotable {

    private String id;
    @Deprecated
    private String name;
    private String uuid;
    private String source;
    @Deprecated
    private Individual individual;

    private int release;
    private int version;
    private String creationDate;
    private Status status;
    private String description;
    private String type;
    private boolean somatic;
    private List<OntologyTerm> phenotypes;

    @Deprecated
    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public Sample() {
    }

    public Sample(String id, String source, Individual individual, String description, int release) {
        this(id, source, individual, description, "", false, release, 1, new LinkedList<>(), new ArrayList<>(), new HashMap<>(),
                new HashMap<>());
    }

    public Sample(String id, String source, Individual individual, String description, String type, boolean somatic, int release,
                  int version, List<AnnotationSet> annotationSets, List<OntologyTerm> phenotypeList, Map<String, Object> stats,
                  Map<String, Object> attributes) {
        this.id = id;
        this.source = source;
        this.version = version;
        this.individual = individual;
        this.type = type;
        this.somatic = somatic;
        this.creationDate = TimeUtils.getTime();
        this.status = new Status();
        this.description = description;
        this.release = release;
        this.phenotypes = phenotypeList;
        this.annotationSets = annotationSets;
        this.stats = stats;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Sample{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", stats=").append(stats);
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
        if (!(o instanceof Sample)) {
            return false;
        }
        Sample sample = (Sample) o;
        return release == sample.release
                && version == sample.version
                && somatic == sample.somatic
                && Objects.equals(uuid, sample.uuid)
                && Objects.equals(id, sample.id)
                && Objects.equals(name, sample.name)
                && Objects.equals(source, sample.source)
                && Objects.equals(individual, sample.individual)
                && Objects.equals(creationDate, sample.creationDate)
                && Objects.equals(status, sample.status)
                && Objects.equals(description, sample.description)
                && Objects.equals(type, sample.type)
                && Objects.equals(phenotypes, sample.phenotypes)
                && Objects.equals(stats, sample.stats)
                && Objects.equals(attributes, sample.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, id, name, source, individual, release, version, creationDate, status,
                description, type, somatic, phenotypes, stats, attributes);
    }

    @Override
    public Sample setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public Sample setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Sample setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Sample setId(String id) {
        this.id = id;
        return this;
    }

    @Deprecated
    public String getName() {
        return name;
    }

    @Deprecated
    public Sample setName(String name) {
        this.name = name;
        return this;
    }

    public String getSource() {
        return source;
    }

    public Sample setSource(String source) {
        this.source = source;
        return this;
    }

    public Individual getIndividual() {
        return individual;
    }

    public Sample setIndividual(Individual individual) {
        this.individual = individual;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Sample setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Sample setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Sample setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isSomatic() {
        return somatic;
    }

    public Sample setSomatic(boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public String getType() {
        return type;
    }

    public Sample setType(String type) {
        this.type = type;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Sample setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Sample setVersion(int version) {
        this.version = version;
        return this;
    }

    public List<OntologyTerm> getPhenotypes() {
        return phenotypes;
    }

    public Sample setPhenotypes(List<OntologyTerm> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public Sample setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Sample setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    // Acl params to communicate the WS and the sample manager
    public static class SampleAclParams extends AclParams {

        private String individual;
        private String file;
        private String cohort;
        private boolean propagate;

        public SampleAclParams() {
        }

        public SampleAclParams(String permissions, Action action, String individual, String file, String cohort) {
            super(permissions, action);
            this.individual = individual;
            this.file = file;
            this.cohort = cohort;
            this.propagate = false;
        }

        public SampleAclParams(String permissions, Action action, String individual, String file, String cohort, boolean propagate) {
            super(permissions, action);
            this.individual = individual;
            this.file = file;
            this.cohort = cohort;
            this.propagate = propagate;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SampleAclParams{");
            sb.append("permissions='").append(permissions).append('\'');
            sb.append(", action=").append(action);
            sb.append(", individual='").append(individual).append('\'');
            sb.append(", file='").append(file).append('\'');
            sb.append(", cohort='").append(cohort).append('\'');
            sb.append(", propagate=").append(propagate);
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SampleAclParams)) {
                return false;
            }
            SampleAclParams that = (SampleAclParams) o;
            return Objects.equals(individual, that.individual)
                    && Objects.equals(file, that.file)
                    && Objects.equals(cohort, that.cohort);
        }

        @Override
        public int hashCode() {
            return Objects.hash(individual, file, cohort);
        }

        public String getIndividual() {
            return individual;
        }

        public SampleAclParams setIndividual(String individual) {
            this.individual = individual;
            return this;
        }

        public String getFile() {
            return file;
        }

        public SampleAclParams setFile(String file) {
            this.file = file;
            return this;
        }

        public String getCohort() {
            return cohort;
        }

        public SampleAclParams setCohort(String cohort) {
            this.cohort = cohort;
            return this;
        }

        public boolean isPropagate() {
            return propagate;
        }

        public SampleAclParams setPropagate(boolean propagate) {
            this.propagate = propagate;
            return this;
        }
    }

}
