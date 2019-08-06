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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opencb.biodata.models.commons.Phenotype;
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
    private SampleProcessing processing;
    private SampleCollection collection;
    private String individualId;

    private int release;
    private int version;
    private String creationDate;
    private String modificationDate;
    private Status status;
    private String description;
    private String type;
    private boolean somatic;
    private List<Phenotype> phenotypes;

    @Deprecated
    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public Sample() {
    }

    public Sample(String id, String source, String individualId, String description, int release) {
        this(id, source, individualId, null, null, release, 1, description, "", false, new LinkedList<>(), new ArrayList<>(),
                new HashMap<>());
    }

    public Sample(String id, String source, String individualId, SampleProcessing processing, SampleCollection collection, int release,
                  int version, String description, String type, boolean somatic, List<Phenotype> phenotypes,
                  List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.source = source;
        this.processing = processing;
        this.collection = collection;
        this.release = release;
        this.version = version;
        this.creationDate = TimeUtils.getTime();
        this.status = new Status();
        this.description = description;
        this.type = type;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
        this.stats = new HashMap<>();
        this.individualId = individualId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Sample{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", processing='").append(processing).append('\'');
        sb.append(", collection='").append(collection).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
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
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Sample sample = (Sample) o;

        return new EqualsBuilder()
                .append(release, sample.release)
                .append(version, sample.version)
                .append(somatic, sample.somatic)
                .append(id, sample.id)
                .append(name, sample.name)
                .append(uuid, sample.uuid)
                .append(source, sample.source)
                .append(processing, sample.processing)
                .append(collection, sample.collection)
                .append(individualId, sample.individualId)
                .append(creationDate, sample.creationDate)
                .append(modificationDate, sample.modificationDate)
                .append(status, sample.status)
                .append(description, sample.description)
                .append(type, sample.type)
                .append(phenotypes, sample.phenotypes)
                .append(stats, sample.stats)
                .append(attributes, sample.attributes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(name)
                .append(uuid)
                .append(source)
                .append(processing)
                .append(collection)
                .append(individualId)
                .append(release)
                .append(version)
                .append(creationDate)
                .append(modificationDate)
                .append(status)
                .append(description)
                .append(type)
                .append(somatic)
                .append(phenotypes)
                .append(stats)
                .append(attributes)
                .toHashCode();
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

    public SampleProcessing getProcessing() {
        return processing;
    }

    public Sample setProcessing(SampleProcessing processing) {
        this.processing = processing;
        return this;
    }

    public SampleCollection getCollection() {
        return collection;
    }

    public Sample setCollection(SampleCollection collection) {
        this.collection = collection;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public Sample setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Sample setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Sample setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
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

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public Sample setPhenotypes(List<Phenotype> phenotypes) {
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
