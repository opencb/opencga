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

package org.opencb.opencga.core.models.project;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.PrivateFields;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.study.Study;

import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Project extends PrivateFields {

    private String id;
    private String name;
    private String uuid;
    /**
     * Full Qualified Name (user@projectId).
      */
    private String fqn;
    private String creationDate;
    private String modificationDate;
    private String description;
    private String organization;
    private Organism organism;
    private int currentRelease;
    private Status status;
    @Deprecated
    private String lastModified;
    private long size;

    private List<Study> studies;

    private ProjectInternal internal;
    private Map<String, Object> attributes;


    public Project() {
    }

    public Project(String id, String name, String description, Status status, String organization, Organism organism, int currentRelease) {
        this(id, name, TimeUtils.getTime(), description, organization, organism, status, null, 0, new LinkedList<>(),
                new HashMap<>(), currentRelease);
    }

    public Project(String id, String name, String creationDate, String description, Status status, String lastModified, long size,
                   String organization, Organism organism, int currentRelease) {
        this(id, name, creationDate, description, organization, organism, status, lastModified, size, new LinkedList<>(),
                new HashMap<>(), currentRelease);
    }

    public Project(String id, String name, String creationDate, String description, String organization, Organism organism, Status status,
                   String lastModified, long size, List<Study> studies,
                   Map<String, Object> attributes, int currentRelease) {
        this.id = id;
        this.name = name;
        this.creationDate = creationDate;
        this.description = description;
        this.organization = organization;
        this.organism = organism;
        this.status = status;
        this.lastModified = lastModified;
        this.size = size;
        this.studies = studies;
        this.attributes = attributes;
        this.currentRelease = currentRelease;
    }

    public static class Organism {

        private String scientificName;
        private String commonName;
        private int taxonomyCode;
        private String assembly;

        public Organism() {
        }

        public Organism(String scientificName, String assembly) {
            this(scientificName, "", -1, assembly);
        }

        public Organism(String scientificName, String commonName, int taxonomyCode, String assembly) {
            this.scientificName = scientificName != null ? scientificName : "";
            this.commonName = commonName != null ? commonName : "";
            this.taxonomyCode = taxonomyCode;
            this.assembly = assembly != null ? assembly : "";
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Organism{");
            sb.append("scientificName='").append(scientificName).append('\'');
            sb.append(", commonName='").append(commonName).append('\'');
            sb.append(", taxonomyCode=").append(taxonomyCode);
            sb.append(", assembly='").append(assembly).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Organism organism = (Organism) o;
            return Objects.equals(taxonomyCode, organism.taxonomyCode) && Objects.equals(scientificName, organism.scientificName)
                    && Objects.equals(commonName, organism.commonName) && Objects.equals(assembly, organism.assembly);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taxonomyCode, scientificName, commonName, assembly);
        }

        public int getTaxonomyCode() {
            return taxonomyCode;
        }

        public Organism setTaxonomyCode(int taxonomyCode) {
            this.taxonomyCode = taxonomyCode;
            return this;
        }

        public String getScientificName() {
            return scientificName;
        }

        public Organism setScientificName(String scientificName) {
            this.scientificName = scientificName;
            return this;
        }

        public String getCommonName() {
            return commonName;
        }

        public Organism setCommonName(String commonName) {
            this.commonName = commonName;
            return this;
        }

        public String getAssembly() {
            return assembly;
        }

        public Organism setAssembly(String assembly) {
            this.assembly = assembly;
            return this;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Project{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", fqn='").append(fqn).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", organism=").append(organism);
        sb.append(", currentRelease=").append(currentRelease);
        sb.append(", status=").append(status);
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", size=").append(size);
        sb.append(", studies=").append(studies);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public Project setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Project setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public Project setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    public String getName() {
        return name;
    }

    public Project setName(String name) {
        this.name = name;
        return this;
    }

    public String getFqn() {
        return fqn;
    }

    public Project setFqn(String fqn) {
        this.fqn = fqn;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Project setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Project setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public int getCurrentRelease() {
        return currentRelease;
    }

    public Project setCurrentRelease(int currentRelease) {
        this.currentRelease = currentRelease;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Project setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public Project setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public Organism getOrganism() {
        return organism;
    }

    public Project setOrganism(Organism organism) {
        this.organism = organism;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Project setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public Project setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public long getSize() {
        return size;
    }

    public Project setSize(long size) {
        this.size = size;
        return this;
    }

    public List<Study> getStudies() {
        return studies;
    }

    public Project setStudies(List<Study> studies) {
        this.studies = studies;
        return this;
    }

    public ProjectInternal getInternal() {
        return internal;
    }

    public Project setInternal(ProjectInternal internal) {
        this.internal = internal;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Project setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
