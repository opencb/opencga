
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

import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Study extends AbstractAcl<StudyAclEntry> {

    private long id;
    private String name;
    private String alias;
    private Type type;
    private String creationDate;
    private String description;
    private Status status;
    private String lastModified;
    private long size;
    // TODO: Pending !!!
    private String cipher;

    private List<Group> groups;
//    private List<StudyAclEntry> acl;

    private List<Experiment> experiments;

    private List<File> files;
    private List<Job> jobs;
    private List<Individual> individuals;
    private List<Sample> samples;

    private List<Dataset> datasets;
    private List<Cohort> cohorts;

    private List<DiseasePanel> panels;

    private List<VariableSet> variableSets;

    private URI uri;

    private int release;
    private Map<File.Bioformat, DataStore> dataStores;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public Study() {
    }

    public Study(String name, String alias, Type type, String description, Status status, URI uri, int release) {
        this(-1, name, alias, type, TimeUtils.getTime(), description, status, null, 0, "",
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(),
                new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), Collections.emptyList(), new LinkedList<>(), uri,
                new HashMap<>(), release, new HashMap<>(), new HashMap<>()
        );
    }

    public Study(long id, String name, String alias, Type type, String creationDate, String description, Status status, String lastModified,
                 long size, String cipher, List<Group> groups, List<StudyAclEntry> acl, List<Experiment> experiments, List<File> files,
                 List<Job> jobs, List<Individual> individuals, List<Sample> samples, List<Dataset> datasets, List<Cohort> cohorts,
                 List<DiseasePanel> panels, List<VariableSet> variableSets, URI uri, Map<File.Bioformat, DataStore> dataStores, int release,
                 Map<String, Object> stats, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.creationDate = creationDate;
        this.description = description;
        this.status = status;
        this.lastModified = lastModified;
        this.size = size;
        this.cipher = cipher;
        this.groups = groups;
        this.acl = acl;
        this.experiments = experiments;
        this.files = files;
        this.jobs = jobs;
        this.individuals = individuals;
        this.samples = samples;
        this.datasets = datasets;
        this.cohorts = cohorts;
        this.panels = panels;
        this.variableSets = variableSets;
        this.uri = uri;
        this.stats = stats;
        this.release = release;
        this.dataStores = dataStores;
        this.attributes = attributes;
    }

    public enum Type {
        CASE_CONTROL,
        CASE_SET,
        CONTROL_SET,
        PAIRED,
        PAIRED_TUMOR,
        AGGREGATE,
        TIME_SERIES,
        FAMILY,
        TRIO,
        COLLECTION
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Study{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", status=").append(status);
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", size=").append(size);
        sb.append(", cipher='").append(cipher).append('\'');
        sb.append(", groups=").append(groups);
        sb.append(", acl=").append(acl);
        sb.append(", experiments=").append(experiments);
        sb.append(", files=").append(files);
        sb.append(", jobs=").append(jobs);
        sb.append(", individuals=").append(individuals);
        sb.append(", samples=").append(samples);
        sb.append(", datasets=").append(datasets);
        sb.append(", cohorts=").append(cohorts);
        sb.append(", panels=").append(panels);
        sb.append(", variableSets=").append(variableSets);
        sb.append(", uri=").append(uri);
        sb.append(", release=").append(release);
        sb.append(", dataStores=").append(dataStores);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Study setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Study setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public Study setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Study setType(Type type) {
        this.type = type;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Study setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Study setDescription(String description) {
        this.description = description;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Study setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public Study setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public long getSize() {
        return size;
    }

    public Study setSize(long size) {
        this.size = size;
        return this;
    }

    public String getCipher() {
        return cipher;
    }

    public Study setCipher(String cipher) {
        this.cipher = cipher;
        return this;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public Study setGroups(List<Group> groups) {
        this.groups = groups;
        return this;
    }

    public Study setAcl(List<StudyAclEntry> acl) {
        this.acl = acl;
        return this;
    }

    public List<Experiment> getExperiments() {
        return experiments;
    }

    public Study setExperiments(List<Experiment> experiments) {
        this.experiments = experiments;
        return this;
    }

    public List<File> getFiles() {
        return files;
    }

    public Study setFiles(List<File> files) {
        this.files = files;
        return this;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public Study setJobs(List<Job> jobs) {
        this.jobs = jobs;
        return this;
    }

    public List<Individual> getIndividuals() {
        return individuals;
    }

    public Study setIndividuals(List<Individual> individuals) {
        this.individuals = individuals;
        return this;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public Study setSamples(List<Sample> samples) {
        this.samples = samples;
        return this;
    }

    public List<Dataset> getDatasets() {
        return datasets;
    }

    public Study setDatasets(List<Dataset> datasets) {
        this.datasets = datasets;
        return this;
    }

    public List<Cohort> getCohorts() {
        return cohorts;
    }

    public Study setCohorts(List<Cohort> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public List<DiseasePanel> getPanels() {
        return panels;
    }

    public Study setPanels(List<DiseasePanel> panels) {
        this.panels = panels;
        return this;
    }

    public List<VariableSet> getVariableSets() {
        return variableSets;
    }

    public Study setVariableSets(List<VariableSet> variableSets) {
        this.variableSets = variableSets;
        return this;
    }

    public URI getUri() {
        return uri;
    }

    public Study setUri(URI uri) {
        this.uri = uri;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Study setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public Study setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public Study setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Study setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    // Acl params to communicate the WS and the sample manager
    public static class StudyAclParams extends AclParams {

        private String template;

        public StudyAclParams() {
        }

        public StudyAclParams(String permissions, Action action, String template) {
            super(permissions, action);
            this.template = template;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StudyAclParams{");
            sb.append("permissions='").append(permissions).append('\'');
            sb.append(", action=").append(action);
            sb.append(", template='").append(template).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getTemplate() {
            return template;
        }

        public StudyAclParams setTemplate(String template) {
            this.template = template;
            return this;
        }
    }

}
