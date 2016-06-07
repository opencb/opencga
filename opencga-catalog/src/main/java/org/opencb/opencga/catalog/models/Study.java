
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

import org.opencb.opencga.catalog.models.acls.StudyAcl;
import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Study {

    private long id;
    private String name;
    private String alias;
    private Type type;
//    private String ownerId;
    private String creationDate;
    private String description;
    private Status status;
    private String lastActivity;
    private long diskUsage;
    private String cipher;

    private List<Group> groups;
    @Deprecated
    private List<Role> roles;
    private List<StudyAcl> acls;

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

    private Map<File.Bioformat, DataStore> dataStores;
    private Map<String, Object> attributes;

    private Map<String, Object> stats;

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
        sb.append(", lastActivity='").append(lastActivity).append('\'');
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", cipher='").append(cipher).append('\'');
        sb.append(", groups=").append(groups);
        sb.append(", acls=").append(acls);
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
        sb.append(", dataStores=").append(dataStores);
        sb.append(", attributes=").append(attributes);
        sb.append(", stats=").append(stats);
        sb.append('}');
        return sb.toString();
    }

    public Study() {
    }

    public Study(String name, String alias, Type type, String description, Status status, URI uri) {
        this(-1, name, alias, type, TimeUtils.getTime(), description, status, null, 0, "",
                null, new ArrayList<>(), new ArrayList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(),
                new LinkedList<>(), Collections.emptyList(), new LinkedList<>(), uri, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    public Study(long id, String name, String alias, Type type, String creationDate, String description, Status status, String lastActivity,
                 long diskUsage, String cipher, List<Group> groups, List<StudyAcl> acls, List<Experiment> experiments, List<File> files,
                 List<Job> jobs, List<Sample> samples, List<Dataset> datasets, List<Cohort> cohorts, List<DiseasePanel> panels,
                 List<VariableSet> variableSets, URI uri, Map<File.Bioformat, DataStore> dataStores, Map<String, Object> stats,
                 Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
//        this.ownerId = ownerId;
        this.creationDate = creationDate;
        this.description = description;
        this.status = status;
        this.lastActivity = lastActivity;
        this.diskUsage = diskUsage;
        this.cipher = cipher;
        this.panels = panels;
//        this.roles = roles;
        this.acls = acls;
        this.groups = groups;
        this.experiments = experiments;
        this.files = files;
        this.jobs = jobs;
        this.samples = samples;
        this.datasets = datasets;
        this.cohorts = cohorts;
        this.variableSets = variableSets;
        this.uri = uri;
        this.dataStores = dataStores;
        this.stats = stats;
        this.attributes = attributes;
    }

    public List<StudyAcl> getAcls() {
        return acls;
    }

    public Study setAcls(List<StudyAcl> acls) {
        this.acls = acls;
        return this;
    }

    public List<DiseasePanel> getPanels() {
        return panels;
    }

    public Study setPanels(List<DiseasePanel> panels) {
        this.panels = panels;
        return this;
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

    public String getLastActivity() {
        return lastActivity;
    }

    public Study setLastActivity(String lastActivity) {
        this.lastActivity = lastActivity;
        return this;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public Study setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
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

    public List<Role> getRoles() {
        return roles;
    }

    public Study setRoles(List<Role> roles) {
        this.roles = roles;
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

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public Study setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Study setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public Study setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

}
