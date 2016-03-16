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

import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Study {

    private int id;
    private String name;
    private String alias;
    private Type type;
    private String creatorId;
    private String creationDate;
    private String description;
    private Status status;
    private String lastActivity;
    private long diskUsage;
    private String cipher;

    private List<Group> groups;

    private List<Experiment> experiments;

    private List<File> files;
    private List<Job> jobs;
    private List<Individual> individuals;
    private List<Sample> samples;

    private List<Dataset> datasets;
    private List<Cohort> cohorts;

    private List<VariableSet> variableSets;

    private URI uri;

    private Map<File.Bioformat, DataStore> dataStores;
    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    public Study() {
    }

    public Study(String name, String alias, Type type, String description, Status status, URI uri) {
        this(-1, name, alias, type, null, TimeUtils.getTime(), description, status, null, 0, "",
                null, new ArrayList<Experiment>(), new ArrayList<File>(), new LinkedList<Job>(),
                new LinkedList<Sample>(), new LinkedList<Dataset>(), new LinkedList<Cohort>(), new LinkedList<VariableSet>(),
                uri, new HashMap<File.Bioformat, DataStore>(), new HashMap<String, Object>(), new HashMap<String, Object>());
    }

    public Study(int id, String name, String alias, Type type, String creatorId, String creationDate,
                 String description, Status status, String lastActivity, long diskUsage, String cipher, List<Group> groups,
                 List<Experiment> experiments, List<File> files, List<Job> jobs, List<Sample> samples, List<Dataset> datasets,
                 List<Cohort> cohorts, List<VariableSet> variableSets, URI uri,
                 Map<File.Bioformat, DataStore> dataStores, Map<String, Object> stats, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.creatorId = creatorId;
        this.creationDate = creationDate;
        this.description = description;
        this.status = status;
        this.lastActivity = lastActivity;
        this.diskUsage = diskUsage;
        this.cipher = cipher;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Study{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creatorId='").append(creatorId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", lastActivity='").append(lastActivity).append('\'');
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", cipher='").append(cipher).append('\'');
        sb.append(", groups=").append(groups);
        sb.append(", experiments=").append(experiments);
        sb.append(", files=").append(files);
        sb.append(", jobs=").append(jobs);
        sb.append(", individuals=").append(individuals);
        sb.append(", samples=").append(samples);
        sb.append(", datasets=").append(datasets);
        sb.append(", cohorts=").append(cohorts);
        sb.append(", variableSets=").append(variableSets);
        sb.append(", uri=").append(uri);
        sb.append(", dataStores=").append(dataStores);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(String creatorId) {
        this.creatorId = creatorId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getLastActivity() {
        return lastActivity;
    }

    public void setLastActivity(String lastActivity) {
        this.lastActivity = lastActivity;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public String getCipher() {
        return cipher;
    }

    public void setCipher(String cipher) {
        this.cipher = cipher;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public Study setGroups(List<Group> groups) {
        this.groups = groups;
        return this;
    }

    public List<Experiment> getExperiments() {
        return experiments;
    }

    public void setExperiments(List<Experiment> experiments) {
        this.experiments = experiments;
    }

    public List<File> getFiles() {
        return files;
    }

    public void setFiles(List<File> files) {
        this.files = files;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }

    public List<Individual> getIndividuals() {
        return individuals;
    }

    public void setIndividuals(List<Individual> individuals) {
        this.individuals = individuals;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public void setSamples(List<Sample> samples) {
        this.samples = samples;
    }

    public List<Dataset> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<Dataset> datasets) {
        this.datasets = datasets;
    }

    public List<Cohort> getCohorts() {
        return cohorts;
    }

    public void setCohorts(List<Cohort> cohorts) {
        this.cohorts = cohorts;
    }

    public List<VariableSet> getVariableSets() {
        return variableSets;
    }

    public void setVariableSets(List<VariableSet> variableSets) {
        this.variableSets = variableSets;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public void setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public void setStats(Map<String, Object> stats) {
        this.stats = stats;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
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
}
