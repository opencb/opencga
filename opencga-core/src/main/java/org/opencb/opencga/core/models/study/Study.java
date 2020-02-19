
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

package org.opencb.opencga.core.models.study;

import org.apache.commons.lang3.ObjectUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.PrivateFields;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Study extends PrivateFields {

    private String id;
    private String name;
    private String uuid;
    private String alias;
    private Type type;
    private String creationDate;
    private String modificationDate;
    private String description;
    private Status status;
    private long size;
    private String fqn;

    private StudyNotification notification;

    private List<Group> groups;

    private List<File> files;
    private List<Job> jobs;
    private List<Individual> individuals;
    private List<Family> families;
    private List<Sample> samples;
    private List<Cohort> cohorts;
    private List<Panel> panels;

    private List<VariableSet> variableSets;

    private Map<Enums.Entity, List<PermissionRule>> permissionRules;

    private URI uri;

    private int release;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public Study() {
    }

    public Study(String name, String alias, Type type, String description, Status status, URI uri, int release) {
        this(alias, name, alias, type, TimeUtils.getTime(), description, null, status,
                0, new ArrayList<>(), new LinkedList<>(), new LinkedList<>(),
                new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), Collections.emptyList(), new LinkedList<>(),
                new HashMap<>(), uri, release, new HashMap<>(), new HashMap<>());
    }

    public Study(String id, String name, String alias, Type type, String creationDate, String description, StudyNotification notification,
                 Status status, long size, List<Group> groups, List<File> files, List<Job> jobs, List<Individual> individuals,
                 List<Family> families, List<Sample> samples, List<Cohort> cohorts, List<Panel> panels, List<VariableSet> variableSets,
                 Map<Enums.Entity, List<PermissionRule>> permissionRules, URI uri, int release, Map<String, Object> stats,
                 Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.creationDate = creationDate;
        this.description = description;
        this.notification = notification;
        this.status = status;
        this.size = size;
        this.groups = ObjectUtils.defaultIfNull(groups, new ArrayList<>());
        this.files = ObjectUtils.defaultIfNull(files, new ArrayList<>());
        this.families = ObjectUtils.defaultIfNull(families, new ArrayList<>());
        this.jobs = ObjectUtils.defaultIfNull(jobs, new ArrayList<>());
        this.individuals = ObjectUtils.defaultIfNull(individuals, new ArrayList<>());
        this.samples = ObjectUtils.defaultIfNull(samples, new ArrayList<>());
        this.cohorts = ObjectUtils.defaultIfNull(cohorts, new ArrayList<>());
        this.panels = ObjectUtils.defaultIfNull(panels, new ArrayList<>());
        this.variableSets = ObjectUtils.defaultIfNull(variableSets, new ArrayList<>());
        this.permissionRules = ObjectUtils.defaultIfNull(permissionRules, new HashMap<>());
        this.uri = uri;
        this.stats = ObjectUtils.defaultIfNull(stats, new HashMap<>());
        this.release = release;
        this.attributes = ObjectUtils.defaultIfNull(attributes, new HashMap<>());
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
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", status=").append(status);
        sb.append(", size=").append(size);
        sb.append(", fqn='").append(fqn).append('\'');
        sb.append(", notification=").append(notification);
        sb.append(", groups=").append(groups);
        sb.append(", files=").append(files);
        sb.append(", jobs=").append(jobs);
        sb.append(", individuals=").append(individuals);
        sb.append(", families=").append(families);
        sb.append(", samples=").append(samples);
        sb.append(", cohorts=").append(cohorts);
        sb.append(", panels=").append(panels);
        sb.append(", variableSets=").append(variableSets);
        sb.append(", permissionRules=").append(permissionRules);
        sb.append(", uri=").append(uri);
        sb.append(", release=").append(release);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public Study setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Study setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public Study setUid(long uid) {
        super.setUid(uid);
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

    public String getModificationDate() {
        return modificationDate;
    }

    public Study setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Study setDescription(String description) {
        this.description = description;
        return this;
    }

    public StudyNotification getNotification() {
        return notification;
    }

    public Study setNotification(StudyNotification notification) {
        this.notification = notification;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Study setStatus(Status status) {
        this.status = status;
        return this;
    }

    public long getSize() {
        return size;
    }

    public Study setSize(long size) {
        this.size = size;
        return this;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public Study setGroups(List<Group> groups) {
        this.groups = groups;
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

    public List<Family> getFamilies() {
        return families;
    }

    public Study setFamilies(List<Family> families) {
        this.families = families;
        return this;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public Study setSamples(List<Sample> samples) {
        this.samples = samples;
        return this;
    }

    public List<Cohort> getCohorts() {
        return cohorts;
    }

    public Study setCohorts(List<Cohort> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public List<Panel> getPanels() {
        return panels;
    }

    public Study setPanels(List<Panel> panels) {
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

    public Map<Enums.Entity, List<PermissionRule>> getPermissionRules() {
        return permissionRules;
    }

    public Study setPermissionRules(Map<Enums.Entity, List<PermissionRule>> permissionRules) {
        this.permissionRules = permissionRules;
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

    public String getFqn() {
        return fqn;
    }

    public Study setFqn(String fqn) {
        this.fqn = fqn;
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

        public StudyAclParams setPermissions(String permissions) {
            super.setPermissions(permissions);
            return this;
        }

        public StudyAclParams setAction(Action action) {
            super.setAction(action);
            return this;
        }
    }

}
