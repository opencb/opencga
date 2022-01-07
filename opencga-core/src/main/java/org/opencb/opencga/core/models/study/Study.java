
/*
 * Copyright 2015-2020 OpenCB
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
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.PrivateFields;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AdditionalInfo;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.ExternalSource;
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

    /**
     * Study ID is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the study creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "name", indexed = true,
            description = FieldConstants.PROJECT_FQN)
    private String name;

    @DataField(id = "name", indexed = true,
            description = FieldConstants.STUDY_ALIAS)
    private String alias;

    /**
     * String representing when the sample was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "creationDate", indexed = true,
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    /**
     * String representing when was the last time the sample was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    /**
     * An string to describe the properties of the sample.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "size", indexed = true,
            description = FieldConstants.STUDY_SIZE)
    private long size;

    @DataField(id = "fqn", indexed = true,
            description = FieldConstants.PROJECT_FQN)
    private String fqn;


    @DataField(id = "notification",
            description = FieldConstants.STUDY_NOTIFICATION)
    private StudyNotification notification;

    /**
     * A List with related groups.
     *
     * @apiNote
     */
    @DataField(id = "groups",
            description = FieldConstants.STUDY_GROUPS)
    private List<Group> groups;

    /**
     * A List with related files.
     *
     * @apiNote
     */

    @DataField(id = "files",
            description = FieldConstants.STUDY_FILES)
    private List<File> files;

    /**
     * A List with related jobs.
     *
     * @apiNote
     */
    @DataField(id = "jobs",
            description = FieldConstants.STUDY_JOBS)
    private List<Job> jobs;

    /**
     * A List with related individuals.
     *
     * @apiNote
     */
    @DataField(id = "individuals",
            description = FieldConstants.STUDY_INDIVIDUALS)
    private List<Individual> individuals;

    /**
     * A List with related families.
     *
     * @apiNote
     */
    @DataField(id = "families",
            description = FieldConstants.STUDY_FAMILIES)
    private List<Family> families;

    /**
     * A List with related samples.
     *
     * @apiNote
     */
    @DataField(id = "samples",
            description = FieldConstants.STUDY_SAMPLES)
    private List<Sample> samples;

    /**
     * A List with related cohorts.
     *
     * @apiNote
     */
    @DataField(id = "cohorts",
            description = FieldConstants.STUDY_COHORTS)
    private List<Cohort> cohorts;

    /**
     * A List with related panels.
     *
     * @apiNote
     */
    @DataField(id = "panels",
            description = FieldConstants.STUDY_PANELS)
    private List<Panel> panels;

    /**
     * A List with related clinicalAnalyses.
     *
     * @apiNote
     */
    @DataField(id = "clinicalAnalyses",
            description = FieldConstants.STUDY_ANALYSES)
    private List<ClinicalAnalysis> clinicalAnalyses;

    /**
     * A List with related variableSets.
     *
     * @apiNote
     */
    @DataField(id = "variableSets",
            description = FieldConstants.STUDY_VARIABLE_SETS)
    private List<VariableSet> variableSets;


    @DataField(id = "permissionRules",
            description = FieldConstants.STUDY_PERMISSION_RULES)
    private Map<Enums.Entity, List<PermissionRule>> permissionRules;

    @DataField(id = "uri",
            description = FieldConstants.STUDY_URI)
    private URI uri;

    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    @DataField(id = "sources", indexed = true,
            description = FieldConstants.STUDY_EXTERNAL_SOURCES)
    private List<ExternalSource> sources;

    @DataField(id = "type", indexed = true,
            description = FieldConstants.STUDY_TYPE)
    private StudyType type;

    /**
     * An object describing the status of the Sample.
     *
     * @apiNote
     */

    @DataField(id = "status", indexed = true,
            description = FieldConstants.GENERIC_CUSTOM_STATUS)
    private CustomStatus status;

    /**
     * An object describing the internal information of the Sample. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */

    @DataField(id = "internal", indexed = true,
            description = FieldConstants.GENERIC_INTERNAL)
    private StudyInternal internal;

    @DataField(id = "additionalInfo", indexed = true,
            description = FieldConstants.GENERIC_ADDITIONAL_INFO_DESCRIPTION)
    private List<AdditionalInfo> additionalInfo;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Study() {
    }

    public Study(String name, String alias, String description, StudyType type, StudyInternal internal, URI uri, int release) {
        this(alias, name, alias, TimeUtils.getTime(), TimeUtils.getTime(), description, type, new LinkedList<>(), null, 0,
                new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(),
                new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new HashMap<>(), uri, release,
                new CustomStatus(), internal, new LinkedList<>(), new HashMap<>());
    }

    public Study(String id, String name, String alias, String creationDate, String modificationDate, String description, StudyType type,
                 List<ExternalSource> sources, StudyNotification notification, long size, List<Group> groups, List<File> files,
                 List<Job> jobs, List<Individual> individuals, List<Family> families, List<Sample> samples, List<Cohort> cohorts,
                 List<org.opencb.opencga.core.models.panel.Panel> panels, List<ClinicalAnalysis> clinicalAnalyses,
                 List<VariableSet> variableSets, Map<Enums.Entity, List<PermissionRule>> permissionRules, URI uri, int release,
                 CustomStatus status, StudyInternal internal, List<AdditionalInfo> additionalInfo, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.notification = notification;
        this.size = size;
        this.groups = ObjectUtils.defaultIfNull(groups, new ArrayList<>());
        this.files = ObjectUtils.defaultIfNull(files, new ArrayList<>());
        this.families = ObjectUtils.defaultIfNull(families, new ArrayList<>());
        this.jobs = ObjectUtils.defaultIfNull(jobs, new ArrayList<>());
        this.individuals = ObjectUtils.defaultIfNull(individuals, new ArrayList<>());
        this.samples = ObjectUtils.defaultIfNull(samples, new ArrayList<>());
        this.cohorts = ObjectUtils.defaultIfNull(cohorts, new ArrayList<>());
        this.panels = ObjectUtils.defaultIfNull(panels, new ArrayList<>());
        this.clinicalAnalyses = ObjectUtils.defaultIfNull(clinicalAnalyses, new ArrayList<>());
        this.sources = sources;
        this.internal = internal;
        this.variableSets = ObjectUtils.defaultIfNull(variableSets, new ArrayList<>());
        this.permissionRules = ObjectUtils.defaultIfNull(permissionRules, new HashMap<>());
        this.uri = uri;
        this.status = status;
        this.release = release;
        this.additionalInfo = additionalInfo;
        this.attributes = ObjectUtils.defaultIfNull(attributes, new HashMap<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Study{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
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
        sb.append(", clinicalAnalyses=").append(clinicalAnalyses);
        sb.append(", variableSets=").append(variableSets);
        sb.append(", permissionRules=").append(permissionRules);
        sb.append(", uri=").append(uri);
        sb.append(", release=").append(release);
        sb.append(", sources=").append(sources);
        sb.append(", type=").append(type);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
        sb.append(", additionalInfo=").append(additionalInfo);
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

    public long getSize() {
        return size;
    }

    public Study setSize(long size) {
        this.size = size;
        return this;
    }

    public List<ExternalSource> getSources() {
        return sources;
    }

    public Study setSources(List<ExternalSource> sources) {
        this.sources = sources;
        return this;
    }

    public StudyType getType() {
        return type;
    }

    public Study setType(StudyType type) {
        this.type = type;
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

    public List<org.opencb.opencga.core.models.panel.Panel> getPanels() {
        return panels;
    }

    public Study setPanels(List<org.opencb.opencga.core.models.panel.Panel> panels) {
        this.panels = panels;
        return this;
    }

    public List<ClinicalAnalysis> getClinicalAnalyses() {
        return clinicalAnalyses;
    }

    public Study setClinicalAnalyses(List<ClinicalAnalysis> clinicalAnalyses) {
        this.clinicalAnalyses = clinicalAnalyses;
        return this;
    }

    public List<VariableSet> getVariableSets() {
        return variableSets;
    }

    public Study setVariableSets(List<VariableSet> variableSets) {
        this.variableSets = variableSets;
        return this;
    }

    public StudyInternal getInternal() {
        return internal;
    }

    public Study setInternal(StudyInternal internal) {
        this.internal = internal;
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

    public CustomStatus getStatus() {
        return status;
    }

    public Study setStatus(CustomStatus status) {
        this.status = status;
        return this;
    }

    public List<AdditionalInfo> getAdditionalInfo() {
        return additionalInfo;
    }

    public Study setAdditionalInfo(List<AdditionalInfo> additionalInfo) {
        this.additionalInfo = additionalInfo;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Study setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
