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

package org.opencb.opencga.core.models.summaries;

import org.opencb.opencga.core.models.file.FileExperiment;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.StudyInternal;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 29/04/16.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class StudySummary {

    @DataField(description = ParamConstants.STUDY_SUMMARY_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.STUDY_SUMMARY_ALIAS_DESCRIPTION)
    private String alias;
    @DataField(description = ParamConstants.STUDY_SUMMARY_CREATOR_ID_DESCRIPTION)
    private String creatorId;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.STUDY_SUMMARY_INTERNAL_DESCRIPTION)
    private StudyInternal internal;
    @DataField(description = ParamConstants.STUDY_SUMMARY_SIZE_DESCRIPTION)
    private long size;
    @DataField(description = ParamConstants.STUDY_SUMMARY_CIPHER_DESCRIPTION)
    private String cipher;

    @DataField(description = ParamConstants.STUDY_SUMMARY_GROUPS_DESCRIPTION)
    private List<Group> groups;

    @DataField(description = ParamConstants.STUDY_SUMMARY_EXPERIMENTS_DESCRIPTION)
    private List<FileExperiment> experiments;

    @DataField(description = ParamConstants.STUDY_SUMMARY_FILES_DESCRIPTION)
    private long files;
    @DataField(description = ParamConstants.STUDY_SUMMARY_JOBS_DESCRIPTION)
    private long jobs;
    @DataField(description = ParamConstants.STUDY_SUMMARY_INDIVIDUALS_DESCRIPTION)
    private long individuals;
    @DataField(description = ParamConstants.STUDY_SUMMARY_SAMPLES_DESCRIPTION)
    private long samples;
    @DataField(description = ParamConstants.STUDY_SUMMARY_COHORTS_DESCRIPTION)
    private long cohorts;

    @DataField(description = ParamConstants.STUDY_SUMMARY_VARIABLE_SETS_DESCRIPTION)
    private List<VariableSet> variableSets;

    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public StudySummary() {
    }

    public String getName() {
        return name;
    }

    public StudySummary setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public StudySummary setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public StudySummary setCreatorId(String creatorId) {
        this.creatorId = creatorId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public StudySummary setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudySummary setDescription(String description) {
        this.description = description;
        return this;
    }

    public StudyInternal getInternal() {
        return internal;
    }

    public StudySummary setInternal(StudyInternal internal) {
        this.internal = internal;
        return this;
    }

    public long getSize() {
        return size;
    }

    public StudySummary setSize(long size) {
        this.size = size;
        return this;
    }

    public String getCipher() {
        return cipher;
    }

    public StudySummary setCipher(String cipher) {
        this.cipher = cipher;
        return this;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public StudySummary setGroups(List<Group> groups) {
        this.groups = groups;
        return this;
    }

    public List<FileExperiment> getExperiments() {
        return experiments;
    }

    public StudySummary setExperiments(List<FileExperiment> experiments) {
        this.experiments = experiments;
        return this;
    }

    public long getFiles() {
        return files;
    }

    public StudySummary setFiles(long files) {
        this.files = files;
        return this;
    }

    public long getJobs() {
        return jobs;
    }

    public StudySummary setJobs(long jobs) {
        this.jobs = jobs;
        return this;
    }

    public long getIndividuals() {
        return individuals;
    }

    public StudySummary setIndividuals(long individuals) {
        this.individuals = individuals;
        return this;
    }

    public long getSamples() {
        return samples;
    }

    public StudySummary setSamples(long samples) {
        this.samples = samples;
        return this;
    }

    public long getCohorts() {
        return cohorts;
    }

    public StudySummary setCohorts(long cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public List<VariableSet> getVariableSets() {
        return variableSets;
    }

    public StudySummary setVariableSets(List<VariableSet> variableSets) {
        this.variableSets = variableSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public StudySummary setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
