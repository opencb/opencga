/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.catalog.models.acls.permissions;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class StudyAclEntry extends AbstractAclEntry<StudyAclEntry.StudyPermissions> {

    public StudyAclEntry() {
    }

    public StudyAclEntry(String member, EnumSet<StudyPermissions> permissions) {
        super(member, permissions);
    }

    public StudyAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(StudyPermissions.class));

        EnumSet<StudyPermissions> aux = EnumSet.allOf(StudyPermissions.class);
        for (StudyPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public StudyAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(StudyPermissions.class));

        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(StudyPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    // Study Permissions

    private static final int FILE = 1;
    private static final int SAMPLE = 2;
    private static final int JOB = 3;
    private static final int COHORT = 4;
    private static final int INDIVIDUAL = 5;
    private static final int DATASET = 6;
    private static final int DISEASE_PANEL = 7;

    public enum StudyPermissions {
        VIEW_STUDY,
        UPDATE_STUDY,
        SHARE_STUDY, // Add members to groups, add members/groups to roles.
        CREATE_VARIABLE_SET,
        VIEW_VARIABLE_SET,
        UPDATE_VARIABLE_SET,
        DELETE_VARIABLE_SET,

        // FILES
        CREATE_FILES(FileAclEntry.FilePermissions.CREATE.name(), FILE),
        VIEW_FILE_HEADERS(FileAclEntry.FilePermissions.VIEW_HEADER.name(), FILE),
        VIEW_FILE_CONTENTS(FileAclEntry.FilePermissions.VIEW_CONTENT.name(), FILE),
        VIEW_FILES(FileAclEntry.FilePermissions.VIEW.name(), FILE),
        DELETE_FILES(FileAclEntry.FilePermissions.DELETE.name(), FILE),
        UPDATE_FILES(FileAclEntry.FilePermissions.UPDATE.name(), FILE),
        DOWNLOAD_FILES(FileAclEntry.FilePermissions.DOWNLOAD.name(), FILE),
        UPLOAD_FILES(FileAclEntry.FilePermissions.UPLOAD.name(), FILE),
        SHARE_FILES(FileAclEntry.FilePermissions.SHARE.name(), FILE),

        // JOBS
        CREATE_JOBS,
        VIEW_JOBS(JobAclEntry.JobPermissions.VIEW.name(), JOB),
        UPDATE_JOBS(JobAclEntry.JobPermissions.UPDATE.name(), JOB),
        DELETE_JOBS(JobAclEntry.JobPermissions.DELETE.name(), JOB),
        SHARE_JOBS(JobAclEntry.JobPermissions.SHARE.name(), JOB),

        // SAMPLES
        CREATE_SAMPLES,
        VIEW_SAMPLES(SampleAclEntry.SamplePermissions.VIEW.name(), SAMPLE),
        UPDATE_SAMPLES(SampleAclEntry.SamplePermissions.UPDATE.name(), SAMPLE),
        DELETE_SAMPLES(SampleAclEntry.SamplePermissions.DELETE.name(), SAMPLE),
        SHARE_SAMPLES(SampleAclEntry.SamplePermissions.SHARE.name(), SAMPLE),
        CREATE_SAMPLE_ANNOTATIONS(SampleAclEntry.SamplePermissions.CREATE_ANNOTATIONS.name(), SAMPLE),
        VIEW_SAMPLE_ANNOTATIONS(SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(), SAMPLE),
        UPDATE_SAMPLE_ANNOTATIONS(SampleAclEntry.SamplePermissions.UPDATE_ANNOTATIONS.name(), SAMPLE),
        DELETE_SAMPLE_ANNOTATIONS(SampleAclEntry.SamplePermissions.DELETE_ANNOTATIONS.name(), SAMPLE),

        // INDIVIDUALS
        CREATE_INDIVIDUALS,
        VIEW_INDIVIDUALS(IndividualAclEntry.IndividualPermissions.VIEW.name(), INDIVIDUAL),
        UPDATE_INDIVIDUALS(IndividualAclEntry.IndividualPermissions.UPDATE.name(), INDIVIDUAL),
        DELETE_INDIVIDUALS(IndividualAclEntry.IndividualPermissions.DELETE.name(), INDIVIDUAL),
        SHARE_INDIVIDUALS(IndividualAclEntry.IndividualPermissions.SHARE.name(), INDIVIDUAL),
        CREATE_INDIVIDUAL_ANNOTATIONS(IndividualAclEntry.IndividualPermissions.CREATE_ANNOTATIONS.name(), INDIVIDUAL),
        VIEW_INDIVIDUAL_ANNOTATIONS(IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), INDIVIDUAL),
        UPDATE_INDIVIDUAL_ANNOTATIONS(IndividualAclEntry.IndividualPermissions.UPDATE_ANNOTATIONS.name(), INDIVIDUAL),
        DELETE_INDIVIDUAL_ANNOTATIONS(IndividualAclEntry.IndividualPermissions.DELETE_ANNOTATIONS.name(), INDIVIDUAL),

        // COHORTS
        CREATE_COHORTS,
        VIEW_COHORTS(CohortAclEntry.CohortPermissions.VIEW.name(), COHORT),
        UPDATE_COHORTS(CohortAclEntry.CohortPermissions.UPDATE.name(), COHORT),
        DELETE_COHORTS(CohortAclEntry.CohortPermissions.DELETE.name(), COHORT),
        SHARE_COHORTS(CohortAclEntry.CohortPermissions.SHARE.name(), COHORT),
        CREATE_COHORT_ANNOTATIONS(CohortAclEntry.CohortPermissions.CREATE_ANNOTATIONS.name(), COHORT),
        VIEW_COHORT_ANNOTATIONS(CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name(), COHORT),
        UPDATE_COHORT_ANNOTATIONS(CohortAclEntry.CohortPermissions.UPDATE_ANNOTATIONS.name(), COHORT),
        DELETE_COHORT_ANNOTATIONS(CohortAclEntry.CohortPermissions.DELETE_ANNOTATIONS.name(), COHORT),

        // DATASETS
        CREATE_DATASETS,
        VIEW_DATASETS(DatasetAclEntry.DatasetPermissions.VIEW.name(), DATASET),
        UPDATE_DATASETS(DatasetAclEntry.DatasetPermissions.UPDATE.name(), DATASET),
        DELETE_DATASETS(DatasetAclEntry.DatasetPermissions.DELETE.name(), DATASET),
        SHARE_DATASETS(DatasetAclEntry.DatasetPermissions.SHARE.name(), DATASET),

        // DISEASE PANELS
        CREATE_PANELS,
        VIEW_PANELS(DiseasePanelAclEntry.DiseasePanelPermissions.VIEW.name(), DISEASE_PANEL),
        UPDATE_PANELS(DiseasePanelAclEntry.DiseasePanelPermissions.UPDATE.name(), DISEASE_PANEL),
        DELETE_PANELS(DiseasePanelAclEntry.DiseasePanelPermissions.DELETE.name(), DISEASE_PANEL),
        SHARE_PANELS(DiseasePanelAclEntry.DiseasePanelPermissions.SHARE.name(), DISEASE_PANEL);

        private String permission;
        private int type;

        StudyPermissions(String permission, int type) {
            this.permission = permission;
            this.type = type;
        }

        StudyPermissions() {
            this(null, -1);
        }

        public FileAclEntry.FilePermissions getFilePermission() {
            if (this.type == FILE) {
                return FileAclEntry.FilePermissions.valueOf(this.permission);
            }
            return null;
        }

        public SampleAclEntry.SamplePermissions getSamplePermission() {
            if (this.type == SAMPLE) {
                return SampleAclEntry.SamplePermissions.valueOf(this.permission);
            }
            return null;
        }

        public JobAclEntry.JobPermissions getJobPermission() {
            if (this.type == JOB) {
                return JobAclEntry.JobPermissions.valueOf(this.permission);
            }
            return null;
        }

        public IndividualAclEntry.IndividualPermissions getIndividualPermission() {
            if (this.type == INDIVIDUAL) {
                return IndividualAclEntry.IndividualPermissions.valueOf(this.permission);
            }
            return null;
        }

        public CohortAclEntry.CohortPermissions getCohortPermission() {
            if (this.type == COHORT) {
                return CohortAclEntry.CohortPermissions.valueOf(this.permission);
            }
            return null;
        }

        public DatasetAclEntry.DatasetPermissions getDatasetPermission() {
            if (this.type == DATASET) {
                return DatasetAclEntry.DatasetPermissions.valueOf(this.permission);
            }
            return null;
        }

        public DiseasePanelAclEntry.DiseasePanelPermissions getDiseasePanelPermission() {
            if (this.type == DISEASE_PANEL) {
                return DiseasePanelAclEntry.DiseasePanelPermissions.valueOf(this.permission);
            }
            return null;
        }
    }
}
