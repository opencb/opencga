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

package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.cohort.CohortAclEntry;
import org.opencb.opencga.core.models.family.FamilyAclEntry;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.job.JobAclEntry;
import org.opencb.opencga.core.models.panel.PanelAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.StudyAclEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public final class Enums {

    private Enums() {
    }

    public enum Entity {
        SAMPLES(Resource.SAMPLE),
        FILES(Resource.FILE),
        COHORTS(Resource.COHORT),
        INDIVIDUALS(Resource.INDIVIDUAL),
        FAMILIES(Resource.FAMILY),
        JOBS(Resource.JOB),
        CLINICAL_ANALYSES(Resource.CLINICAL_ANALYSIS),
        DISEASE_PANELS(Resource.DISEASE_PANEL);

        private final Resource resource;

        Entity(Resource resource) {
            this.resource = resource;
        }

        public Resource getResource() {
            return resource;
        }
    }

    public enum Resource {
        USER,
        PROJECT,
        STUDY,
        FILE,
        SAMPLE,
        JOB,
        INDIVIDUAL,
        COHORT,
        DISEASE_PANEL,
        FAMILY,
        CLINICAL_ANALYSIS,
        INTERPRETATION,
        VARIANT,
        ALIGNMENT,
        CLINICAL,
        EXPRESSION,
        FUNCTIONAL;

        public List<String> getFullPermissionList() {
            switch (this) {
                case STUDY:
                    return EnumSet.allOf(StudyAclEntry.StudyPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case SAMPLE:
                    return EnumSet.allOf(SampleAclEntry.SamplePermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case FILE:
                    return EnumSet.allOf(FileAclEntry.FilePermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case COHORT:
                    return EnumSet.allOf(CohortAclEntry.CohortPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case INDIVIDUAL:
                    return EnumSet.allOf(IndividualAclEntry.IndividualPermissions.class).stream().map(String::valueOf)
                            .collect(Collectors.toList());
                case FAMILY:
                    return EnumSet.allOf(FamilyAclEntry.FamilyPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case JOB:
                    return EnumSet.allOf(JobAclEntry.JobPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case DISEASE_PANEL:
                    return EnumSet.allOf(PanelAclEntry.PanelPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case CLINICAL_ANALYSIS:
                    return EnumSet.allOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class).stream().map(String::valueOf)
                            .collect(Collectors.toList());
                default:
                    return Collections.emptyList();
            }
        }
    }

    public enum Action {
        CREATE,
        UPDATE,
        INFO,
        SEARCH,
        COUNT,
        DELETE,
        DOWNLOAD,
        VIEW_LOG,
        VIEW_CONTENT,
        HEAD_CONTENT,
        TAIL_CONTENT,
        INDEX,
        CHANGE_PERMISSION,

        LOGIN,
        CHANGE_USER_PASSWORD,
        RESET_USER_PASSWORD,
        CHANGE_USER_CONFIG,
        FETCH_USER_CONFIG,

        INCREMENT_PROJECT_RELEASE,

        FETCH_STUDY_GROUPS,
        ADD_STUDY_GROUP,
        REMOVE_STUDY_GROUP,
        UPDATE_USERS_FROM_STUDY_GROUP,
        FETCH_STUDY_PERMISSION_RULES,
        ADD_STUDY_PERMISSION_RULE,
        REMOVE_STUDY_PERMISSION_RULE,
        FETCH_ACLS,
        UPDATE_ACLS,
        FETCH_VARIABLE_SET,
        ADD_VARIABLE_SET,
        DELETE_VARIABLE_SET,
        ADD_VARIABLE_TO_VARIABLE_SET,
        REMOVE_VARIABLE_FROM_VARIABLE_SET,

        AGGREGATION_STATS,

        RELATIVES,

        UPLOAD,
        LINK,
        UNLINK,
        GREP,
        TREE,
        DOWNLOAD_AND_REGISTER,
        MOVE_AND_REGISTER,

        VISIT,

        IMPORT,

        IMPORT_EXTERNAL_USERS,
        IMPORT_EXTERNAL_GROUP_OF_USERS,
        SYNC_EXTERNAL_GROUP_OF_USERS,

        // Variants
        VARIANT_STORAGE_OPERATION,
        SAMPLE_DATA,
        FACET
    }

    public enum Priority {
        URGENT(1),
        HIGH(2),
        MEDIUM(3),
        LOW(4);

        private int value;

        Priority(int value) {
            this.value = value;
        }

        public static Priority getPriority(int value) {
            switch (value) {
                case 1:
                    return Priority.LOW;
                case 2:
                    return Priority.MEDIUM;
                case 3:
                    return Priority.HIGH;
                case 4:
                    return Priority.URGENT;
                default:
                    return null;
            }
        }

        public static Priority getPriority(String key) {
            return Priority.valueOf(key);
        }

        public int getValue() {
            return this.value;
        }

    }

    public enum PermissionRuleAction {
        ADD,
        REMOVE,
        REVERT,
        NONE
    }

    public enum CohortType {
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

    public static class ExecutionStatus extends Status {

        /**
         * PENDING status means that the job is ready to be put into the queue.
         */
        public static final String PENDING = "PENDING";
        /**
         * QUEUED status means that the job is waiting on the queue to have an available slot for execution.
         */
        public static final String QUEUED = "QUEUED";
        /**
         * RUNNING status means that the job is running.
         */
        public static final String RUNNING = "RUNNING";
        /**
         * DONE status means that the job has finished the execution and everything finished successfully.
         */
        public static final String DONE = "DONE";
        /**
         * ERROR status means that the job finished with an error.
         */
        public static final String ERROR = "ERROR";
        /**
         * UNKNOWN status means that the job status could not be obtained.
         */
        public static final String UNKNOWN = "UNKNOWN";
        /**
         * ABORTED status means that the job was aborted and could not be executed.
         */
        public static final String ABORTED = "ABORTED";
        /**
         * REGISTERING status means that the job status could not be obtained.
         */
        public static final String REGISTERING = "REGISTERING";
        /**
         * UNREGISTERED status means that the job status could not be obtained.
         */
        public static final String UNREGISTERED = "UNREGISTERED";


        public static final List<String> STATUS_LIST = Arrays.asList(PENDING, QUEUED, RUNNING, DONE, ERROR, UNKNOWN, REGISTERING,
                UNREGISTERED, ABORTED, DELETED);

        public ExecutionStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public ExecutionStatus(String status) {
            this(status, "");
        }

        public ExecutionStatus() {
            this(PENDING, "");
        }


        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && STATUS_LIST.contains(status)) {
                return true;
            }
            return false;
        }
    }
}
