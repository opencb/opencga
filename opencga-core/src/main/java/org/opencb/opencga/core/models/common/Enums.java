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

import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.cohort.CohortPermissions;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.file.FilePermissions;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.job.JobPermissions;
import org.opencb.opencga.core.models.panel.PanelPermissions;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.StudyPermissions;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public final class Enums {

    private Enums() {
    }

    /**
     * Interface representing an entity type in OpenCGA.
     * This interface can be implemented in enterprise projects to add custom entity types
     * while maintaining compatibility with the OpenCGA core system.
     * <p>
     * Example usage in enterprise project:
     * <pre>
     * public enum EnterpriseEntity implements Enums.EntityType {
     *     CONTRACTS(EnterpriseResource.CONTRACT),
     *     INVOICES(EnterpriseResource.INVOICE);
     *
     *     private final ResourceType resource;
     *
     *     EnterpriseEntity(ResourceType resource) {
     *         this.resource = resource;
     *     }
     *
     *     public ResourceType getResource() {
     *         return resource;
     *     }
     * }
     * </pre>
     */
    public interface EntityType {
        /**
         * Returns the resource type associated with this entity.
         *
         * @return the resource type
         */
        ResourceType getResource();

        /**
         * Returns the name of this entity type.
         *
         * @return the entity name
         */
        String name();
    }

    /**
     * Interface representing a resource type in OpenCGA.
     * This interface can be implemented in enterprise projects to add custom resource types
     * while maintaining compatibility with the OpenCGA core system, including permissions,
     * auditing, and authorization.
     * <p>
     * Enterprise implementations should provide their own permission management logic
     * by implementing the three methods: getFullPermissionList(), toStudyPermission(),
     * and fromStudyPermission().
     * <p>
     * Example usage in enterprise project:
     * <pre>
     * public enum EnterpriseResource implements Enums.ResourceType {
     *     CONTRACT,
     *     INVOICE,
     *     BILLING;
     *
     *     public List&lt;String&gt; getFullPermissionList() {
     *         // Return enterprise-specific permissions
     *         return Arrays.asList("VIEW", "EDIT", "DELETE", "APPROVE");
     *     }
     *
     *     public String toStudyPermission(String permission) {
     *         // Convert to study permission if applicable
     *         return permission;
     *     }
     *
     *     public String fromStudyPermission(String permission) {
     *         // Convert from study permission if applicable
     *         return permission;
     *     }
     * }
     * </pre>
     */
    public interface ResourceType {
        /**
         * Returns the full list of permissions available for this resource type.
         * Enterprise implementations should return the list of permissions specific to their custom resources.
         *
         * @return list of permission strings
         */
        List<String> getFullPermissionList();

        /**
         * Converts a resource-specific permission to its equivalent study-level permission.
         * This is used for hierarchical permission management where study permissions control
         * access to resources within the study.
         *
         * @param permission the resource-specific permission
         * @return the equivalent study permission string
         */
        String toStudyPermission(String permission);

        /**
         * Converts a study-level permission to its equivalent resource-specific permission.
         * This is the inverse operation of {@link #toStudyPermission(String)}.
         *
         * @param permission the study permission
         * @return the equivalent resource-specific permission string
         * @throws IllegalArgumentException if the study permission doesn't have an equivalent for this resource
         */
        String fromStudyPermission(String permission);

        /**
         * Returns the name of this resource type.
         *
         * @return the resource name
         */
        String name();
    }

    /**
     * Standard OpenCGA entity types.
     * Each entity represents a collection of resources that can be queried and managed together.
     * Entities are used primarily for permission rules that apply to multiple instances of a resource type.
     */
    public enum Entity implements EntityType {
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

    /**
     * Standard OpenCGA resource types.
     * Resources represent the main data objects in OpenCGA that can be accessed, modified,
     * and have permissions assigned to them. They are used throughout the system for:
     * <ul>
     *   <li>Audit logging - tracking operations performed on resources</li>
     *   <li>Authorization - managing access control and permissions</li>
     *   <li>API endpoints - routing requests to appropriate handlers</li>
     * </ul>
     */
    public enum Resource implements ResourceType {
        AUDIT,
        NOTE,
        ORGANIZATION,
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
        RGA,
        FUNCTIONAL,
        EXTERNAL_TOOL,
        RESOURCE;

        public List<String> getFullPermissionList() {
            switch (this) {
                case STUDY:
                    return EnumSet.allOf(StudyPermissions.Permissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case SAMPLE:
                    return EnumSet.allOf(SamplePermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case FILE:
                    return EnumSet.allOf(FilePermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case COHORT:
                    return EnumSet.allOf(CohortPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case INDIVIDUAL:
                    return EnumSet.allOf(IndividualPermissions.class).stream().map(String::valueOf)
                            .collect(Collectors.toList());
                case FAMILY:
                    return EnumSet.allOf(FamilyPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case JOB:
                    return EnumSet.allOf(JobPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case DISEASE_PANEL:
                    return EnumSet.allOf(PanelPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
                case CLINICAL_ANALYSIS:
                    return EnumSet.allOf(ClinicalAnalysisPermissions.class).stream().map(String::valueOf)
                            .collect(Collectors.toList());
                default:
                    return Collections.emptyList();
            }
        }

        public String toStudyPermission(String permission) {
            switch (this) {
                case SAMPLE:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.SAMPLE).name();
                case FILE:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.FILE).name();
                case COHORT:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.COHORT).name();
                case INDIVIDUAL:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.INDIVIDUAL).name();
                case FAMILY:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.FAMILY).name();
                case JOB:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.JOB).name();
                case DISEASE_PANEL:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.DISEASE_PANEL).name();
                case CLINICAL_ANALYSIS:
                    return StudyPermissions.Permissions.getStudyPermission(permission, StudyPermissions.CLINICAL_ANALYSIS).name();
                case STUDY:
                default:
                    return permission;
            }
        }

        public String fromStudyPermission(String permission) {
            StudyPermissions.Permissions studyPermission = StudyPermissions.Permissions.valueOf(permission);
            switch (this) {
                case SAMPLE:
                    if (studyPermission.getType() == StudyPermissions.SAMPLE) {
                        return studyPermission.getPermission();
                    }
                    break;
                case FILE:
                    if (studyPermission.getType() == StudyPermissions.FILE) {
                        return studyPermission.getPermission();
                    }
                    break;
                case COHORT:
                    if (studyPermission.getType() == StudyPermissions.COHORT) {
                        return studyPermission.getPermission();
                    }
                    break;
                case INDIVIDUAL:
                    if (studyPermission.getType() == StudyPermissions.INDIVIDUAL) {
                        return studyPermission.getPermission();
                    }
                    break;
                case FAMILY:
                    if (studyPermission.getType() == StudyPermissions.FAMILY) {
                        return studyPermission.getPermission();
                    }
                    break;
                case JOB:
                    if (studyPermission.getType() == StudyPermissions.JOB) {
                        return studyPermission.getPermission();
                    }
                    break;
                case DISEASE_PANEL:
                    if (studyPermission.getType() == StudyPermissions.DISEASE_PANEL) {
                        return studyPermission.getPermission();
                    }
                    break;
                case CLINICAL_ANALYSIS:
                    if (studyPermission.getType() == StudyPermissions.CLINICAL_ANALYSIS) {
                        return studyPermission.getPermission();
                    }
                    break;
                case STUDY:
                default:
                    return permission;
            }
            throw new IllegalArgumentException("Study permission '" + permission + "' does not seem to have an equivalent valid permission"
                    + " for the  entity '" + this + "'");
        }

    }

    public enum Action {
        CREATE,
        GENERATE,
        CLEAR,
        UPDATE,
        UPDATE_INTERNAL,
        MERGE,
        INFO,
        SEARCH,
        COUNT,
        DISTINCT,
        DELETE,
        DOWNLOAD,
        VIEW_LOG,
        VIEW_CONTENT,
        IMAGE_CONTENT,
        HEAD_CONTENT,
        TAIL_CONTENT,
        INDEX,
        CHANGE_PERMISSION,
        REVERT,
        REUSE,

        LOGIN,
        REFRESH_TOKEN,
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

        UPLOAD_TEMPLATE,
        RUN_TEMPLATE,
        DELETE_TEMPLATE,

        AGGREGATION_STATS,

        RELATIVES,

        UPLOAD,
        LINK,
        UNLINK,
        GREP,
        TREE,
        DOWNLOAD_AND_REGISTER,
        MOVE,
        MOVE_AND_REGISTER,

        VISIT,
        KILL_JOB,
        RESCHEDULE_JOB,

        IMPORT,

        FETCH_ORGANIZATION_IDS,
        IMPORT_EXTERNAL_USERS,
        IMPORT_EXTERNAL_GROUP_OF_USERS,
        SYNC_EXTERNAL_GROUP_OF_USERS,

        // RGA
        RESET_RGA_INDEXES,
        UPDATE_RGA_INDEX,

        // Variants
        VARIANT_STORAGE_OPERATION,
        SAMPLE_DATA,
        FACET
    }

    public enum Priority {
        URGENT(1),
        HIGH(2),
        MEDIUM(3),
        LOW(4),
        UNKNOWN(5);

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

    public static class ExecutionStatus extends InternalStatus {

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
            if (InternalStatus.isValid(status)) {
                return true;
            }
            if (status != null && STATUS_LIST.contains(status)) {
                return true;
            }
            return false;
        }
    }
}
