package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.models.acls.permissions.*;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public class Enums {

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
        CATALOG;

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

        UPLOAD,
        LINK,
        UNLINK,
        GREP,
        TREE,

        VISIT,

        IMPORT,

        IMPORT_EXTERNAL_USERS,
        IMPORT_EXTERNAL_GROUP_OF_USERS,
        SYNC_EXTERNAL_GROUP_OF_USERS,

        // Variants
        SAMPLE_DATA,
        FACET
    }

}
