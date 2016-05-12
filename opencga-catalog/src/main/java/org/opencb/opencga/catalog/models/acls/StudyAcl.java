package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;

/**
 * Created by pfurio on 11/05/16.
 */
public class StudyAcl {

    private String role;
    private List<String> users;
    private EnumSet<StudyPermissions> permissions;

    public StudyAcl() {
    }

    public StudyAcl(String role, List<String> users, EnumSet<StudyPermissions> permissions) {
        this.role = role;
        this.users = users;
        this.permissions = permissions;
    }

    public StudyAcl(String role, List<String> users, ObjectMap permissions) {
        this.role = role;
        this.users = users;

        EnumSet<StudyPermissions> aux = EnumSet.allOf(StudyPermissions.class);
        for (StudyPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public String getRole() {
        return role;
    }

    public StudyAcl setRole(String role) {
        this.role = role;
        return this;
    }

    public List<String> getUsers() {
        return users;
    }

    public StudyAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<StudyPermissions> getPermissions() {
        return permissions;
    }

    public StudyAcl setPermissions(EnumSet<StudyPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }

    public enum StudyPermissions {
        VIEW_STUDY,
        UPDATE_STUDY,
        SHARE_STUDY,

        // FILES
        CREATE_FILES,
        VIEW_FILE_HEADER,
        VIEW_FILE_STATS,
        VIEW_FILE_CONTENT,
        DELETE_FILES,
        UPDATE_FILES,
        DOWNLOAD_FILES,
        SHARE_FILES,

        // JOBS
        CREATE_JOBS,
        VIEW_JOBS,
        UPDATE_JOBS,
        DELETE_JOBS,
        SHARE_JOBS,

        // SAMPLES
        CREATE_SAMPLES,
        VIEW_SAMPLES,
        UPDATE_SAMPLES,
        DELETE_SAMPLES,
        CREATE_SAMPLE_ANNOTATIONS,
        VIEW_SAMPLE_ANNOTATIONS,
        UPDATE_SAMPLE_ANNOTATIONS,
        DELETE_SAMPLE_ANNOTATIONS,
        SHARE_SAMPLES,

        // INDIVIDUALS
        CREATE_INDIVIDUALS,
        VIEW_INDIVIDUALS,
        UPDATE_INDIVIDUALS,
        DELETE_INDIVIDUALS,
        CREATE_INDIVIDUAL_ANNOTATIONS,
        VIEW_INDIVIDUAL_ANNOTATIONS,
        UPDATE_INDIVIDUAL_ANNOTATIONS,
        DELETE_INDIVIDUAL_ANNOTATIONS,
        SHARE_INDIVIDUALS,

        // COHORTS
        CREATE_COHORTS,
        VIEW_COHORTS,
        UPDATE_COHORTS,
        DELETE_COHORTS,
        CREATE_COHORT_ANNOTATIONS,
        VIEW_COHORT_ANNOTATIONS,
        UPDATE_COHORT_ANNOTATIONS,
        DELETE_COHORT_ANNOTATIONS,
        SHARE_COHORTS,

        // DATASETS
        CREATE_DATASETS,
        VIEW_DATASETS,
        UPDATE_DATASETS,
        DELETE_DATASETS
    }
}
