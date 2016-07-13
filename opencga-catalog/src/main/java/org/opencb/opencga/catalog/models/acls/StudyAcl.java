package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class StudyAcl extends ParentAcl<StudyAcl.StudyPermissions> {

    public StudyAcl() {
    }

    public StudyAcl(String member, EnumSet<StudyPermissions> permissions) {
        super(member, permissions);
    }

    public StudyAcl(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(StudyPermissions.class));

        EnumSet<StudyPermissions> aux = EnumSet.allOf(StudyPermissions.class);
        for (StudyPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public StudyAcl(String member, List<String> permissions) {
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
        CREATE_FILES(FileAcl.FilePermissions.CREATE.name(), FILE),
        VIEW_FILE_HEADERS(FileAcl.FilePermissions.VIEW_HEADER.name(), FILE),
        VIEW_FILE_CONTENTS(FileAcl.FilePermissions.VIEW_CONTENT.name(), FILE),
        VIEW_FILES(FileAcl.FilePermissions.VIEW.name(), FILE),
        DELETE_FILES(FileAcl.FilePermissions.DELETE.name(), FILE),
        UPDATE_FILES(FileAcl.FilePermissions.UPDATE.name(), FILE),
        DOWNLOAD_FILES(FileAcl.FilePermissions.DOWNLOAD.name(), FILE),
        SHARE_FILES(FileAcl.FilePermissions.SHARE.name(), FILE),

        // JOBS
        CREATE_JOBS,
        VIEW_JOBS(JobAcl.JobPermissions.VIEW.name(), JOB),
        UPDATE_JOBS(JobAcl.JobPermissions.UPDATE.name(), JOB),
        DELETE_JOBS(JobAcl.JobPermissions.DELETE.name(), JOB),
        SHARE_JOBS(JobAcl.JobPermissions.SHARE.name(), JOB),

        // SAMPLES
        CREATE_SAMPLES,
        VIEW_SAMPLES(SampleAcl.SamplePermissions.VIEW.name(), SAMPLE),
        UPDATE_SAMPLES(SampleAcl.SamplePermissions.UPDATE.name(), SAMPLE),
        DELETE_SAMPLES(SampleAcl.SamplePermissions.DELETE.name(), SAMPLE),
        SHARE_SAMPLES(SampleAcl.SamplePermissions.SHARE.name(), SAMPLE),
        CREATE_SAMPLE_ANNOTATIONS(SampleAcl.SamplePermissions.CREATE_ANNOTATIONS.name(), SAMPLE),
        VIEW_SAMPLE_ANNOTATIONS(SampleAcl.SamplePermissions.VIEW_ANNOTATIONS.name(), SAMPLE),
        UPDATE_SAMPLE_ANNOTATIONS(SampleAcl.SamplePermissions.UPDATE_ANNOTATIONS.name(), SAMPLE),
        DELETE_SAMPLE_ANNOTATIONS(SampleAcl.SamplePermissions.DELETE_ANNOTATIONS.name(), SAMPLE),

        // INDIVIDUALS
        CREATE_INDIVIDUALS,
        VIEW_INDIVIDUALS(IndividualAcl.IndividualPermissions.VIEW.name(), INDIVIDUAL),
        UPDATE_INDIVIDUALS(IndividualAcl.IndividualPermissions.UPDATE.name(), INDIVIDUAL),
        DELETE_INDIVIDUALS(IndividualAcl.IndividualPermissions.DELETE.name(), INDIVIDUAL),
        SHARE_INDIVIDUALS(IndividualAcl.IndividualPermissions.SHARE.name(), INDIVIDUAL),
        CREATE_INDIVIDUAL_ANNOTATIONS(IndividualAcl.IndividualPermissions.CREATE_ANNOTATIONS.name(), INDIVIDUAL),
        VIEW_INDIVIDUAL_ANNOTATIONS(IndividualAcl.IndividualPermissions.VIEW_ANNOTATIONS.name(), INDIVIDUAL),
        UPDATE_INDIVIDUAL_ANNOTATIONS(IndividualAcl.IndividualPermissions.UPDATE_ANNOTATIONS.name(), INDIVIDUAL),
        DELETE_INDIVIDUAL_ANNOTATIONS(IndividualAcl.IndividualPermissions.DELETE_ANNOTATIONS.name(), INDIVIDUAL),

        // COHORTS
        CREATE_COHORTS,
        VIEW_COHORTS(CohortAcl.CohortPermissions.VIEW.name(), COHORT),
        UPDATE_COHORTS(CohortAcl.CohortPermissions.UPDATE.name(), COHORT),
        DELETE_COHORTS(CohortAcl.CohortPermissions.DELETE.name(), COHORT),
        SHARE_COHORTS(CohortAcl.CohortPermissions.SHARE.name(), COHORT),
        CREATE_COHORT_ANNOTATIONS(CohortAcl.CohortPermissions.CREATE_ANNOTATIONS.name(), COHORT),
        VIEW_COHORT_ANNOTATIONS(CohortAcl.CohortPermissions.VIEW_ANNOTATIONS.name(), COHORT),
        UPDATE_COHORT_ANNOTATIONS(CohortAcl.CohortPermissions.UPDATE_ANNOTATIONS.name(), COHORT),
        DELETE_COHORT_ANNOTATIONS(CohortAcl.CohortPermissions.DELETE_ANNOTATIONS.name(), COHORT),

        // DATASETS
        CREATE_DATASETS,
        VIEW_DATASETS(DatasetAcl.DatasetPermissions.VIEW.name(), DATASET),
        UPDATE_DATASETS(DatasetAcl.DatasetPermissions.UPDATE.name(), DATASET),
        DELETE_DATASETS(DatasetAcl.DatasetPermissions.DELETE.name(), DATASET),
        SHARE_DATASETS(DatasetAcl.DatasetPermissions.SHARE.name(), DATASET),

        // DISEASE PANELS
        CREATE_PANELS,
        VIEW_PANELS(DiseasePanelAcl.DiseasePanelPermissions.VIEW.name(), DISEASE_PANEL),
        UPDATE_PANELS(DiseasePanelAcl.DiseasePanelPermissions.UPDATE.name(), DISEASE_PANEL),
        DELETE_PANELS(DiseasePanelAcl.DiseasePanelPermissions.DELETE.name(), DISEASE_PANEL),
        SHARE_PANELS(DiseasePanelAcl.DiseasePanelPermissions.SHARE.name(), DISEASE_PANEL);

        private String permission;
        private int type;

        StudyPermissions(String permission, int type) {
            this.permission = permission;
            this.type = type;
        }

        StudyPermissions() {
            this(null, -1);
        }

        public FileAcl.FilePermissions getFilePermission() {
            if (this.type == FILE) {
                return FileAcl.FilePermissions.valueOf(this.permission);
            }
            return null;
        }

        public SampleAcl.SamplePermissions getSamplePermission() {
            if (this.type == SAMPLE) {
                return SampleAcl.SamplePermissions.valueOf(this.permission);
            }
            return null;
        }

        public JobAcl.JobPermissions getJobPermission() {
            if (this.type == JOB) {
                return JobAcl.JobPermissions.valueOf(this.permission);
            }
            return null;
        }

        public IndividualAcl.IndividualPermissions getIndividualPermission() {
            if (this.type == INDIVIDUAL) {
                return IndividualAcl.IndividualPermissions.valueOf(this.permission);
            }
            return null;
        }

        public CohortAcl.CohortPermissions getCohortPermission() {
            if (this.type == COHORT) {
                return CohortAcl.CohortPermissions.valueOf(this.permission);
            }
            return null;
        }

        public DatasetAcl.DatasetPermissions getDatasetPermission() {
            if (this.type == DATASET) {
                return DatasetAcl.DatasetPermissions.valueOf(this.permission);
            }
            return null;
        }

        public DiseasePanelAcl.DiseasePanelPermissions getDiseasePanelPermission() {
            if (this.type == DISEASE_PANEL) {
                return DiseasePanelAcl.DiseasePanelPermissions.valueOf(this.permission);
            }
            return null;
        }
    }
}
