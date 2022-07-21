package org.opencb.opencga.core.models.study;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.cohort.CohortPermissions;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.file.FilePermissions;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.job.JobPermissions;
import org.opencb.opencga.core.models.panel.PanelPermissions;
import org.opencb.opencga.core.models.sample.SamplePermissions;

import java.util.*;

public class StudyPermissions {
    public static final int FILE = 1;
    public static final int SAMPLE = 2;
    public static final int JOB = 3;
    public static final int COHORT = 4;
    public static final int INDIVIDUAL = 5;
    public static final int DISEASE_PANEL = 7;
    public static final int FAMILY = 8;
    public static final int CLINICAL_ANALYSIS = 9;

    // Study Permissions
    public enum Permissions {
        NONE(Collections.emptyList()),
        CONFIDENTIAL_VARIABLE_SET_ACCESS(Collections.emptyList()),

        // FILES
        VIEW_FILES(Collections.emptyList(), FilePermissions.VIEW.name(), FILE),
        VIEW_FILE_HEADER(Collections.singletonList(VIEW_FILES), FilePermissions.VIEW_HEADER.name(), FILE),
        VIEW_FILE_CONTENT(Collections.singletonList(VIEW_FILES), FilePermissions.VIEW_CONTENT.name(), FILE),
        WRITE_FILES(Collections.singletonList(VIEW_FILES), FilePermissions.WRITE.name(), FILE),
        DELETE_FILES(Arrays.asList(VIEW_FILES, WRITE_FILES), FilePermissions.DELETE.name(), FILE),
        DOWNLOAD_FILES(Collections.singletonList(VIEW_FILES), FilePermissions.DOWNLOAD.name(), FILE),
        UPLOAD_FILES(Arrays.asList(WRITE_FILES, VIEW_FILES), FilePermissions.UPLOAD.name(), FILE),
        VIEW_FILE_ANNOTATIONS(Collections.singletonList(VIEW_FILES), FilePermissions.VIEW_ANNOTATIONS.name(), FILE),
        WRITE_FILE_ANNOTATIONS(Arrays.asList(VIEW_FILE_ANNOTATIONS, VIEW_FILES), FilePermissions.WRITE_ANNOTATIONS.name(),
                FILE),
        DELETE_FILE_ANNOTATIONS(Arrays.asList(WRITE_FILE_ANNOTATIONS, VIEW_FILE_ANNOTATIONS, VIEW_FILES),
                FilePermissions.DELETE_ANNOTATIONS.name(), FILE),

        // JOBS
        EXECUTE_JOBS(Collections.emptyList()),
        VIEW_JOBS(Collections.emptyList(), JobPermissions.VIEW.name(), JOB),
        WRITE_JOBS(Collections.singletonList(VIEW_JOBS), JobPermissions.WRITE.name(), JOB),
        DELETE_JOBS(Arrays.asList(VIEW_JOBS, WRITE_JOBS), JobPermissions.DELETE.name(), JOB),

        // SAMPLES
        VIEW_SAMPLES(Collections.emptyList(), SamplePermissions.VIEW.name(), SAMPLE),
        WRITE_SAMPLES(Collections.singletonList(VIEW_SAMPLES), SamplePermissions.WRITE.name(), SAMPLE),
        DELETE_SAMPLES(Arrays.asList(VIEW_SAMPLES, WRITE_SAMPLES), SamplePermissions.DELETE.name(), SAMPLE),
        VIEW_SAMPLE_ANNOTATIONS(Collections.singletonList(VIEW_SAMPLES), SamplePermissions.VIEW_ANNOTATIONS.name(), SAMPLE),
        WRITE_SAMPLE_ANNOTATIONS(Arrays.asList(VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS),
                SamplePermissions.WRITE_ANNOTATIONS.name(), SAMPLE),
        DELETE_SAMPLE_ANNOTATIONS(Arrays.asList(VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS, WRITE_SAMPLE_ANNOTATIONS),
                SamplePermissions.DELETE_ANNOTATIONS.name(), SAMPLE),
        VIEW_AGGREGATED_VARIANTS(Collections.emptyList()),
        VIEW_SAMPLE_VARIANTS(Arrays.asList(VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS, VIEW_AGGREGATED_VARIANTS),
                SamplePermissions.VIEW_VARIANTS.name(), SAMPLE),

        // INDIVIDUALS
        VIEW_INDIVIDUALS(Collections.emptyList(), IndividualPermissions.VIEW.name(), INDIVIDUAL),
        WRITE_INDIVIDUALS(Collections.singletonList(VIEW_INDIVIDUALS), IndividualPermissions.WRITE.name(), INDIVIDUAL),
        DELETE_INDIVIDUALS(Arrays.asList(VIEW_INDIVIDUALS, WRITE_INDIVIDUALS), IndividualPermissions.DELETE.name(),
                INDIVIDUAL),
        VIEW_INDIVIDUAL_ANNOTATIONS(Collections.singletonList(VIEW_INDIVIDUALS),
                IndividualPermissions.VIEW_ANNOTATIONS.name(), INDIVIDUAL),
        WRITE_INDIVIDUAL_ANNOTATIONS(Arrays.asList(VIEW_INDIVIDUALS, VIEW_INDIVIDUAL_ANNOTATIONS),
                IndividualPermissions.WRITE_ANNOTATIONS.name(), INDIVIDUAL),
        DELETE_INDIVIDUAL_ANNOTATIONS(Arrays.asList(VIEW_INDIVIDUALS, VIEW_INDIVIDUAL_ANNOTATIONS, WRITE_INDIVIDUAL_ANNOTATIONS),
                IndividualPermissions.DELETE_ANNOTATIONS.name(), INDIVIDUAL),

        // FAMILIES
        VIEW_FAMILIES(Collections.emptyList(), FamilyPermissions.VIEW.name(), FAMILY),
        WRITE_FAMILIES(Collections.singletonList(VIEW_FAMILIES), FamilyPermissions.WRITE.name(), FAMILY),
        DELETE_FAMILIES(Arrays.asList(VIEW_FAMILIES, WRITE_FAMILIES), FamilyPermissions.DELETE.name(), FAMILY),
        VIEW_FAMILY_ANNOTATIONS(Collections.singletonList(VIEW_FAMILIES), FamilyPermissions.VIEW_ANNOTATIONS.name(), FAMILY),
        WRITE_FAMILY_ANNOTATIONS(Arrays.asList(VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS),
                FamilyPermissions.WRITE_ANNOTATIONS.name(), FAMILY),
        DELETE_FAMILY_ANNOTATIONS(Arrays.asList(VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS, WRITE_FAMILY_ANNOTATIONS),
                FamilyPermissions.DELETE_ANNOTATIONS.name(), FAMILY),

        // COHORTS
        VIEW_COHORTS(Collections.emptyList(), CohortPermissions.VIEW.name(), COHORT),
        WRITE_COHORTS(Collections.singletonList(VIEW_COHORTS), CohortPermissions.WRITE.name(), COHORT),
        DELETE_COHORTS(Arrays.asList(VIEW_COHORTS, WRITE_COHORTS), CohortPermissions.DELETE.name(), COHORT),
        VIEW_COHORT_ANNOTATIONS(Collections.singletonList(VIEW_COHORTS), CohortPermissions.VIEW_ANNOTATIONS.name(), COHORT),
        WRITE_COHORT_ANNOTATIONS(Arrays.asList(VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS),
                CohortPermissions.WRITE_ANNOTATIONS.name(), COHORT),
        DELETE_COHORT_ANNOTATIONS(Arrays.asList(VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS, WRITE_COHORT_ANNOTATIONS),
                CohortPermissions.DELETE_ANNOTATIONS.name(), COHORT),

        // DISEASE PANELS
        VIEW_PANELS(Collections.emptyList(), PanelPermissions.VIEW.name(), DISEASE_PANEL),
        WRITE_PANELS(Collections.singletonList(VIEW_PANELS), PanelPermissions.WRITE.name(), DISEASE_PANEL),
        DELETE_PANELS(Arrays.asList(VIEW_PANELS, WRITE_PANELS), PanelPermissions.DELETE.name(), DISEASE_PANEL),

        // CLINICAL ANALYSIS
        VIEW_CLINICAL_ANALYSIS(Collections.emptyList(), ClinicalAnalysisPermissions.VIEW.name(),
                CLINICAL_ANALYSIS),
        WRITE_CLINICAL_ANALYSIS(Collections.singletonList(VIEW_CLINICAL_ANALYSIS),
                ClinicalAnalysisPermissions.WRITE.name(), CLINICAL_ANALYSIS),
        DELETE_CLINICAL_ANALYSIS(Arrays.asList(VIEW_CLINICAL_ANALYSIS, WRITE_CLINICAL_ANALYSIS),
                ClinicalAnalysisPermissions.DELETE.name(), CLINICAL_ANALYSIS);

        private final static Map<String, Permissions> map;

        static {
            map = new LinkedMap<>();
            for (Permissions params : Permissions.values()) {
                map.put(params.permission + "-" + params.type, params);
            }
        }

        private final List<Permissions> implicitPermissions;
        private String permission;
        private int type;

        Permissions(List<Permissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        Permissions(List<Permissions> implicitPermissions, String permission, int type) {
            this.implicitPermissions = implicitPermissions;
            this.permission = permission;
            this.type = type;
        }

        public static Permissions getStudyPermission(String permission, int type) {
            return map.get(permission + "-" + type);
        }

        public List<Permissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<Permissions> getDependentPermissions() {
            List<Permissions> dependentPermissions = new LinkedList<>();
            for (Permissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }
}
