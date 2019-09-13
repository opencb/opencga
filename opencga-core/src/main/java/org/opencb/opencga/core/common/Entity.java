package org.opencb.opencga.core.common;

import org.opencb.opencga.core.models.acls.permissions.*;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public enum Entity {
    USER,
    PROJECT,
    STUDY,
    SAMPLE,
    FILE,
    DATASET,
    COHORT,
    INDIVIDUAL,
    FAMILY,
    JOB,
    PANEL,
    CLINICAL_ANALYSIS;

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
            case PANEL:
                return EnumSet.allOf(PanelAclEntry.PanelPermissions.class).stream().map(String::valueOf).collect(Collectors.toList());
            case CLINICAL_ANALYSIS:
                return EnumSet.allOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class).stream().map(String::valueOf)
                        .collect(Collectors.toList());
            default:
                return Collections.emptyList();
        }
    }
}
