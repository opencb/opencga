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

import org.apache.commons.collections.map.LinkedMap;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.AbstractAclEntry;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.cohort.CohortAclEntry;
import org.opencb.opencga.core.models.family.FamilyAclEntry;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.job.JobAclEntry;
import org.opencb.opencga.core.models.panel.PanelAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclEntry;

import java.util.*;
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

    public static final int FILE = 1;
    public static final int SAMPLE = 2;
    public static final int JOB = 3;
    public static final int COHORT = 4;
    public static final int INDIVIDUAL = 5;
    public static final int DISEASE_PANEL = 7;
    public static final int FAMILY = 8;
    public static final int CLINICAL_ANALYSIS = 9;

    public enum StudyPermissions {
        CONFIDENTIAL_VARIABLE_SET_ACCESS(Collections.emptyList()),

        // FILES
        VIEW_FILES(Collections.emptyList(), FileAclEntry.FilePermissions.VIEW.name(), FILE),
        VIEW_FILE_HEADERS(Collections.singletonList(VIEW_FILES), FileAclEntry.FilePermissions.VIEW_HEADER.name(), FILE),
        VIEW_FILE_CONTENTS(Collections.singletonList(VIEW_FILES), FileAclEntry.FilePermissions.VIEW_CONTENT.name(), FILE),
        WRITE_FILES(Collections.singletonList(VIEW_FILES), FileAclEntry.FilePermissions.WRITE.name(), FILE),
        DELETE_FILES(Arrays.asList(VIEW_FILES, WRITE_FILES), FileAclEntry.FilePermissions.DELETE.name(), FILE),
        DOWNLOAD_FILES(Collections.singletonList(VIEW_FILES), FileAclEntry.FilePermissions.DOWNLOAD.name(), FILE),
        UPLOAD_FILES(Arrays.asList(WRITE_FILES, VIEW_FILES), FileAclEntry.FilePermissions.UPLOAD.name(), FILE),
        VIEW_FILE_ANNOTATIONS(Collections.singletonList(VIEW_FILES), FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name(), FILE),
        WRITE_FILE_ANNOTATIONS(Arrays.asList(VIEW_FILE_ANNOTATIONS, VIEW_FILES), FileAclEntry.FilePermissions.WRITE_ANNOTATIONS.name(),
                FILE),
        DELETE_FILE_ANNOTATIONS(Arrays.asList(WRITE_FILE_ANNOTATIONS, VIEW_FILE_ANNOTATIONS, VIEW_FILES),
                FileAclEntry.FilePermissions.DELETE_ANNOTATIONS.name(), FILE),

        // JOBS
        EXECUTION(Collections.emptyList()),
        VIEW_JOBS(Collections.emptyList(), JobAclEntry.JobPermissions.VIEW.name(), JOB),
        WRITE_JOBS(Collections.singletonList(VIEW_JOBS), JobAclEntry.JobPermissions.UPDATE.name(), JOB),
        DELETE_JOBS(Arrays.asList(VIEW_JOBS, WRITE_JOBS), JobAclEntry.JobPermissions.DELETE.name(), JOB),

        // SAMPLES
        VIEW_SAMPLES(Collections.emptyList(), SampleAclEntry.SamplePermissions.VIEW.name(), SAMPLE),
        WRITE_SAMPLES(Collections.singletonList(VIEW_SAMPLES), SampleAclEntry.SamplePermissions.UPDATE.name(), SAMPLE),
        DELETE_SAMPLES(Arrays.asList(VIEW_SAMPLES, WRITE_SAMPLES), SampleAclEntry.SamplePermissions.DELETE.name(), SAMPLE),
        VIEW_SAMPLE_ANNOTATIONS(Collections.singletonList(VIEW_SAMPLES), SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(), SAMPLE),
        WRITE_SAMPLE_ANNOTATIONS(Arrays.asList(VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS),
                SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS.name(), SAMPLE),
        DELETE_SAMPLE_ANNOTATIONS(Arrays.asList(VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS, WRITE_SAMPLE_ANNOTATIONS),
                SampleAclEntry.SamplePermissions.DELETE_ANNOTATIONS.name(), SAMPLE),
        VIEW_AGGREGATED_VARIANTS(Collections.emptyList()),
        VIEW_SAMPLE_VARIANTS(Arrays.asList(VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS, VIEW_AGGREGATED_VARIANTS),
                SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name(), SAMPLE),

        // INDIVIDUALS
        VIEW_INDIVIDUALS(Collections.emptyList(), IndividualAclEntry.IndividualPermissions.VIEW.name(), INDIVIDUAL),
        WRITE_INDIVIDUALS(Collections.singletonList(VIEW_INDIVIDUALS), IndividualAclEntry.IndividualPermissions.UPDATE.name(), INDIVIDUAL),
        DELETE_INDIVIDUALS(Arrays.asList(VIEW_INDIVIDUALS, WRITE_INDIVIDUALS), IndividualAclEntry.IndividualPermissions.DELETE.name(),
                INDIVIDUAL),
        VIEW_INDIVIDUAL_ANNOTATIONS(Collections.singletonList(VIEW_INDIVIDUALS),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), INDIVIDUAL),
        WRITE_INDIVIDUAL_ANNOTATIONS(Arrays.asList(VIEW_INDIVIDUALS, VIEW_INDIVIDUAL_ANNOTATIONS),
                IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS.name(), INDIVIDUAL),
        DELETE_INDIVIDUAL_ANNOTATIONS(Arrays.asList(VIEW_INDIVIDUALS, VIEW_INDIVIDUAL_ANNOTATIONS, WRITE_INDIVIDUAL_ANNOTATIONS),
                IndividualAclEntry.IndividualPermissions.DELETE_ANNOTATIONS.name(), INDIVIDUAL),

        // FAMILIES
        VIEW_FAMILIES(Collections.emptyList(), FamilyAclEntry.FamilyPermissions.VIEW.name(), FAMILY),
        WRITE_FAMILIES(Collections.singletonList(VIEW_FAMILIES), FamilyAclEntry.FamilyPermissions.UPDATE.name(), FAMILY),
        DELETE_FAMILIES(Arrays.asList(VIEW_FAMILIES, WRITE_FAMILIES), FamilyAclEntry.FamilyPermissions.DELETE.name(), FAMILY),
        VIEW_FAMILY_ANNOTATIONS(Collections.singletonList(VIEW_FAMILIES), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name(), FAMILY),
        WRITE_FAMILY_ANNOTATIONS(Arrays.asList(VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS),
                FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS.name(), FAMILY),
        DELETE_FAMILY_ANNOTATIONS(Arrays.asList(VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS, WRITE_FAMILY_ANNOTATIONS),
                FamilyAclEntry.FamilyPermissions.DELETE_ANNOTATIONS.name(), FAMILY),

        // COHORTS
        VIEW_COHORTS(Collections.emptyList(), CohortAclEntry.CohortPermissions.VIEW.name(), COHORT),
        WRITE_COHORTS(Collections.singletonList(VIEW_COHORTS), CohortAclEntry.CohortPermissions.UPDATE.name(), COHORT),
        DELETE_COHORTS(Arrays.asList(VIEW_COHORTS, WRITE_COHORTS), CohortAclEntry.CohortPermissions.DELETE.name(), COHORT),
        VIEW_COHORT_ANNOTATIONS(Collections.singletonList(VIEW_COHORTS), CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name(), COHORT),
        WRITE_COHORT_ANNOTATIONS(Arrays.asList(VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS),
                CohortAclEntry.CohortPermissions.WRITE_ANNOTATIONS.name(), COHORT),
        DELETE_COHORT_ANNOTATIONS(Arrays.asList(VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS, WRITE_COHORT_ANNOTATIONS),
                CohortAclEntry.CohortPermissions.DELETE_ANNOTATIONS.name(), COHORT),

        // DISEASE PANELS
        VIEW_PANELS(Collections.emptyList(), PanelAclEntry.PanelPermissions.VIEW.name(), DISEASE_PANEL),
        WRITE_PANELS(Collections.singletonList(VIEW_PANELS), PanelAclEntry.PanelPermissions.UPDATE.name(), DISEASE_PANEL),
        DELETE_PANELS(Arrays.asList(VIEW_PANELS, WRITE_PANELS), PanelAclEntry.PanelPermissions.DELETE.name(), DISEASE_PANEL),

        // CLINICAL ANALYSIS
        VIEW_CLINICAL_ANALYSIS(Collections.emptyList(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name(),
                CLINICAL_ANALYSIS),
        WRITE_CLINICAL_ANALYSIS(Collections.singletonList(VIEW_CLINICAL_ANALYSIS),
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE.name(), CLINICAL_ANALYSIS),
        DELETE_CLINICAL_ANALYSIS(Arrays.asList(VIEW_CLINICAL_ANALYSIS, WRITE_CLINICAL_ANALYSIS),
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE.name(), CLINICAL_ANALYSIS);

        private static Map<String, StudyPermissions> map;
        static {
            map = new LinkedMap();
            for (StudyPermissions params : StudyPermissions.values()) {
                map.put(params.permission + "-" + params.type, params);
            }
        }

        private List<StudyPermissions> implicitPermissions;
        private String permission;
        private int type;

        StudyPermissions(List<StudyPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        StudyPermissions(List<StudyPermissions> implicitPermissions, String permission, int type) {
            this.implicitPermissions = implicitPermissions;
            this.permission = permission;
            this.type = type;
        }

        public static StudyPermissions getStudyPermission(String permission, int type) {
            return map.get(permission + "-" + type);
        }

        public List<StudyPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<StudyPermissions> getDependentPermissions() {
            List<StudyPermissions> dependentPermissions = new LinkedList<>();
            for (StudyPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }
}
