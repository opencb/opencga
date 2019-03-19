/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.core.models;

import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis extends PrivateStudyUid {

    private String id;
    @Deprecated
    private String name;
    private String uuid;
    private String description;
    private Type type;

    private Disorder disorder;

    // Map of sample id, list of files (VCF, BAM and BIGWIG)
    private Map<String, List<File>> files;

    private Individual proband;
    private Family family;
    private Map<String, FamiliarRelationship> roleToProband;
    private List<Interpretation> interpretations;

    private ClinicalConsent consent;

    private ClinicalAnalyst analyst;
    private Priority priority;
    private List<String> flags;

    private String creationDate;
    private String modificationDate;
    private String dueDate;
    private ClinicalStatus status;
    private int release;

    private List<Comment> comments;
    private Map<String, Object> attributes;

    public enum Priority {
        URGENT, HIGH, MEDIUM, LOW
    }

    public enum Type {
        SINGLE, FAMILY, CANCER, COHORT, AUTOCOMPARATIVE
    }

    // Todo: Think about a better place to have this enum
    @Deprecated
    public enum Action {
        ADD,
        SET,
        REMOVE
    }

    public enum FamiliarRelationship {
        TWINS_MONOZYGOUS("TwinsMonozygous"), TWINS_DIZYGOUS("TwinsDizygous"), TWINS_UNKNOWN("TwinsUnknown"), FULL_SIBLING("FullSibling"),
        FULL_SIBLING_F("FullSiblingF"), FULL_SIBLING_M("FullSiblingM"), MOTHER("Mother"), FATHER("Father"), SON("Son"),
        DAUGHTER("Daughter"), CHILD_OF_UNKNOWN_SEX("ChildOfUnknownSex"), MATERNAL_AUNT("MaternalAunt"), MATERNAL_UNCLE("MaternalUncle"),
        MATERNAL_UNCLE_OR_AUNT("MaternalUncleOrAunt"), PATERNAL_AUNT("PaternalAunt"), PATERNAL_UNCLE("PaternalUncle"),
        PATERNAL_UNCLE_OR_AUNT("PaternalUncleOrAunt"), MATERNAL_GRANDMOTHER("MaternalGrandmother"),
        PATERNAL_GRANDMOTHER("PaternalGrandmother"), MATERNAL_GRANDFATHER("MaternalGrandfather"),
        PATERNAL_GRANDFATHER("PaternalGrandfather"), DOUBLE_FIRST_COUSIN("DoubleFirstCousin"),
        MATERNAL_COUSIN_SISTER("MaternalCousinSister"), PATERNAL_COUSIN_SISTER("PaternalCousinSister"),
        MATERNAL_COUSIN_BROTHER("MaternalCousinBrother"), PATERNAL_COUSIN_BROTHER("PaternalCousinBrother"), COUSIN("Cousin"),
        SPOUSE("Spouse"), HUSBAND("Husband"), OTHER("Other"), RELATION_IS_NOT_CLEAR("RelationIsNotClear"), UNRELATED("Unrelated"),
        PROBAND("Proband"), UNKNOWN("Unknown");

        private final String value;

        FamiliarRelationship(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static class ClinicalAnalyst {
        private String assignedBy;
        private String assignee;
        private String date;

        public ClinicalAnalyst() {
            this("", "");
        }

        public ClinicalAnalyst(String assignee, String assignedBy) {
            this.assignee = assignee;
            this.assignedBy = assignedBy;
            this.date = TimeUtils.getTime();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ClinicalAnalyst{");
            sb.append("assignee='").append(assignee).append('\'');
            sb.append(", assignedBy='").append(assignedBy).append('\'');
            sb.append(", date='").append(date).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getAssignee() {
            return assignee;
        }

        public ClinicalAnalyst setAssignee(String assignee) {
            this.assignee = assignee;
            return this;
        }

        public String getAssignedBy() {
            return assignedBy;
        }

        public ClinicalAnalyst setAssignedBy(String assignedBy) {
            this.assignedBy = assignedBy;
            return this;
        }

        public String getDate() {
            return date;
        }

        public ClinicalAnalyst setDate(String date) {
            this.date = date;
            return this;
        }
    }

    public static class ClinicalStatus extends Status {

        public static final String INCOMPLETE = "INCOMPLETE";
        public static final String READY_FOR_VALIDATION = "READY_FOR_VALIDATION";
        public static final String READY_FOR_INTERPRETATION = "READY_FOR_INTERPRETATION";
        public static final String INTERPRETATION_IN_PROGRESS = "INTERPRETATION_IN_PROGRESS";
//        public static final String INTERPRETED = "INTERPRETED";
        public static final String READY_FOR_INTEPRETATION_REVIEW = "READY_FOR_INTEPRETATION_REVIEW";
        public static final String INTERPRETATION_REVIEW_IN_PROGRESS = "INTERPRETATION_REVIEW_IN_PROGRESS";
        public static final String READY_FOR_REPORT = "READY_FOR_REPORT";
        public static final String REPORT_IN_PROGRESS = "REPORT_IN_PROGRESS";
        public static final String DONE = "DONE";
        public static final String REVIEW_IN_PROGRESS = "REVIEW_IN_PROGRESS";
        public static final String CLOSED = "CLOSED";
        public static final String REJECTED = "REJECTED";

        public static final List<String> STATUS_LIST = Arrays.asList(INCOMPLETE, READY, DELETED, READY_FOR_VALIDATION,
                READY_FOR_INTERPRETATION, INTERPRETATION_IN_PROGRESS, READY_FOR_INTEPRETATION_REVIEW, INTERPRETATION_REVIEW_IN_PROGRESS,
                READY_FOR_REPORT, REPORT_IN_PROGRESS, DONE, REVIEW_IN_PROGRESS, CLOSED, REJECTED);

        public ClinicalStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public ClinicalStatus(String status) {
            this(status, "");
        }

        public ClinicalStatus() {
            this(READY_FOR_INTERPRETATION, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }

            if (STATUS_LIST.contains(status)) {
                return true;
            }
            return false;
        }
    }

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(String id, String description, Type type, Disorder disorder, Map<String, List<File>> files, Individual proband,
                            Family family, Map<String, FamiliarRelationship> roleToProband, ClinicalConsent consent,
                            List<Interpretation> interpretations, Priority priority, ClinicalAnalyst analyst, List<String> flags,
                            String creationDate, String dueDate, List<Comment> comments, ClinicalStatus status, int release,
                            Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.roleToProband = roleToProband;
        this.interpretations = interpretations;
        this.priority = priority;
        this.flags = flags;
        this.analyst = analyst;
        this.consent = consent;
        this.creationDate = creationDate;
        this.dueDate = dueDate;
        this.comments = comments;
        this.status = status;
        this.release = release;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysis{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", roleToProband=").append(roleToProband);
        sb.append(", interpretations=").append(interpretations);
        sb.append(", analyst=").append(analyst);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", consent=").append(consent);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", comments=").append(comments);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public ClinicalAnalysis setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysis setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public ClinicalAnalysis setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public ClinicalAnalysis setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalAnalysis setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysis setDescription(String description) {
        this.description = description;
        return this;
    }

    public Type getType() {
        return type;
    }

    public ClinicalAnalysis setType(Type type) {
        this.type = type;
        return this;
    }

    public Disorder getDisorder() {
        return disorder;
    }

    public ClinicalAnalysis setDisorder(Disorder disorder) {
        this.disorder = disorder;
        return this;
    }

    public Map<String, List<File>> getFiles() {
        return files;
    }

    public ClinicalAnalysis setFiles(Map<String, List<File>> files) {
        this.files = files;
        return this;
    }

    public Individual getProband() {
        return proband;
    }

    public ClinicalAnalysis setProband(Individual proband) {
        this.proband = proband;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public ClinicalAnalysis setFamily(Family family) {
        this.family = family;
        return this;
    }

    public Map<String, FamiliarRelationship> getRoleToProband() {
        return roleToProband;
    }

    public ClinicalAnalysis setRoleToProband(Map<String, FamiliarRelationship> roleToProband) {
        this.roleToProband = roleToProband;
        return this;
    }

    public List<Interpretation> getInterpretations() {
        return interpretations;
    }

    public ClinicalAnalysis setInterpretations(List<Interpretation> interpretations) {
        this.interpretations = interpretations;
        return this;
    }

    public ClinicalAnalyst getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysis setAnalyst(ClinicalAnalyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public ClinicalConsent getConsent() {
        return consent;
    }

    public ClinicalAnalysis setConsent(ClinicalConsent consent) {
        this.consent = consent;
        return this;
    }

    public Priority getPriority() {
        return priority;
    }

    public ClinicalAnalysis setPriority(Priority priority) {
        this.priority = priority;
        return this;
    }

    public List<String> getFlags() {
        return flags;
    }

    public ClinicalAnalysis setFlags(List<String> flags) {
        this.flags = flags;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysis setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ClinicalAnalysis setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ClinicalAnalysis setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalAnalysis setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public ClinicalStatus getStatus() {
        return status;
    }

    public ClinicalAnalysis setStatus(ClinicalStatus status) {
        this.status = status;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ClinicalAnalysis setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysis setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
