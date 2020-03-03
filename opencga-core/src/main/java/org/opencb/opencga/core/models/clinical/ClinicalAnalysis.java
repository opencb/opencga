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

package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;

import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis extends PrivateStudyUid {

    private String id;
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

    private ClinicalAnalysisAnalyst analyst;
    private Enums.Priority priority;
    private List<String> flags;

    private String creationDate;
    private String modificationDate;
    private String dueDate;
    private ClinicalAnalysisInternal internal;
    private int release;

    private List<Comment> comments;
    private List<Alert> alerts;
    private Map<String, Object> attributes;

    public enum Type {
        SINGLE, FAMILY, CANCER, COHORT, AUTOCOMPARATIVE
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

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(String id, String description, Type type, Disorder disorder, Map<String, List<File>> files, Individual proband,
                            Family family, Map<String, FamiliarRelationship> roleToProband, ClinicalConsent consent,
                            List<Interpretation> interpretations, Enums.Priority priority, ClinicalAnalysisAnalyst analyst,
                            List<String> flags, String creationDate, String dueDate, List<Comment> comments, List<Alert> alerts,
                            ClinicalAnalysisInternal internal, int release, Map<String, Object> attributes) {
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
        this.internal = internal;
        this.release = release;
        this.alerts = alerts;
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
        sb.append(", consent=").append(consent);
        sb.append(", analyst=").append(analyst);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", internal=").append(internal);
        sb.append(", release=").append(release);
        sb.append(", comments=").append(comments);
        sb.append(", alerts=").append(alerts);
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

    public ClinicalAnalysisAnalyst getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysis setAnalyst(ClinicalAnalysisAnalyst analyst) {
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

    public Enums.Priority getPriority() {
        return priority;
    }

    public ClinicalAnalysis setPriority(Enums.Priority priority) {
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

    public List<Alert> getAlerts() {
        return alerts;
    }

    public ClinicalAnalysis setAlerts(List<Alert> alerts) {
        this.alerts = alerts;
        return this;
    }

    public ClinicalAnalysisInternal getInternal() {
        return internal;
    }

    public ClinicalAnalysis setInternal(ClinicalAnalysisInternal internal) {
        this.internal = internal;
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
