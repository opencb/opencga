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

package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentAnnotation;
import org.opencb.opencga.core.models.study.configuration.ClinicalPriorityAnnotation;

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

    // List of files (VCF, BAM and BIGWIG)
    private List<File> files;

    private Individual proband;
    private Family family;

    private boolean locked;

    private Interpretation interpretation;
    private List<Interpretation> secondaryInterpretations;

    private ClinicalConsentAnnotation consent;

    private ClinicalAnalyst analyst;
    private ClinicalPriorityAnnotation priority;
    private List<FlagAnnotation> flags;

    private String creationDate;
    private String modificationDate;
    private String dueDate;
    private int release;

    private ClinicalAnalysisQualityControl qualityControl;

    private List<ClinicalComment> comments;
    private List<ClinicalAudit> audit;
    private ClinicalAnalysisInternal internal;
    private Map<String, Object> attributes;

    private Status status;

    public enum Type {
        SINGLE, FAMILY, CANCER, COHORT, AUTOCOMPARATIVE
    }

    public ClinicalAnalysis() {
    }


    public ClinicalAnalysis(String id, String description, Type type, Disorder disorder, List<File> files, Individual proband,
                            Family family, boolean locked, Interpretation interpretation, List<Interpretation> secondaryInterpretations,
                            ClinicalConsentAnnotation consent, ClinicalAnalyst analyst, ClinicalPriorityAnnotation priority,
                            List<FlagAnnotation> flags, String creationDate, String modificationDate, String dueDate, int release,
                            List<ClinicalComment> comments, ClinicalAnalysisQualityControl qualityControl, List<ClinicalAudit> audit,
                            ClinicalAnalysisInternal internal, Map<String, Object> attributes, Status status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.locked = locked;
        this.interpretation = interpretation;
        this.secondaryInterpretations = secondaryInterpretations;
        this.consent = consent;
        this.analyst = analyst;
        this.priority = priority;
        this.flags = flags;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.dueDate = dueDate;
        this.qualityControl = qualityControl;
        this.release = release;
        this.comments = comments;
        this.audit = audit;
        this.internal = internal;
        this.attributes = attributes;
        this.status = status;
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
        sb.append(", locked=").append(locked);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", secondaryInterpretations=").append(secondaryInterpretations);
        sb.append(", consent=").append(consent);
        sb.append(", analyst=").append(analyst);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", qualityControl='").append(qualityControl).append('\'');
        sb.append(", release=").append(release);
        sb.append(", comments=").append(comments);
        sb.append(", audit=").append(audit);
        sb.append(", internal=").append(internal);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public ClinicalAnalysis setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ClinicalAnalysis setId(String id) {
        this.id = id;
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

    public List<File> getFiles() {
        return files;
    }

    public ClinicalAnalysis setFiles(List<File> files) {
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

    public boolean isLocked() {
        return locked;
    }

    public ClinicalAnalysis setLocked(boolean locked) {
        this.locked = locked;
        return this;
    }

    public Interpretation getInterpretation() {
        return interpretation;
    }

    public ClinicalAnalysis setInterpretation(Interpretation interpretation) {
        this.interpretation = interpretation;
        return this;
    }

    public List<Interpretation> getSecondaryInterpretations() {
        return secondaryInterpretations;
    }

    public ClinicalAnalysis setSecondaryInterpretations(List<Interpretation> secondaryInterpretations) {
        this.secondaryInterpretations = secondaryInterpretations;
        return this;
    }

    public ClinicalConsentAnnotation getConsent() {
        return consent;
    }

    public ClinicalAnalysis setConsent(ClinicalConsentAnnotation consent) {
        this.consent = consent;
        return this;
    }

    public ClinicalAnalyst getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysis setAnalyst(ClinicalAnalyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public ClinicalPriorityAnnotation getPriority() {
        return priority;
    }

    public ClinicalAnalysis setPriority(ClinicalPriorityAnnotation priority) {
        this.priority = priority;
        return this;
    }

    public List<FlagAnnotation> getFlags() {
        return flags;
    }

    public ClinicalAnalysis setFlags(List<FlagAnnotation> flags) {
        this.flags = flags;
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

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysis setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ClinicalAnalysis setRelease(int release) {
        this.release = release;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public ClinicalAnalysis setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public List<ClinicalAudit> getAudit() {
        return audit;
    }

    public ClinicalAnalysis setAudit(List<ClinicalAudit> audit) {
        this.audit = audit;
        return this;
    }

    public ClinicalAnalysisQualityControl getQualityControl() {
        return qualityControl;
    }

    public ClinicalAnalysis setQualityControl(ClinicalAnalysisQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public ClinicalAnalysisInternal getInternal() {
        return internal;
    }

    public ClinicalAnalysis setInternal(ClinicalAnalysisInternal internal) {
        this.internal = internal;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysis setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public ClinicalAnalysis setStatus(Status status) {
        this.status = status;
        return this;
    }
}
