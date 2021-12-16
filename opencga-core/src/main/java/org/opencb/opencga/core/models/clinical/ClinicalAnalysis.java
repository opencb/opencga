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
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentAnnotation;
import org.opencb.opencga.core.models.study.configuration.ClinicalPriorityAnnotation;

import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis extends PrivateStudyUid {

    /**
     * ClinicalAnalysis ID is a mandatory parameter when creating a new ClinicalAnalysis, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.CLINICALANALYSIS_ID_DESCRIPTION)
    private String id;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the ClinicalAnalysis creation and cannot be
     * changed.
     *
     * @apiNote Internal, Unique, Immutable
     */
    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    /**
     * An string to describe the properties of the ClinicalAnalysis.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "type", indexed = true,
            description = FieldConstants.CLINICALANALYSIS_TYPE)
    private Type type;

    @DataField(id = "disorder", indexed = true,
            description = FieldConstants.CLINICALANALYSIS_DISORDER)
    private Disorder disorder;

    // List of files (VCF, BAM and BIGWIG)
    @DataField(id = "files", indexed = true,
            description = FieldConstants.CLINICALANALYSIS_FILES)
    private List<File> files;

    @DataField(id = "proband", indexed = true,
            description = FieldConstants.CLINICALANALYSIS_PROBAND)
    private Individual proband;

    private Family family;

    private List<Panel> panels;
    private boolean panelLock;

    private boolean locked;

    private Interpretation interpretation;
    private List<Interpretation> secondaryInterpretations;

    private ClinicalConsentAnnotation consent;

    private ClinicalAnalyst analyst;
    private ClinicalReport report;
    private ClinicalPriorityAnnotation priority;
    private List<FlagAnnotation> flags;

    /**
     * String representing when the sample was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    private String creationDate;

    /**
     * String representing when was the last time the sample was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    private String modificationDate;

    private String dueDate;

    /**
     * An integer describing the current data release.
     *
     * @apiNote Internal
     */
    private int release;

    private ClinicalAnalysisQualityControl qualityControl;

    private List<ClinicalComment> comments;
    private List<ClinicalAudit> audit;

    /**
     * An object describing the internal information of the ClinicalAnalysis. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */
    private ClinicalAnalysisInternal internal;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    private Map<String, Object> attributes;

    /**
     * An object describing the status of the ClinicalAnalysis.
     *
     * @apiNote
     */
    private Status status;

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(String id, String description, Type type, Disorder disorder, List<File> files, Individual proband,
                            Family family, List<Panel> panels, boolean panelLock, boolean locked, Interpretation interpretation,
                            List<Interpretation> secondaryInterpretations, ClinicalConsentAnnotation consent, ClinicalAnalyst analyst,
                            ClinicalReport report, ClinicalPriorityAnnotation priority, List<FlagAnnotation> flags, String creationDate,
                            String modificationDate, String dueDate, int release, List<ClinicalComment> comments,
                            ClinicalAnalysisQualityControl qualityControl, List<ClinicalAudit> audit, ClinicalAnalysisInternal internal,
                            Map<String, Object> attributes, Status status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.panels = panels;
        this.panelLock = panelLock;
        this.locked = locked;
        this.interpretation = interpretation;
        this.secondaryInterpretations = secondaryInterpretations;
        this.consent = consent;
        this.analyst = analyst;
        this.report = report;
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
        sb.append(", panels=").append(panels);
        sb.append(", panelLock=").append(panelLock);
        sb.append(", locked=").append(locked);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", secondaryInterpretations=").append(secondaryInterpretations);
        sb.append(", consent=").append(consent);
        sb.append(", analyst=").append(analyst);
        sb.append(", report=").append(report);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", release=").append(release);
        sb.append(", qualityControl=").append(qualityControl);
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

    public List<Panel> getPanels() {
        return panels;
    }

    public ClinicalAnalysis setPanels(List<Panel> panels) {
        this.panels = panels;
        return this;
    }

    public boolean isPanelLock() {
        return panelLock;
    }

    public ClinicalAnalysis setPanelLock(boolean panelLock) {
        this.panelLock = panelLock;
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

    public ClinicalReport getReport() {
        return report;
    }

    public ClinicalAnalysis setReport(ClinicalReport report) {
        this.report = report;
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

    public enum Type {
        SINGLE, FAMILY, CANCER, COHORT, AUTOCOMPARATIVE
    }
}
