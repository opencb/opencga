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
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentAnnotationParam;

import java.util.*;
import java.util.stream.Collectors;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisCreateParams {

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_TYPE_DESCRIPTION)
    private ClinicalAnalysis.Type type;

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_DISORDER_DESCRIPTION)
    private DisorderReferenceParam disorder;

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_FILES_DESCRIPTION)
    private List<FileReferenceParam> files;

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_PROBAND_DESCRIPTION)
    private ProbandParam proband;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_FAMILY_DESCRIPTION)
    private FamilyParam family;

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_PANELS_DESCRIPTION)
    private List<PanelReferenceParam> panels;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_PANEL_LOCK_DESCRIPTION)
    private Boolean panelLock;

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_ANALYST_DESCRIPTION)
    private ClinicalAnalystParam analyst;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_REPORT_DESCRIPTION)
    private ClinicalReport report;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_INTERPRETATION_DESCRIPTION)
    private InterpretationCreateParams interpretation;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_QUALITY_CONTROL_DESCRIPTION)
    private ClinicalAnalysisQualityControlUpdateParam qualityControl;

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_CONSENT_DESCRIPTION)
    private ClinicalConsentAnnotationParam consent;

    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_DUE_DATE_DESCRIPTION)
    private String dueDate;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_COMMENTS_DESCRIPTION)
    private List<ClinicalCommentParam> comments;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_PRIORITY_DESCRIPTION)
    private PriorityParam priority;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_CREATE_PARAMS_FLAGS_DESCRIPTION)
    private List<FlagValueParam> flags;

    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;
    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private StatusParam status;

    public ClinicalAnalysisCreateParams() {
    }

    public ClinicalAnalysisCreateParams(String id, String description, ClinicalAnalysis.Type type, DisorderReferenceParam disorder,
                                        List<FileReferenceParam> files, ProbandParam proband, FamilyParam family,
                                        List<PanelReferenceParam> panels, Boolean panelLock, ClinicalAnalystParam analyst,
                                        ClinicalReport report, InterpretationCreateParams interpretation,
                                        ClinicalConsentAnnotationParam consent, String creationDate, String modificationDate,
                                        String dueDate, List<ClinicalCommentParam> comments,
                                        ClinicalAnalysisQualityControlUpdateParam qualityControl, PriorityParam priority,
                                        List<FlagValueParam> flags, Map<String, Object> attributes, StatusParam status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.panels = panels;
        this.panelLock = panelLock;
        this.report = report;
        this.analyst = analyst;
        this.interpretation = interpretation;
        this.consent = consent;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.dueDate = dueDate;
        this.comments = comments;
        this.qualityControl = qualityControl;
        this.priority = priority;
        this.flags = flags;
        this.attributes = attributes;
        this.status = status;
    }

    public static ClinicalAnalysisCreateParams of(ClinicalAnalysis clinicalAnalysis) {
        return new ClinicalAnalysisCreateParams(clinicalAnalysis.getId(), clinicalAnalysis.getDescription(),
                clinicalAnalysis.getType(), DisorderReferenceParam.of(clinicalAnalysis.getDisorder()),
                clinicalAnalysis.getFiles() != null
                        ? clinicalAnalysis.getFiles().stream().map(FileReferenceParam::of).collect(Collectors.toList())
                        : null,
                clinicalAnalysis.getProband() != null ? ProbandParam.of(clinicalAnalysis.getProband()) : null,
                clinicalAnalysis.getFamily() != null ? FamilyParam.of(clinicalAnalysis.getFamily()) : null,
                clinicalAnalysis.getPanels() != null
                        ? clinicalAnalysis.getPanels().stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())
                        : null,
                clinicalAnalysis.isPanelLock(),
                clinicalAnalysis.getAnalyst() != null ? ClinicalAnalystParam.of(clinicalAnalysis.getAnalyst()) : null,
                clinicalAnalysis.getReport(),
                clinicalAnalysis.getInterpretation() != null
                        ? InterpretationCreateParams.of(clinicalAnalysis.getInterpretation())
                        : null,
                ClinicalConsentAnnotationParam.of(clinicalAnalysis.getConsent()), clinicalAnalysis.getCreationDate(),
                clinicalAnalysis.getModificationDate(), clinicalAnalysis.getDueDate(),
                clinicalAnalysis.getComments() != null
                        ? clinicalAnalysis.getComments().stream().map(ClinicalCommentParam::of).collect(Collectors.toList())
                        : null,
                clinicalAnalysis.getQualityControl() != null
                        ? ClinicalAnalysisQualityControlUpdateParam.of(clinicalAnalysis.getQualityControl())
                        : null,
                PriorityParam.of(clinicalAnalysis.getPriority()),
                clinicalAnalysis.getFlags() != null
                        ? clinicalAnalysis.getFlags().stream().map(FlagValueParam::of).collect(Collectors.toList())
                        : null,
                clinicalAnalysis.getAttributes(), StatusParam.of(clinicalAnalysis.getStatus()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", panels=").append(panels);
        sb.append(", panelLock=").append(panelLock);
        sb.append(", analyst=").append(analyst);
        sb.append(", report=").append(report);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", consent=").append(consent);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public ClinicalAnalysis toClinicalAnalysis() {

        Individual individual = null;
        if (proband != null) {
            individual = new Individual().setId(proband.getId());
            if (proband.getSamples() != null) {
                List<Sample> sampleList = proband.getSamples().stream()
                        .map(sample -> new Sample().setId(sample.getId()))
                        .collect(Collectors.toList());
                individual.setSamples(sampleList);
            }
        }

        Family f = null;
        if (family != null) {
            f = new Family().setId(family.getId());
            if (family.getMembers() != null) {
                List<Individual> members = new ArrayList<>(family.getMembers().size());
                for (ProbandParam member : family.getMembers()) {
                    Individual auxIndividual = new Individual().setId(member.getId());
                    if (member.getSamples() != null) {
                        List<Sample> samples = member.getSamples().stream().map(s -> new Sample().setId(s.getId())).collect(Collectors.toList());
                        auxIndividual.setSamples(samples);
                    }
                    members.add(auxIndividual);
                }
                f.setMembers(members);
            }
        }

        Interpretation primaryInterpretation = interpretation != null ? interpretation.toClinicalInterpretation() : null;

        String assignee = analyst != null ? analyst.getId() : "";

        List<File> caFiles = new LinkedList<>();
        if (files != null) {
            for (FileReferenceParam file : files) {
                caFiles.add(file.toFile());
            }
        }

        List<Panel> diseasePanelList = panels != null ? new ArrayList<>(panels.size()) : Collections.emptyList();
        if (panels != null) {
            for (PanelReferenceParam panel : panels) {
                diseasePanelList.add(new org.opencb.opencga.core.models.panel.Panel().setId(panel.getId()));
            }
        }

        return new ClinicalAnalysis(id, description, type, disorder != null ? disorder.toDisorder() : null, caFiles, individual, f,
                diseasePanelList, panelLock != null ? panelLock : false, false, primaryInterpretation, new LinkedList<>(),
                consent != null ? consent.toClinicalConsentAnnotation() : null,
                new ClinicalAnalyst(assignee, assignee, "", "", TimeUtils.getTime()), report,
                priority != null ? priority.toClinicalPriorityAnnotation() : null,
                flags != null ? flags.stream().map(FlagValueParam::toFlagAnnotation).collect(Collectors.toList()) : null, creationDate, modificationDate, dueDate,
                1,
                comments != null ? comments.stream().map(ClinicalCommentParam::toClinicalComment).collect(Collectors.toList()) : null, qualityControl != null ? qualityControl.toClinicalQualityControl() : null, new LinkedList<>(), null,
                attributes, status != null ? status.toStatus() : null);
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysisCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysisCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalysis.Type getType() {
        return type;
    }

    public ClinicalAnalysisCreateParams setType(ClinicalAnalysis.Type type) {
        this.type = type;
        return this;
    }

    public DisorderReferenceParam getDisorder() {
        return disorder;
    }

    public ClinicalAnalysisCreateParams setDisorder(DisorderReferenceParam disorder) {
        this.disorder = disorder;
        return this;
    }

    public List<FileReferenceParam> getFiles() {
        return files;
    }

    public ClinicalAnalysisCreateParams setFiles(List<FileReferenceParam> files) {
        this.files = files;
        return this;
    }

    public ProbandParam getProband() {
        return proband;
    }

    public ClinicalAnalysisCreateParams setProband(ProbandParam proband) {
        this.proband = proband;
        return this;
    }

    public FamilyParam getFamily() {
        return family;
    }

    public ClinicalAnalysisCreateParams setFamily(FamilyParam family) {
        this.family = family;
        return this;
    }

    public List<PanelReferenceParam> getPanels() {
        return panels;
    }

    public ClinicalAnalysisCreateParams setPanels(List<PanelReferenceParam> panels) {
        this.panels = panels;
        return this;
    }

    public Boolean getPanelLock() {
        return panelLock;
    }

    public ClinicalAnalysisCreateParams setPanelLock(Boolean panelLock) {
        this.panelLock = panelLock;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisCreateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public ClinicalReport getReport() {
        return report;
    }

    public ClinicalAnalysisCreateParams setReport(ClinicalReport report) {
        this.report = report;
        return this;
    }

    public InterpretationCreateParams getInterpretation() {
        return interpretation;
    }

    public ClinicalAnalysisCreateParams setInterpretation(InterpretationCreateParams interpretation) {
        this.interpretation = interpretation;
        return this;
    }

    public ClinicalAnalysisQualityControlUpdateParam getQualityControl() {
        return qualityControl;
    }

    public ClinicalAnalysisCreateParams setQualityControl(ClinicalAnalysisQualityControlUpdateParam qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public ClinicalConsentAnnotationParam getConsent() {
        return consent;
    }

    public ClinicalAnalysisCreateParams setConsent(ClinicalConsentAnnotationParam consent) {
        this.consent = consent;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ClinicalAnalysisCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ClinicalAnalysisCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysisCreateParams setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public List<ClinicalCommentParam> getComments() {
        return comments;
    }

    public ClinicalAnalysisCreateParams setComments(List<ClinicalCommentParam> comments) {
        this.comments = comments;
        return this;
    }

    public PriorityParam getPriority() {
        return priority;
    }

    public ClinicalAnalysisCreateParams setPriority(PriorityParam priority) {
        this.priority = priority;
        return this;
    }

    public List<FlagValueParam> getFlags() {
        return flags;
    }

    public ClinicalAnalysisCreateParams setFlags(List<FlagValueParam> flags) {
        this.flags = flags;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysisCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public StatusParam getStatus() {
        return status;
    }

    public ClinicalAnalysisCreateParams setStatus(StatusParam status) {
        this.status = status;
        return this;
    }

}
