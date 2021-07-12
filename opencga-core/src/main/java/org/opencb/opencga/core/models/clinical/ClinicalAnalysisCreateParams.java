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
import org.opencb.opencga.core.models.common.EntryParam;
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

public class ClinicalAnalysisCreateParams {

    private String id;
    private String description;
    private ClinicalAnalysis.Type type;

    private DisorderReferenceParam disorder;

    private List<FileReferenceParam> files;

    private ProbandParam proband;
    private FamilyParam family;

    private List<PanelReferenceParam> panels;
    private Boolean panelLocked;

    private ClinicalAnalystParam analyst;
    private EntryParam interpretation;
    private ClinicalAnalysisQualityControlUpdateParam qualityControl;

    private ClinicalConsentAnnotationParam consent;

    private String dueDate;
    private List<ClinicalCommentParam> comments;
    private PriorityParam priority;
    private List<FlagValueParam> flags;

    private Map<String, Object> attributes;
    private StatusParam status;

    public ClinicalAnalysisCreateParams() {
    }

    public ClinicalAnalysisCreateParams(String id, String description, ClinicalAnalysis.Type type, DisorderReferenceParam disorder,
                                        List<FileReferenceParam> files, ProbandParam proband, FamilyParam family,
                                        List<PanelReferenceParam> panels, Boolean panelLocked, ClinicalAnalystParam analyst,
                                        EntryParam interpretation, ClinicalConsentAnnotationParam consent, String dueDate,
                                        List<ClinicalCommentParam> comments, ClinicalAnalysisQualityControlUpdateParam qualityControl,
                                        PriorityParam priority, List<FlagValueParam> flags, Map<String, Object> attributes,
                                        StatusParam status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.panels = panels;
        this.panelLocked = panelLocked;
        this.analyst = analyst;
        this.interpretation = interpretation;
        this.consent = consent;
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
                clinicalAnalysis.isPanelLocked(),
                clinicalAnalysis.getAnalyst() != null ? ClinicalAnalystParam.of(clinicalAnalysis.getAnalyst()) : null,
                clinicalAnalysis.getInterpretation() != null
                        ? new EntryParam(clinicalAnalysis.getInterpretation().getId())
                        : null,
                ClinicalConsentAnnotationParam.of(clinicalAnalysis.getConsent()), clinicalAnalysis.getDueDate(),
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
        sb.append(", panelLocked=").append(panelLocked);
        sb.append(", analyst=").append(analyst);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", consent=").append(consent);
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

        Interpretation primaryInterpretation = interpretation != null ? new Interpretation().setId(interpretation.getId()) : null;

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
                diseasePanelList, panelLocked != null ? panelLocked : false, false, primaryInterpretation, new LinkedList<>(),
                consent != null ? consent.toClinicalConsentAnnotation() : null,
                new ClinicalAnalyst(assignee, assignee, "", "", TimeUtils.getTime()),
                priority != null ? priority.toClinicalPriorityAnnotation() : null,
                flags != null ? flags.stream().map(FlagValueParam::toFlagAnnotation).collect(Collectors.toList()) : null , null, null,
                dueDate, 1,
                comments != null ? comments.stream().map(ClinicalCommentParam::toClinicalComment).collect(Collectors.toList()) : null,
                qualityControl != null ? qualityControl.toClinicalQualityControl() : null, new LinkedList<>(), null, attributes,
                status != null ? status.toCustomStatus() : null);
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

    public Boolean getPanelLocked() {
        return panelLocked;
    }

    public ClinicalAnalysisCreateParams setPanelLocked(Boolean panelLocked) {
        this.panelLocked = panelLocked;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisCreateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public EntryParam getInterpretation() {
        return interpretation;
    }

    public ClinicalAnalysisCreateParams setInterpretation(EntryParam interpretation) {
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
