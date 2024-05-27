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

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.opencga.core.models.common.AnnotationSet;
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
    private Boolean panelLock;

    @Deprecated
    private ClinicalAnalystParam analyst;
    private List<ClinicalAnalystParam> analysts;
    private ClinicalReport report;
    private ClinicalRequest request;
    private ClinicalResponsible responsible;
    private InterpretationCreateParams interpretation;
    private ClinicalAnalysisQualityControlUpdateParam qualityControl;

    private ClinicalConsentAnnotationParam consent;

    private String creationDate;
    private String modificationDate;
    private String dueDate;
    private List<ClinicalCommentParam> comments;
    private PriorityParam priority;
    private List<FlagValueParam> flags;

    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;
    private StatusParam status;

    public ClinicalAnalysisCreateParams() {
    }

    public ClinicalAnalysisCreateParams(String id, String description, ClinicalAnalysis.Type type, DisorderReferenceParam disorder,
                                        List<FileReferenceParam> files, ProbandParam proband, FamilyParam family,
                                        List<PanelReferenceParam> panels, Boolean panelLock, List<ClinicalAnalystParam> analysts,
                                        ClinicalReport report, ClinicalRequest request, ClinicalResponsible responsible,
                                        InterpretationCreateParams interpretation, ClinicalConsentAnnotationParam consent,
                                        String creationDate, String modificationDate, String dueDate, List<ClinicalCommentParam> comments,
                                        ClinicalAnalysisQualityControlUpdateParam qualityControl, PriorityParam priority,
                                        List<FlagValueParam> flags, List<AnnotationSet> annotationSets, Map<String, Object> attributes,
                                        StatusParam status) {
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
        this.request = request;
        this.responsible = responsible;
        this.analysts = analysts;
        this.interpretation = interpretation;
        this.consent = consent;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.dueDate = dueDate;
        this.comments = comments;
        this.qualityControl = qualityControl;
        this.priority = priority;
        this.flags = flags;
        this.annotationSets = annotationSets;
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
                clinicalAnalysis.getAnalysts() != null
                        ? clinicalAnalysis.getAnalysts().stream().map(ClinicalAnalystParam::of).collect(Collectors.toList())
                        : null,
                clinicalAnalysis.getReport(), clinicalAnalysis.getRequest(), clinicalAnalysis.getResponsible(),
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
                clinicalAnalysis.getAnnotationSets(),
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
        sb.append(", analysts=").append(analysts);
        sb.append(", report=").append(report);
        sb.append(", request=").append(request);
        sb.append(", responsible=").append(responsible);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", consent=").append(consent);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", annotationSets=").append(annotationSets);
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

        List<ClinicalAnalyst> clinicalAnalystList = null;
        if (analysts != null) {
            clinicalAnalystList = new ArrayList<>(analysts.size());
            for (ClinicalAnalystParam analyst : analysts) {
                clinicalAnalystList.add(new ClinicalAnalyst(analyst.getId(), analyst.getId(), "", "", Collections.emptyMap()));
            }
        }

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
                clinicalAnalystList, report, request, responsible, priority != null ? priority.toClinicalPriorityAnnotation() : null,
                flags != null ? flags.stream().map(FlagValueParam::toFlagAnnotation).collect(Collectors.toList()) : null, creationDate, modificationDate, dueDate,
                1, 1,
                comments != null ? comments.stream().map(ClinicalCommentParam::toClinicalComment).collect(Collectors.toList()) : null,
                qualityControl != null ? qualityControl.toClinicalQualityControl() : null, new LinkedList<>(), null,
                annotationSets, attributes, status != null ? status.toStatus() : null);
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

    @Deprecated
    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    @Deprecated
    public ClinicalAnalysisCreateParams setAnalyst(ClinicalAnalystParam analyst) {
        if (analyst != null && CollectionUtils.isEmpty(this.analysts)) {
            this.analysts = Collections.singletonList(analyst);
        }
        return this;
    }

    public List<ClinicalAnalystParam> getAnalysts() {
        return analysts;
    }

    public ClinicalAnalysisCreateParams setAnalysts(List<ClinicalAnalystParam> analysts) {
        this.analysts = analysts;
        return this;
    }

    public ClinicalReport getReport() {
        return report;
    }

    public ClinicalAnalysisCreateParams setReport(ClinicalReport report) {
        this.report = report;
        return this;
    }

    public ClinicalRequest getRequest() {
        return request;
    }

    public ClinicalAnalysisCreateParams setRequest(ClinicalRequest request) {
        this.request = request;
        return this;
    }

    public ClinicalResponsible getResponsible() {
        return responsible;
    }

    public ClinicalAnalysisCreateParams setResponsible(ClinicalResponsible responsible) {
        this.responsible = responsible;
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

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public ClinicalAnalysisCreateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
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
