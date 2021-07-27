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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentAnnotationParam;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ClinicalAnalysisUpdateParams {

    private String id;
    private String description;
    private ClinicalAnalysis.Type type;

    private DisorderReferenceParam disorder;
    private List<FileReferenceParam> files;

    private List<PanelReferenceParam> panels;
    private Boolean panelLock;

    private ProbandParam proband;
    private FamilyParam family;

    private Boolean locked;
    private ClinicalAnalystParam analyst;
    private ClinicalAnalysisQualityControlUpdateParam qualityControl;

    private ClinicalConsentAnnotationParam consent;

    private String creationDate;
    private String dueDate;
    private List<ClinicalCommentParam> comments;
    private PriorityParam priority; // id
    private List<FlagValueParam> flags; // id

    private Map<String, Object> attributes;
    private StatusParam status;

    public ClinicalAnalysisUpdateParams() {
    }

    public ClinicalAnalysisUpdateParams(String id, String description, ClinicalAnalysis.Type type, DisorderReferenceParam disorder,
                                        List<FileReferenceParam> files, ProbandParam proband, FamilyParam family,
                                        List<PanelReferenceParam> panels, Boolean panelLock, Boolean locked, ClinicalAnalystParam analyst,
                                        ClinicalAnalysisQualityControlUpdateParam qualityControl, ClinicalConsentAnnotationParam consent,
                                        String creationDate, String dueDate, List<ClinicalCommentParam> comments, PriorityParam priority,
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
        this.locked = locked;
        this.analyst = analyst;
        this.qualityControl = qualityControl;
        this.consent = consent;
        this.creationDate = creationDate;
        this.dueDate = dueDate;
        this.comments = comments;
        this.priority = priority;
        this.flags = flags;
        this.attributes = attributes;
        this.status = status;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @JsonIgnore
    public ClinicalAnalysis toClinicalAnalysis() {
        return new ClinicalAnalysis(id, description, type, disorder.toDisorder(),
                files != null ? files.stream().map(FileReferenceParam::toFile).collect(Collectors.toList()) : null,
                proband != null ? proband.toIndividual() : null,
                family != null ? family.toFamily() : null,
                panels != null ? panels.stream().map(p -> new Panel().setId(p.getId())).collect(Collectors.toList()) : null,
                panelLock != null ? panelLock : false,
                locked, null, null,
                consent != null ? consent.toClinicalConsentAnnotation() : null,
                analyst != null ? analyst.toClinicalAnalyst() : null,
                priority != null ? priority.toClinicalPriorityAnnotation() : null,
                flags != null ? flags.stream().map(FlagValueParam::toFlagAnnotation).collect(Collectors.toList()) : null,
                creationDate, TimeUtils.getTime(), dueDate, 1,
                comments != null ? comments.stream().map(ClinicalCommentParam::toClinicalComment).collect(Collectors.toList()) : null,
                qualityControl != null ? qualityControl.toClinicalQualityControl() : null,
                null, null, attributes, status != null ? status.toCustomStatus() : null);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", panels=").append(panels);
        sb.append(", panelLock=").append(panelLock);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", locked=").append(locked);
        sb.append(", analyst=").append(analyst);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", consent=").append(consent);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysisUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysisUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalysis.Type getType() {
        return type;
    }

    public ClinicalAnalysisUpdateParams setType(ClinicalAnalysis.Type type) {
        this.type = type;
        return this;
    }

    public DisorderReferenceParam getDisorder() {
        return disorder;
    }

    public ClinicalAnalysisUpdateParams setDisorder(DisorderReferenceParam disorder) {
        this.disorder = disorder;
        return this;
    }

    public List<FileReferenceParam> getFiles() {
        return files;
    }

    public ClinicalAnalysisUpdateParams setFiles(List<FileReferenceParam> files) {
        this.files = files;
        return this;
    }

    public List<PanelReferenceParam> getPanels() {
        return panels;
    }

    public ProbandParam getProband() {
        return proband;
    }

    public ClinicalAnalysisUpdateParams setProband(ProbandParam proband) {
        this.proband = proband;
        return this;
    }

    public FamilyParam getFamily() {
        return family;
    }

    public ClinicalAnalysisUpdateParams setFamily(FamilyParam family) {
        this.family = family;
        return this;
    }

    public ClinicalAnalysisUpdateParams setPanels(List<PanelReferenceParam> panels) {
        this.panels = panels;
        return this;
    }

    public Boolean getPanelLock() {
        return panelLock;
    }

    public ClinicalAnalysisUpdateParams setPanelLock(Boolean panelLock) {
        this.panelLock = panelLock;
        return this;
    }

    public Boolean getLocked() {
        return locked;
    }

    public ClinicalAnalysisUpdateParams setLocked(Boolean locked) {
        this.locked = locked;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisUpdateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public ClinicalAnalysisQualityControlUpdateParam getQualityControl() {
        return qualityControl;
    }

    public ClinicalAnalysisUpdateParams setQualityControl(ClinicalAnalysisQualityControlUpdateParam qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public ClinicalConsentAnnotationParam getConsent() {
        return consent;
    }

    public ClinicalAnalysisUpdateParams setConsent(ClinicalConsentAnnotationParam consent) {
        this.consent = consent;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ClinicalAnalysisUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysisUpdateParams setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public List<ClinicalCommentParam> getComments() {
        return comments;
    }

    public ClinicalAnalysisUpdateParams setComments(List<ClinicalCommentParam> comments) {
        this.comments = comments;
        return this;
    }

    public PriorityParam getPriority() {
        return priority;
    }

    public ClinicalAnalysisUpdateParams setPriority(PriorityParam priority) {
        this.priority = priority;
        return this;
    }

    public List<FlagValueParam> getFlags() {
        return flags;
    }

    public ClinicalAnalysisUpdateParams setFlags(List<FlagValueParam> flags) {
        this.flags = flags;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysisUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public StatusParam getStatus() {
        return status;
    }

    public ClinicalAnalysisUpdateParams setStatus(StatusParam status) {
        this.status = status;
        return this;
    }
}
