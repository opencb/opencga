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
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentAnnotationParam;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ClinicalAnalysisUpdateParams {

    private String id;
    private String description;
    private DisorderReferenceParam disorder;

    private List<FileReferenceParam> files;

    private List<String> panels;

    private Boolean locked;
//    private ProbandParam proband;
//    private FamilyParam family
    private ClinicalAnalystParam analyst;
    private ClinicalAnalysisQualityControlUpdateParam qualityControl;

    private ClinicalConsentAnnotationParam consent;

    private String dueDate;
    private List<ClinicalCommentParam> comments;
    private PriorityParam priority; // id
    private List<FlagValueParam> flags; // id

    private Map<String, Object> attributes;
    private StatusParam status;

    public ClinicalAnalysisUpdateParams() {
    }

    public ClinicalAnalysisUpdateParams(String id, String description, DisorderReferenceParam disorder, List<FileReferenceParam> files,
                                        List<String> panels, Boolean locked, ClinicalAnalystParam analyst,
                                        ClinicalConsentAnnotationParam consent, String dueDate,
                                        ClinicalAnalysisQualityControlUpdateParam qualityControl, List<ClinicalCommentParam> comments,
                                        PriorityParam priority, List<FlagValueParam> flags, Map<String, Object> attributes,
                                        StatusParam status) {
        this.id = id;
        this.description = description;
        this.disorder = disorder;
        this.files = files;
        this.panels = panels;
        this.locked = locked;
        this.analyst = analyst;
        this.consent = consent;
        this.dueDate = dueDate;
        this.comments = comments;
        this.priority = priority;
        this.flags = flags;
        this.qualityControl = qualityControl;
        this.attributes = attributes;
        this.status = status;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", panels=").append(panels);
        sb.append(", locked=").append(locked);
        sb.append(", analyst=").append(analyst);
        sb.append(", consent=").append(consent);
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public static class SampleParams {
        private String id;

        public SampleParams() {
        }

        public String getId() {
            return id;
        }

        public SampleParams setId(String id) {
            this.id = id;
            return this;
        }
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

    public List<String> getPanels() {
        return panels;
    }

    public ClinicalAnalysisUpdateParams setPanels(List<String> panels) {
        this.panels = panels;
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

    public ClinicalConsentAnnotationParam getConsent() {
        return consent;
    }

    public ClinicalAnalysisUpdateParams setConsent(ClinicalConsentAnnotationParam consent) {
        this.consent = consent;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysisUpdateParams setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public ClinicalAnalysisQualityControlUpdateParam getQualityControl() {
        return qualityControl;
    }

    public ClinicalAnalysisUpdateParams setQualityControl(ClinicalAnalysisQualityControlUpdateParam qualityControl) {
        this.qualityControl = qualityControl;
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
