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
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileReferenceParam;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ClinicalUpdateParams {

    private String id;
    private String description;
    private ClinicalAnalysis.Type type;

    private Disorder disorder;

    private List<FileReferenceParam> files;

//    private ProbandParam proband;
//    private FamilyParam family;
    private Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband;
    private ClinicalAnalystParam analyst;
    private ClinicalAnalysisInternal internal;
    private Interpretation interpretation;
    private List<Interpretation> secondaryInterpretations;

    private ClinicalAnalysisQcUpdateParams qualityControl;

    private ClinicalConsent consent;

    private String dueDate;
    private List<Comment> comments;
    private List<Alert> alerts;
    private Enums.Priority priority;
    private List<String> flags;

    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public ClinicalUpdateParams() {
    }

    public ClinicalUpdateParams(String id, String description, ClinicalAnalysis.Type type, Disorder disorder,
                                List<FileReferenceParam> files, Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband,
                                ClinicalAnalystParam analyst, ClinicalAnalysisInternal internal, Interpretation interpretation,
                                List<Interpretation> secondaryInterpretations, ClinicalAnalysisQcUpdateParams qualityControl,
                                ClinicalConsent consent, String dueDate, List<Comment> comments, List<Alert> alerts,
                                Enums.Priority priority, List<String> flags, Map<String, Object> attributes, CustomStatusParams status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.roleToProband = roleToProband;
        this.analyst = analyst;
        this.internal = internal;
        this.interpretation = interpretation;
        this.secondaryInterpretations = secondaryInterpretations;
        this.qualityControl = qualityControl;
        this.consent = consent;
        this.dueDate = dueDate;
        this.comments = comments;
        this.alerts = alerts;
        this.priority = priority;
        this.flags = flags;
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
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", roleToProband=").append(roleToProband);
        sb.append(", analyst=").append(analyst);
        sb.append(", internal=").append(internal);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", secondaryInterpretations=").append(secondaryInterpretations);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", consent=").append(consent);
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", alerts=").append(alerts);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
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

    public static class ClinicalAnalystParam {
        private String assignee;

        public ClinicalAnalystParam(String assignee) {
            this.assignee = assignee;
        }

        public String getAssignee() {
            return assignee;
        }

        public ClinicalAnalystParam setAssignee(String assignee) {
            this.assignee = assignee;
            return this;
        }
    }

    public String getId() {
        return id;
    }

    public ClinicalUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalysis.Type getType() {
        return type;
    }

    public ClinicalUpdateParams setType(ClinicalAnalysis.Type type) {
        this.type = type;
        return this;
    }

    public Disorder getDisorder() {
        return disorder;
    }

    public ClinicalUpdateParams setDisorder(Disorder disorder) {
        this.disorder = disorder;
        return this;
    }

    public List<FileReferenceParam> getFiles() {
        return files;
    }

    public ClinicalUpdateParams setFiles(List<FileReferenceParam> files) {
        this.files = files;
        return this;
    }

    public Map<String, ClinicalAnalysis.FamiliarRelationship> getRoleToProband() {
        return roleToProband;
    }

    public ClinicalUpdateParams setRoleToProband(Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband) {
        this.roleToProband = roleToProband;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public ClinicalUpdateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public Interpretation getInterpretation() {
        return interpretation;
    }

    public ClinicalUpdateParams setInterpretation(Interpretation interpretation) {
        this.interpretation = interpretation;
        return this;
    }

    public List<Interpretation> getSecondaryInterpretations() {
        return secondaryInterpretations;
    }

    public ClinicalUpdateParams setSecondaryInterpretations(List<Interpretation> secondaryInterpretations) {
        this.secondaryInterpretations = secondaryInterpretations;
        return this;
    }

    public ClinicalAnalysisQcUpdateParams getQualityControl() {
        return qualityControl;
    }

    public ClinicalUpdateParams setQualityControl(ClinicalAnalysisQcUpdateParams qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public ClinicalConsent getConsent() {
        return consent;
    }

    public ClinicalUpdateParams setConsent(ClinicalConsent consent) {
        this.consent = consent;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalUpdateParams setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalUpdateParams setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public List<Alert> getAlerts() {
        return alerts;
    }

    public ClinicalUpdateParams setAlerts(List<Alert> alerts) {
        this.alerts = alerts;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public ClinicalUpdateParams setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public List<String> getFlags() {
        return flags;
    }

    public ClinicalUpdateParams setFlags(List<String> flags) {
        this.flags = flags;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public ClinicalAnalysisInternal getInternal() {
        return internal;
    }

    public ClinicalUpdateParams setInternal(ClinicalAnalysisInternal internal) {
        this.internal = internal;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public ClinicalUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }
}
