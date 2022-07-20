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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class InterpretationCreateParams {

    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_CLINICAL_ANALYSIS_ID_DESCRIPTION)
    private String clinicalAnalysisId;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_ANALYST_DESCRIPTION)
    private ClinicalAnalystParam analyst;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_METHOD_DESCRIPTION)
    private InterpretationMethod method;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_PRIMARY_FINDINGS_DESCRIPTION)
    private List<ClinicalVariant> primaryFindings;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_SECONDARY_FINDINGS_DESCRIPTION)
    private List<ClinicalVariant> secondaryFindings;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_PANELS_DESCRIPTION)
    private List<PanelReferenceParam> panels;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_COMMENTS_DESCRIPTION)
    private List<ClinicalCommentParam> comments;
    @DataField(description = ParamConstants.INTERPRETATION_CREATE_PARAMS_LOCKED_DESCRIPTION)
    private Boolean locked;
    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private StatusParam status;
    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public InterpretationCreateParams() {
    }

    public InterpretationCreateParams(String description, String clinicalAnalysisId, String creationDate, String modificationDate,
                                      ClinicalAnalystParam analyst, InterpretationMethod method, List<ClinicalVariant> primaryFindings,
                                      List<ClinicalVariant> secondaryFindings, List<PanelReferenceParam> panels,
                                      List<ClinicalCommentParam> comments, StatusParam status, Boolean locked,
                                      Map<String, Object> attributes) {
        this.description = description;
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.analyst = analyst;
        this.method = method;
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
        this.panels = panels;
        this.comments = comments;
        this.status = status;
        this.locked = locked;
        this.attributes = attributes;
    }

    public static InterpretationCreateParams of(Interpretation interpretation) {
        return new InterpretationCreateParams(interpretation.getDescription(),
                interpretation.getClinicalAnalysisId(), interpretation.getCreationDate(), interpretation.getModificationDate(),
                ClinicalAnalystParam.of(interpretation.getAnalyst()), interpretation.getMethod(), interpretation.getPrimaryFindings(),
                interpretation.getSecondaryFindings(),
                interpretation.getPanels() != null
                        ? interpretation.getPanels().stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())
                        : null,
                interpretation.getComments() != null
                        ? interpretation.getComments().stream().map(ClinicalCommentParam::of).collect(Collectors.toList())
                        : null,
                StatusParam.of(interpretation.getStatus()),
                interpretation.isLocked(),
                interpretation.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationCreateParams{");
        sb.append("description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", analyst=").append(analyst);
        sb.append(", method=").append(method);
        sb.append(", primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", panels=").append(panels);
        sb.append(", comments=").append(comments);
        sb.append(", status=").append(status);
        sb.append(", locked=").append(locked);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Interpretation toClinicalInterpretation() {
        return new Interpretation(null, description, clinicalAnalysisId, analyst != null ? analyst.toClinicalAnalyst() : null, method,
                creationDate, modificationDate,  locked != null ? locked : false, primaryFindings, secondaryFindings,
                panels != null ? panels.stream().map(p -> new Panel().setId(p.getId())).collect(Collectors.toList()) : null,
                comments != null ? comments.stream().map(ClinicalCommentParam::toClinicalComment).collect(Collectors.toList()) : null,
                status != null ? status.toStatus() : null, attributes);
    }

    public ObjectMap toInterpretationObjectMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this.toClinicalInterpretation()));
    }

    public String getDescription() {
        return description;
    }

    public InterpretationCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public InterpretationCreateParams setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public InterpretationCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public InterpretationCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public InterpretationCreateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public InterpretationMethod getMethod() {
        return method;
    }

    public InterpretationCreateParams setMethod(InterpretationMethod method) {
        this.method = method;
        return this;
    }

    public List<ClinicalVariant> getPrimaryFindings() {
        return primaryFindings;
    }

    public InterpretationCreateParams setPrimaryFindings(List<ClinicalVariant> primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public List<ClinicalVariant> getSecondaryFindings() {
        return secondaryFindings;
    }

    public InterpretationCreateParams setSecondaryFindings(List<ClinicalVariant> secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }

    public List<PanelReferenceParam> getPanels() {
        return panels;
    }

    public InterpretationCreateParams setPanels(List<PanelReferenceParam> panels) {
        this.panels = panels;
        return this;
    }

    public List<ClinicalCommentParam> getComments() {
        return comments;
    }

    public InterpretationCreateParams setComments(List<ClinicalCommentParam> comments) {
        this.comments = comments;
        return this;
    }

    public StatusParam getStatus() {
        return status;
    }

    public InterpretationCreateParams setStatus(StatusParam status) {
        this.status = status;
        return this;
    }

    public Boolean getLocked() {
        return locked;
    }

    public InterpretationCreateParams setLocked(Boolean locked) {
        this.locked = locked;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public InterpretationCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
