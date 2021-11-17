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
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class InterpretationCreateParams {

    private String id;
    private String description;
    private String clinicalAnalysisId;
    private String creationDate;
    private String modificationDate;
    private ClinicalAnalystParam analyst;
    private InterpretationMethod method;
    private List<ClinicalVariant> primaryFindings;
    private List<ClinicalVariant> secondaryFindings;
    private List<PanelReferenceParam> panels;
    private List<ClinicalCommentParam> comments;
    private Map<String, Object> attributes;

    public InterpretationCreateParams() {
    }

    public InterpretationCreateParams(String id, String description, String clinicalAnalysisId, String creationDate,
                                      String modificationDate, ClinicalAnalystParam analyst, InterpretationMethod method,
                                      List<ClinicalVariant> primaryFindings, List<ClinicalVariant> secondaryFindings,
                                      List<PanelReferenceParam> panels, List<ClinicalCommentParam> comments,
                                      Map<String, Object> attributes) {
        this.id = id;
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
        this.attributes = attributes;
    }

    public static InterpretationCreateParams of(Interpretation interpretation) {
        return new InterpretationCreateParams(interpretation.getId(), interpretation.getDescription(),
                interpretation.getClinicalAnalysisId(), interpretation.getCreationDate(), interpretation.getModificationDate(),
                ClinicalAnalystParam.of(interpretation.getAnalyst()), interpretation.getMethod(), interpretation.getPrimaryFindings(),
                interpretation.getSecondaryFindings(),
                interpretation.getPanels() != null
                        ? interpretation.getPanels().stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())
                        : null,
                interpretation.getComments() != null
                        ? interpretation.getComments().stream().map(ClinicalCommentParam::of).collect(Collectors.toList())
                        : null,
                interpretation.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", analyst=").append(analyst);
        sb.append(", method=").append(method);
        sb.append(", primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", panels=").append(panels);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Interpretation toClinicalInterpretation() {
        return new Interpretation(id, description, clinicalAnalysisId, analyst != null ? analyst.toClinicalAnalyst() : null, method,
                creationDate, modificationDate, primaryFindings, secondaryFindings,
                panels != null ? panels.stream().map(p -> new Panel().setId(p.getId())).collect(Collectors.toList()) : null,
                comments != null ? comments.stream().map(ClinicalCommentParam::toClinicalComment).collect(Collectors.toList()) : null,
                attributes);
    }

    public ObjectMap toInterpretationObjectMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this.toClinicalInterpretation()));
    }

    public String getId() {
        return id;
    }

    public InterpretationCreateParams setId(String id) {
        this.id = id;
        return this;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public InterpretationCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
