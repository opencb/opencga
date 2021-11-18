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
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class InterpretationUpdateParams {

    private String description;
    private ClinicalAnalystParam analyst;
    private InterpretationMethod method;
    private String creationDate;
    private String modificationDate;
    private List<ClinicalVariant> primaryFindings;
    private List<ClinicalVariant> secondaryFindings;
    private List<PanelReferenceParam> panels;
    private List<ClinicalCommentParam> comments;
    private Map<String, Object> attributes;
    private StatusParam status;

    public InterpretationUpdateParams() {
    }

    public InterpretationUpdateParams(String description, ClinicalAnalystParam analyst, InterpretationMethod method,
                                      String creationDate, String modificationDate, List<ClinicalVariant> primaryFindings,
                                      List<ClinicalVariant> secondaryFindings, List<PanelReferenceParam> panels,
                                      List<ClinicalCommentParam> comments, Map<String, Object> attributes, StatusParam status) {
        this.description = description;
        this.analyst = analyst;
        this.method = method;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
        this.panels = panels;
        this.comments = comments;
        this.attributes = attributes;
        this.status = status;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationUpdateParams{");
        sb.append("description='").append(description).append('\'');
        sb.append(", analyst=").append(analyst);
        sb.append(", method=").append(method);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", panels=").append(panels);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public InterpretationUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public InterpretationUpdateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public InterpretationMethod getMethod() {
        return method;
    }

    public InterpretationUpdateParams setMethod(InterpretationMethod method) {
        this.method = method;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public InterpretationUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public InterpretationUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<ClinicalVariant> getPrimaryFindings() {
        return primaryFindings;
    }

    public InterpretationUpdateParams setPrimaryFindings(List<ClinicalVariant> primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public List<ClinicalVariant> getSecondaryFindings() {
        return secondaryFindings;
    }

    public InterpretationUpdateParams setSecondaryFindings(List<ClinicalVariant> secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }

    public List<PanelReferenceParam> getPanels() {
        return panels;
    }

    public InterpretationUpdateParams setPanels(List<PanelReferenceParam> panels) {
        this.panels = panels;
        return this;
    }

    public List<ClinicalCommentParam> getComments() {
        return comments;
    }

    public InterpretationUpdateParams setComments(List<ClinicalCommentParam> comments) {
        this.comments = comments;
        return this;
    }

    public StatusParam getStatus() {
        return status;
    }

    public InterpretationUpdateParams setStatus(StatusParam status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public InterpretationUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
