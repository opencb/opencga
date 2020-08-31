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
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.interpretation.Analyst;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class InterpretationCreateParams {

    private String id;
    private String description;
    private String clinicalAnalysisId;
    private Analyst analyst;
    private List<InterpretationMethod> methods;
    private String creationDate;
    private List<ClinicalVariant> primaryFindings;
    private List<ClinicalVariant> secondaryFindings;
    private List<ClinicalComment> comments;
    private Map<String, Object> attributes;

    public InterpretationCreateParams() {
    }

    public InterpretationCreateParams(String id, String description, String clinicalAnalysisId, Analyst analyst,
                                      List<InterpretationMethod> methods, String creationDate, List<ClinicalVariant> primaryFindings,
                                      List<ClinicalVariant> secondaryFindings, List<ClinicalComment> comments, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.analyst = analyst;
        this.methods = methods;
        this.creationDate = creationDate;
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
        this.comments = comments;
        this.attributes = attributes;
    }

    public static InterpretationCreateParams of(Interpretation interpretation) {
        return new InterpretationCreateParams(interpretation.getId(), interpretation.getDescription(),
                interpretation.getClinicalAnalysisId(), interpretation.getAnalyst(), interpretation.getMethods(),
                interpretation.getCreationDate(), interpretation.getPrimaryFindings(), interpretation.getSecondaryFindings(),
                interpretation.getComments(), interpretation.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", analyst=").append(analyst);
        sb.append(", methods=").append(methods);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Interpretation toClinicalInterpretation() {
        return new Interpretation(id, description, clinicalAnalysisId, analyst, methods, creationDate, primaryFindings, secondaryFindings,
                comments, attributes);
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

    public Analyst getAnalyst() {
        return analyst;
    }

    public InterpretationCreateParams setAnalyst(Analyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<InterpretationMethod> getMethods() {
        return methods;
    }

    public InterpretationCreateParams setMethods(List<InterpretationMethod> methods) {
        this.methods = methods;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public InterpretationCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
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

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public InterpretationCreateParams setComments(List<ClinicalComment> comments) {
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
