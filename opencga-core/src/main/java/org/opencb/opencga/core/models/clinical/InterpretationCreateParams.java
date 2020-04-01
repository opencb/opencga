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
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class InterpretationCreateParams {

    private String id;
    private String description;
    private String clinicalAnalysisId;
    private List<DiseasePanel> panels;
    private Software software;
    private Analyst analyst;
    private List<Software> dependencies;
    private Map<String, Object> filters;
    private String creationDate;
    private List<ReportedVariant> primaryFindings;
    private List<ReportedVariant> secondaryFindings;
    private List<ReportedLowCoverage> reportedLowCoverages;
    private List<Comment> comments;
    private Map<String, Object> attributes;

    public InterpretationCreateParams() {
    }

    public InterpretationCreateParams(String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels,
                                      Software software, Analyst analyst, List<Software> dependencies, Map<String, Object> filters,
                                      String creationDate, List<ReportedVariant> primaryFindings, List<ReportedVariant> secondaryFindings,
                                      List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments,
                                      Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.panels = panels;
        this.software = software;
        this.analyst = analyst;
        this.dependencies = dependencies;
        this.filters = filters;
        this.creationDate = creationDate;
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
        this.reportedLowCoverages = reportedLowCoverages;
        this.comments = comments;
        this.attributes = attributes;
    }

    public static InterpretationCreateParams of(Interpretation interpretation) {
        return new InterpretationCreateParams(interpretation.getId(), interpretation.getDescription(),
                interpretation.getClinicalAnalysisId(), interpretation.getPanels(), interpretation.getSoftware(),
                interpretation.getAnalyst(), interpretation.getDependencies(), interpretation.getFilters(),
                interpretation.getCreationDate(), interpretation.getPrimaryFindings(), interpretation.getSecondaryFindings(),
                interpretation.getLowCoverageRegions(), interpretation.getComments(), interpretation.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", panels=").append(panels);
        sb.append(", software=").append(software);
        sb.append(", analyst=").append(analyst);
        sb.append(", dependencies=").append(dependencies);
        sb.append(", filters=").append(filters);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", reportedLowCoverages=").append(reportedLowCoverages);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Interpretation toClinicalInterpretation() {
        return new Interpretation(id, description, clinicalAnalysisId, panels, software, analyst, dependencies, filters, creationDate,
                primaryFindings, secondaryFindings, reportedLowCoverages, comments, attributes);
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

    public List<DiseasePanel> getPanels() {
        return panels;
    }

    public InterpretationCreateParams setPanels(List<DiseasePanel> panels) {
        this.panels = panels;
        return this;
    }

    public Software getSoftware() {
        return software;
    }

    public InterpretationCreateParams setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public Analyst getAnalyst() {
        return analyst;
    }

    public InterpretationCreateParams setAnalyst(Analyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<Software> getDependencies() {
        return dependencies;
    }

    public InterpretationCreateParams setDependencies(List<Software> dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public Map<String, Object> getFilters() {
        return filters;
    }

    public InterpretationCreateParams setFilters(Map<String, Object> filters) {
        this.filters = filters;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public InterpretationCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public List<ReportedVariant> getPrimaryFindings() {
        return primaryFindings;
    }

    public InterpretationCreateParams setPrimaryFindings(List<ReportedVariant> primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public List<ReportedVariant> getSecondaryFindings() {
        return secondaryFindings;
    }

    public InterpretationCreateParams setSecondaryFindings(List<ReportedVariant> secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }

    public List<ReportedLowCoverage> getReportedLowCoverages() {
        return reportedLowCoverages;
    }

    public InterpretationCreateParams setReportedLowCoverages(List<ReportedLowCoverage> reportedLowCoverages) {
        this.reportedLowCoverages = reportedLowCoverages;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public InterpretationCreateParams setComments(List<Comment> comments) {
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
