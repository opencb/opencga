package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.opencga.core.models.DiseasePanel;

import java.util.List;
import java.util.Map;

public class Interpretation {

    private String id;
    private String description;
    private String clinicalAnalysisId;
    private List<DiseasePanel> panels;

    /**
     * Interpretation algorithm tool used to generate this interpretation.
     */
    private Software software;
    private Analyst analyst;
    private List<Software> dependencies;
    private Map<String, Object> filters;
    private String creationDate;

    private List<ReportedVariant> reportedVariants;
    private List<ReportedLowCoverage> reportedLowCoverages;

    private List<Comment> comments;

    /**
     * Users can add custom information in this field.
     * OpenCGA uses this field to store the Clinical Analysis object in key 'OPENCGA_CLINICAL_ANALYSIS'
     */
    private Map<String, Object> attributes;


    public Interpretation() {
    }

    public Interpretation(String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
                          List<ReportedVariant> reportedVariants, List<ReportedLowCoverage> reportedLowCoverages,
                          List<Comment> comments, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.panels = panels;
        this.software = software;
        this.analyst = analyst;
        this.dependencies = dependencies;
        this.filters = filters;
        this.creationDate = creationDate;
        this.reportedVariants = reportedVariants;
        this.reportedLowCoverages = reportedLowCoverages;
        this.comments = comments;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Interpretation{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", panels=").append(panels);
        sb.append(", software=").append(software);
        sb.append(", analyst=").append(analyst);
        sb.append(", dependencies=").append(dependencies);
        sb.append(", filters=").append(filters);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", reportedVariants=").append(reportedVariants);
        sb.append(", reportedLowCoverages=").append(reportedLowCoverages);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Interpretation setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Interpretation setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public Interpretation setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public List<DiseasePanel> getPanels() {
        return panels;
    }

    public Interpretation setPanels(List<DiseasePanel> panels) {
        this.panels = panels;
        return this;
    }

    public Software getSoftware() {
        return software;
    }

    public Interpretation setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public Analyst getAnalyst() {
        return analyst;
    }

    public Interpretation setAnalyst(Analyst analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<Software> getDependencies() {
        return dependencies;
    }

    public Interpretation setDependencies(List<Software> dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public Map<String, Object> getFilters() {
        return filters;
    }

    public Interpretation setFilters(Map<String, Object> filters) {
        this.filters = filters;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Interpretation setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public List<ReportedVariant> getReportedVariants() {
        return reportedVariants;
    }

    public Interpretation setReportedVariants(List<ReportedVariant> reportedVariants) {
        this.reportedVariants = reportedVariants;
        return this;
    }

    public List<ReportedLowCoverage> getReportedLowCoverages() {
        return reportedLowCoverages;
    }

    public Interpretation setReportedLowCoverages(List<ReportedLowCoverage> reportedLowCoverages) {
        this.reportedLowCoverages = reportedLowCoverages;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public Interpretation setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Interpretation setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
