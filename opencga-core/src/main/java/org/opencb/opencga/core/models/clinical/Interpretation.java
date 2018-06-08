package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Software;

import java.util.List;
import java.util.Map;

public class Interpretation {

    private long id;
    private String name;
    private String description;

    private ClinicalAnalysis clinicalAnalysis;

    private Software software;
    private Analyst analyst;
    private List<Version> versions;
    private Map<String, Object> filters;
    private String creationDate;

    private List<Comment> comments;
    private Map<String, Object> attributes;

    private List<ReportedVariant> reportedVariants;

    public Interpretation() {
    }

    public Interpretation(long id, String name, String description, ClinicalAnalysis clinicalAnalysis, Software software, Analyst
            analyst, List<Version> versions, Map<String, Object> filters, String creationDate, List<Comment> comments,
                          Map<String, Object> attributes, List<ReportedVariant> reportedVariants) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.clinicalAnalysis = clinicalAnalysis;
        this.software = software;
        this.analyst = analyst;
        this.versions = versions;
        this.filters = filters;
        this.creationDate = creationDate;
        this.comments = comments;
        this.attributes = attributes;
        this.reportedVariants = reportedVariants;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysis=").append(clinicalAnalysis);
        sb.append(", software=").append(software);
        sb.append(", analyst=").append(analyst);
        sb.append(", versions=").append(versions);
        sb.append(", filters=").append(filters);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append(", reportedVariants=").append(reportedVariants);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Interpretation setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Interpretation setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Interpretation setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalysis getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public Interpretation setClinicalAnalysis(ClinicalAnalysis clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
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

    public List<Version> getVersions() {
        return versions;
    }

    public Interpretation setVersions(List<Version> versions) {
        this.versions = versions;
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

    public List<ReportedVariant> getReportedVariants() {
        return reportedVariants;
    }

    public Interpretation setReportedVariants(List<ReportedVariant> reportedVariants) {
        this.reportedVariants = reportedVariants;
        return this;
    }
}
