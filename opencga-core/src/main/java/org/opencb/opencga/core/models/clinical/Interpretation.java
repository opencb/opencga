package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.opencga.core.models.DiseasePanel;

import java.util.List;
import java.util.Map;

public class Interpretation {

    private String id;
    @Deprecated
    private String name;
    private String description;

    private DiseasePanel panel;
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

    public Interpretation(String id, String name, String description, DiseasePanel panel, Software software, Analyst analyst,
                          List<Version> versions, Map<String, Object> filters, String creationDate, List<Comment> comments, Map<String,
            Object> attributes, List<ReportedVariant> reportedVariants) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.panel = panel;
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
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", panel=").append(panel);
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

    public String getId() {
        return id;
    }

    public Interpretation setId(String id) {
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

    public DiseasePanel getPanel() {
        return panel;
    }

    public Interpretation setPanel(DiseasePanel panel) {
        this.panel = panel;
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
