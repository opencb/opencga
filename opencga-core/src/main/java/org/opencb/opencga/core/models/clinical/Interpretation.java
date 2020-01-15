package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.opencga.core.models.IPrivateStudyUid;

import java.util.List;
import java.util.Map;

public class Interpretation extends org.opencb.biodata.models.clinical.interpretation.Interpretation implements IPrivateStudyUid {

    private String uuid;

    // Private fields
    private long studyUid;
    private long uid;

    public Interpretation() {
        super();
    }

    public Interpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
        this(interpretation.getId(), interpretation.getDescription(), interpretation.getClinicalAnalysisId(), interpretation.getPanels(),
                interpretation.getSoftware(), interpretation.getAnalyst(), interpretation.getDependencies(), interpretation.getFilters(),
                interpretation.getCreationDate(), interpretation.getPrimaryFindings(), interpretation.getSecondaryFindings(),
                interpretation.getLowCoverageRegions(), interpretation.getComments(), interpretation.getAttributes());
    }

    public Interpretation(String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
                          List<ReportedVariant> primaryFindinds, List<ReportedVariant> secondaryFindings,
                          List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments, Map<String, Object> attributes) {
        super(id, description, clinicalAnalysisId, software, analyst, dependencies, filters, panels, primaryFindinds, secondaryFindings,
                reportedLowCoverages, comments, Status.NOT_REVIEWED, creationDate, 1, attributes);
    }

    public Interpretation(String uuid, String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
                          List<ReportedVariant> primaryFindinds, List<ReportedVariant> secondaryFindings,
                          List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments, Map<String, Object> attributes) {
        super(id, description, clinicalAnalysisId, software, analyst, dependencies, filters, panels, primaryFindinds, secondaryFindings,
                reportedLowCoverages, comments, Status.NOT_REVIEWED, creationDate, 1, attributes);
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("id='").append(this.getId()).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(this.getDescription()).append('\'');
        sb.append(", clinicalAnalysisId='").append(this.getClinicalAnalysisId()).append('\'');
        sb.append(", software=").append(this.getSoftware());
        sb.append(", analyst=").append(this.getAnalyst());
        sb.append(", dependencies=").append(this.getDependencies());
        sb.append(", filters=").append(this.getFilters());
        sb.append(", panels=").append(this.getPanels());
        sb.append(", primaryFindings=").append(this.getPrimaryFindings());
        sb.append(", secondaryFindings=").append(this.getSecondaryFindings());
        sb.append(", lowCoverageRegions=").append(this.getLowCoverageRegions());
        sb.append(", comments=").append(this.getComments());
        sb.append(", status=").append(this.getStatus());
        sb.append(", creationDate='").append(this.getCreationDate()).append('\'');
        sb.append(", version=").append(this.getVersion());
        sb.append(", attributes=").append(this.getAttributes());
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public Interpretation setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public Interpretation setUid(long uid) {
        this.uid = uid;
        return this;
    }

    @Override
    public long getStudyUid() {
        return studyUid;
    }

    @Override
    public Interpretation setStudyUid(long studyUid) {
        this.studyUid = studyUid;
        return this;
    }


    // Biodata interpretation setters

    @Override
    public Interpretation setId(String id) {
        super.setId(id);
        return this;
    }

    @Override
    public Interpretation setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    @Override
    public Interpretation setClinicalAnalysisId(String clinicalAnalysisId) {
        super.setClinicalAnalysisId(clinicalAnalysisId);
        return this;
    }

    @Override
    public Interpretation setSoftware(Software software) {
        super.setSoftware(software);
        return this;
    }

    @Override
    public Interpretation setAnalyst(Analyst analyst) {
        super.setAnalyst(analyst);
        return this;
    }

    @Override
    public Interpretation setDependencies(List<Software> dependencies) {
        super.setDependencies(dependencies);
        return this;
    }

    @Override
    public Interpretation setFilters(Map<String, Object> filters) {
        super.setFilters(filters);
        return this;
    }

    @Override
    public Interpretation setPanels(List<DiseasePanel> panels) {
        super.setPanels(panels);
        return this;
    }

    @Override
    public Interpretation setPrimaryFindings(List<ReportedVariant> primaryFindings) {
        super.setPrimaryFindings(primaryFindings);
        return this;
    }

    @Override
    public Interpretation setSecondaryFindings(List<ReportedVariant> secondaryFindings) {
        super.setSecondaryFindings(secondaryFindings);
        return this;
    }

    @Override
    public Interpretation setLowCoverageRegions(List<ReportedLowCoverage> lowCoverageRegions) {
        super.setLowCoverageRegions(lowCoverageRegions);
        return this;
    }

    @Override
    public Interpretation setComments(List<Comment> comments) {
        super.setComments(comments);
        return this;
    }

    @Override
    public Interpretation setStatus(Interpretation.Status status) {
        super.setStatus(status);
        return this;
    }

    @Override
    public Interpretation setCreationDate(String creationDate) {
        super.setCreationDate(creationDate);
        return this;
    }

    @Override
    public Interpretation setVersion(int version) {
        super.setVersion(version);
        return this;
    }

    @Override
    public Interpretation setAttributes(Map<String, Object> attributes) {
        super.setAttributes(attributes);
        return this;
    }

}
