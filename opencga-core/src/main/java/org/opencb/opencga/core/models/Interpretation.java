package org.opencb.opencga.core.models;

import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;

import java.util.List;
import java.util.Map;

public class Interpretation extends PrivateStudyUid {

    private org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation;

    private String uuid;

    public Interpretation() {
        this.interpretation = new org.opencb.biodata.models.clinical.interpretation.Interpretation();
    }

    public Interpretation(String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
                          List<ReportedVariant> reportedVariants, List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments,
                          Map<String, Object> attributes) {
        this.interpretation = new org.opencb.biodata.models.clinical.interpretation.Interpretation(id, description, clinicalAnalysisId,
                panels, Status.READY, software, analyst, dependencies, filters, creationDate, reportedVariants, reportedLowCoverages,
                comments, attributes, 1);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("id='").append(interpretation.getId()).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(interpretation.getDescription()).append('\'');
        sb.append(", clinicalAnalysisId='").append(interpretation.getClinicalAnalysisId()).append('\'');
        sb.append(", panels=").append(interpretation.getPanels());
        sb.append(", software=").append(interpretation.getSoftware());
        sb.append(", analyst=").append(interpretation.getAnalyst());
        sb.append(", dependencies=").append(interpretation.getDependencies());
        sb.append(", filters=").append(interpretation.getFilters());
        sb.append(", creationDate='").append(interpretation.getCreationDate()).append('\'');
        sb.append(", reportedVariants=").append(interpretation.getReportedVariants());
        sb.append(", reportedLowCoverages=").append(interpretation.getReportedLowCoverages());
        sb.append(", comments=").append(interpretation.getComments());
        sb.append(", attributes=").append(interpretation.getAttributes());
        sb.append('}');
        return sb.toString();
    }

    public org.opencb.biodata.models.clinical.interpretation.Interpretation getInterpretation() {
        return interpretation;
    }

    public Interpretation setInterpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
        this.interpretation = interpretation;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Interpretation setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
