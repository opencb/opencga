package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.common.TimeUtils;

public class ClinicalAnalysisAnalyst {

    private String assignee;
    private String assignedBy;
    private String date;

    public ClinicalAnalysisAnalyst() {
    }

    public ClinicalAnalysisAnalyst(String assignee, String assignedBy) {
        this.assignee = assignee;
        this.assignedBy = assignedBy;
        this.date = TimeUtils.getTime();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisAnalyst{");
        sb.append("assignee='").append(assignee).append('\'');
        sb.append(", assignedBy='").append(assignedBy).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAssignee() {
        return assignee;
    }

    public ClinicalAnalysisAnalyst setAssignee(String assignee) {
        this.assignee = assignee;
        return this;
    }

    public String getAssignedBy() {
        return assignedBy;
    }

    public ClinicalAnalysisAnalyst setAssignedBy(String assignedBy) {
        this.assignedBy = assignedBy;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalAnalysisAnalyst setDate(String date) {
        this.date = date;
        return this;
    }
}
