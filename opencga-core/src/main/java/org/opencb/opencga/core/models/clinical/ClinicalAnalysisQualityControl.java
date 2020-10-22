package org.opencb.opencga.core.models.clinical;

import java.util.Date;

public class ClinicalAnalysisQualityControl {

    private QualityControlSummary summary;
    private String comment;
    private String user;
    private Date date;

    public enum QualityControlSummary {
        EXCELLENT, GOOD, NORMAL, BAD, UNKNOWN
    }

    public ClinicalAnalysisQualityControl() {
        this(QualityControlSummary.UNKNOWN, "", "", null);
    }

    public ClinicalAnalysisQualityControl(QualityControlSummary summary, String comment, String user, Date date) {
        this.summary = summary;
        this.comment = comment;
        this.user = user;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQualityControl{");
        sb.append("summary=").append(summary);
        sb.append(", comment='").append(comment).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", date=").append(date);
        sb.append('}');
        return sb.toString();
    }

    public QualityControlSummary getSummary() {
        return summary;
    }

    public ClinicalAnalysisQualityControl setSummary(QualityControlSummary summary) {
        this.summary = summary;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public ClinicalAnalysisQualityControl setComment(String comment) {
        this.comment = comment;
        return this;
    }

    public String getUser() {
        return user;
    }

    public ClinicalAnalysisQualityControl setUser(String user) {
        this.user = user;
        return this;
    }

    public Date getDate() {
        return date;
    }

    public ClinicalAnalysisQualityControl setDate(Date date) {
        this.date = date;
        return this;
    }
}
