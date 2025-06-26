package org.opencb.opencga.core.models.clinical;

public class CvdbIndex {

    private String jobId;
    private CvdbIndexStatus status;

    public CvdbIndex() {
    }

    public CvdbIndex(String jobId, CvdbIndexStatus status) {
        this.jobId = jobId;
        this.status = status;
    }

    public static CvdbIndex init() {
        return new CvdbIndex("", CvdbIndexStatus.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbIndex{");
        sb.append("jobId='").append(jobId).append('\'');
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getJobId() {
        return jobId;
    }

    public CvdbIndex setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public CvdbIndexStatus getStatus() {
        return status;
    }

    public CvdbIndex setStatus(CvdbIndexStatus status) {
        this.status = status;
        return this;
    }
}
