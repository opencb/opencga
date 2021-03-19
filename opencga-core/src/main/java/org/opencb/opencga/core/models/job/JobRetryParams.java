package org.opencb.opencga.core.models.job;

public class JobRetryParams {

    private String job;

    public JobRetryParams() {
    }

    public JobRetryParams(String job) {
        this.job = job;
    }

    public String getJob() {
        return job;
    }

    public JobRetryParams setJob(String job) {
        this.job = job;
        return this;
    }
}
