package org.opencb.opencga.core.models.job;

import org.opencb.commons.datastore.core.ObjectMap;

public class JobRetryParams {

    private String job;
    private boolean force;
    private String scheduledStartTime;
    private ObjectMap params;

    public JobRetryParams() {
    }

    public JobRetryParams(String job, boolean force, String scheduledStartTime, ObjectMap params) {
        this.job = job;
        this.force = force;
        this.scheduledStartTime = scheduledStartTime;
        this.params = params;
    }

    public String getJob() {
        return job;
    }

    public JobRetryParams setJob(String job) {
        this.job = job;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public JobRetryParams setForce(boolean force) {
        this.force = force;
        return this;
    }

    public String getScheduledStartTime() {
        return scheduledStartTime;
    }

    public JobRetryParams setScheduledStartTime(String scheduledStartTime) {
        this.scheduledStartTime = scheduledStartTime;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public JobRetryParams setParams(ObjectMap params) {
        this.params = params;
        return this;
    }
}
