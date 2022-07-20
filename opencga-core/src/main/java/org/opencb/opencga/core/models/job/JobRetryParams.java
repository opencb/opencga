package org.opencb.opencga.core.models.job;

import org.opencb.commons.datastore.core.ObjectMap;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class JobRetryParams {

    @DataField(description = ParamConstants.JOB_RETRY_PARAMS_JOB_DESCRIPTION)
    private String job;
    @DataField(description = ParamConstants.JOB_RETRY_PARAMS_FORCE_DESCRIPTION)
    private boolean force;
    @DataField(description = ParamConstants.JOB_RETRY_PARAMS_PARAMS_DESCRIPTION)
    private ObjectMap params;

    public JobRetryParams() {
    }

    public JobRetryParams(String job, boolean force, ObjectMap params) {
        this.job = job;
        this.force = force;
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

    public ObjectMap getParams() {
        return params;
    }

    public JobRetryParams setParams(ObjectMap params) {
        this.params = params;
        return this;
    }
}
