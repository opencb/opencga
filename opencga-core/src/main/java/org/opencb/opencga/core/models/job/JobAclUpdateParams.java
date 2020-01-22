package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.AclParams;

public class JobAclUpdateParams extends AclParams {

    private String job;

    public JobAclUpdateParams() {
    }

    public JobAclUpdateParams(String permissions, Action action, String job) {
        super(permissions, action);
        this.job = job;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobAclUpdateParams{");
        sb.append("job='").append(job).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }

    public String getJob() {
        return job;
    }

    public JobAclUpdateParams setJob(String job) {
        this.job = job;
        return this;
    }

    public JobAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public JobAclUpdateParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
