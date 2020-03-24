package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.AclParams;

// Acl params to communicate the WS and the sample manager
public class FileAclParams extends AclParams {

    private String sample;

    public FileAclParams() {
    }

    public FileAclParams(String permissions, Action action, String sample) {
        super(permissions, action);
        this.sample = sample;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileAclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append(", sample='").append(sample).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public FileAclParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public FileAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public FileAclParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
