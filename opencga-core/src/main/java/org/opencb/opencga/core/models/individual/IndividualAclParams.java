package org.opencb.opencga.core.models.individual;

import org.opencb.opencga.core.models.AclParams;

// Acl params to communicate the WS and the sample manager
public class IndividualAclParams extends AclParams {

    private String sample;
    private boolean propagate;

    public IndividualAclParams() {

    }

    public IndividualAclParams(String permissions, Action action, String sample, boolean propagate) {
        super(permissions, action);
        this.sample = sample;
        this.propagate = propagate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualAclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", propagate=").append(propagate);
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public IndividualAclParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public boolean isPropagate() {
        return propagate;
    }

    public IndividualAclParams setPropagate(boolean propagate) {
        this.propagate = propagate;
        return this;
    }

    public IndividualAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public IndividualAclParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
