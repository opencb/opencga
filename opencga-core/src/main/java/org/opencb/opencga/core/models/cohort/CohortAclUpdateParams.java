package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.AclParams;

public class CohortAclUpdateParams extends AclParams {

    private String cohort;

    public CohortAclUpdateParams() {
    }

    public CohortAclUpdateParams(String permissions, Action action, String cohort) {
        super(permissions, action);
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortAclUpdateParams{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }

    public String getCohort() {
        return cohort;
    }

    public CohortAclUpdateParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public CohortAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public CohortAclUpdateParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
