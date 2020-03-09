package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.AclParams;

// Acl params to communicate the WS and the sample manager
public class StudyAclParams extends AclParams {

    private String template;

    public StudyAclParams() {
    }

    public StudyAclParams(String permissions, Action action, String template) {
        super(permissions, action);
        this.template = template;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyAclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append(", template='").append(template).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getTemplate() {
        return template;
    }

    public StudyAclParams setTemplate(String template) {
        this.template = template;
        return this;
    }

    public StudyAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public StudyAclParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
