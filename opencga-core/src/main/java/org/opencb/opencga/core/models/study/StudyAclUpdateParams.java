package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.AclParams;

public class StudyAclUpdateParams extends AclParams {

    private String study;
    private String template;

    public StudyAclUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyAclUpdateParams{");
        sb.append("study='").append(study).append('\'');
        sb.append(", template='").append(template).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public StudyAclUpdateParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getTemplate() {
        return template;
    }

    public StudyAclUpdateParams setTemplate(String template) {
        this.template = template;
        return this;
    }

    public StudyAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public StudyAclUpdateParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
