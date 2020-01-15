package org.opencb.opencga.core.models.admin;

import java.util.List;

public class UserImportParams {

    private String authenticationOriginId;
    private List<String> id;
    private ResourceType resourceType;
    private String study;
    private String studyGroup;

    public enum ResourceType {
        USER,
        GROUP,
        APPLICATION
    }

    public UserImportParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserImportParams{");
        sb.append("authenticationOriginId='").append(authenticationOriginId).append('\'');
        sb.append(", id=").append(id);
        sb.append(", resourceType=").append(resourceType);
        sb.append(", study='").append(study).append('\'');
        sb.append(", studyGroup='").append(studyGroup).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAuthenticationOriginId() {
        return authenticationOriginId;
    }

    public UserImportParams setAuthenticationOriginId(String authenticationOriginId) {
        this.authenticationOriginId = authenticationOriginId;
        return this;
    }

    public List<String> getId() {
        return id;
    }

    public UserImportParams setId(List<String> id) {
        this.id = id;
        return this;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public UserImportParams setResourceType(ResourceType resourceType) {
        this.resourceType = resourceType;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public UserImportParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getStudyGroup() {
        return studyGroup;
    }

    public UserImportParams setStudyGroup(String studyGroup) {
        this.studyGroup = studyGroup;
        return this;
    }
}
