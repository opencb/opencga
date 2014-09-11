package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Project {

    private String projectId;
    private String projectName;
    private String userId;
    private String creationDate;
    private String description;
    private String state;

    private List<Study> studies;

    public Project() {
    }

    public Project(String projectId, String projectName, String userId, String creationDate, String description, String state, List<Study> studies) {
        this.projectId = projectId;
        this.projectName = projectName;
        this.userId = userId;
        this.creationDate = creationDate;
        this.description = description;
        this.state = state;
        this.studies = studies;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<Study> getStudies() {
        return studies;
    }

    public void setStudies(List<Study> studies) {
        this.studies = studies;
    }
}
