package org.opencb.opencga.catalog.core.beans;

import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Project {

//    private int id;
    private String name;
    private String alias;

    /*
     * This must be a valid userId
     */
    //private String ownerId;   Not needed
    private String creationDate;
    private String description;
    private String organization;
    private String status;
    private String lastActivity;
    private long diskUsage;

    private List<Acl> acl;
    private List<Study> studies;

    private Map<String, Object> attributes;

    public Project() {
    }

    public Project(String name, String alias, String creationDate, String description, String status,
                   String lastActivity, long diskUsage, String organization) {
        this(name, alias, creationDate, description, organization, status, lastActivity, diskUsage,
                new LinkedList<Acl>(), new LinkedList<Study>(), new HashMap<String, Object>());
    }

    public Project(String name, String alias, String creationDate, String description, String organization,
                   String status, String lastActivity, long diskUsage, List<Acl> acl, List<Study> studies,
                   Map<String, Object> attributes) {
        this.name = name;
        this.alias = alias;
        this.creationDate = creationDate;
        this.description = description;
        this.organization = organization;
        this.status = status;
        this.lastActivity = lastActivity;
        this.diskUsage = diskUsage;
        this.acl = acl;
        this.studies = studies;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Project{" +
                "attributes=" + attributes +
                ", studies=" + studies +
                ", acl=" + acl +
                ", diskUsage=" + diskUsage +
                ", lastActivity='" + lastActivity + '\'' +
                ", status='" + status + '\'' +
                ", organization='" + organization + '\'' +
                ", description='" + description + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", alias='" + alias + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLastActivity() {
        return lastActivity;
    }

    public void setLastActivity(String lastActivity) {
        this.lastActivity = lastActivity;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }

    public List<Study> getStudies() {
        return studies;
    }

    public void setStudies(List<Study> studies) {
        this.studies = studies;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }
}
