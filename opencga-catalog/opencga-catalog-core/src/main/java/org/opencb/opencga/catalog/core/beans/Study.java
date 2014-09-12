package org.opencb.opencga.catalog.core.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Study {

    private int id;
    private String name;
    private String alias;
    private String type;
    private String creatorId;
    private String creationDate;
    private String description;
    private String status;
    private long diskUsage;
    private String cipher;

    private List<Acl> acl;
    private List<Experiment> experiments;
    private List<File> files;

    private Map<String, Object> attributes;

    /**
     * To think about:
        public static final String STUDY_TYPE = "study_type";
     */

    public Study() {
    }

    public Study(int id, String name, String alias, String type, String creatorId, String creationDate, String description,
                 String status) {
        this(id, name, alias, type, creatorId, creationDate, description, status, 0, "", new ArrayList<Acl>(),
                new ArrayList<Experiment>(), new ArrayList<File>(), new HashMap<String, Object>());
    }

    public Study(int id, String name, String alias, String type, String creatorId, String creationDate, String description,
                 String status, long diskUsage, String cipher, List<Acl> acl, List<Experiment> experiments,
                 List<File> files, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.creatorId = creatorId;
        this.creationDate = creationDate;
        this.description = description;
        this.status = status;
        this.diskUsage = diskUsage;
        this.cipher = cipher;
        this.acl = acl;
        this.experiments = experiments;
        this.files = files;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Study{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", creatorId='" + creatorId + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", description='" + description + '\'' +
                ", status='" + status + '\'' +
                ", diskUsage=" + diskUsage +
                ", cipher='" + cipher + '\'' +
                ", acl=" + acl +
                ", files=" + files +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(String creatorId) {
        this.creatorId = creatorId;
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

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public String getCipher() {
        return cipher;
    }

    public void setCipher(String cipher) {
        this.cipher = cipher;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }

    public List<File> getFiles() {
        return files;
    }

    public void setFiles(List<File> files) {
        this.files = files;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public List<Experiment> getExperiments() {
        return experiments;
    }

    public void setExperiments(List<Experiment> experiments) {
        this.experiments = experiments;
    }

}
