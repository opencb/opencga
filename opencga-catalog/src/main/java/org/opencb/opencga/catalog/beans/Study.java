package org.opencb.opencga.catalog.beans;

import org.opencb.opencga.lib.common.TimeUtils;

import java.net.URI;
import java.util.*;

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
    private List<Analysis> analyses;

    private URI uri;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    /**
     * To think about:
        public static final String STUDY_TYPE = "study_type";
        private List<File> files;
        private List<Dataset> files;
        private List<Sample> files;
        private List<SampleAnnotationDescription> files;
     */

    public Study() {
    }

    public Study(String name, String alias, String type, String description, String status, URI uri) {
        this(-1, name, alias, type, null, TimeUtils.getTime(), description, status, 0, "", new ArrayList<Acl>(),
                new ArrayList<Experiment>(), new ArrayList<File>(), new LinkedList<Analysis>(),
                uri, new HashMap<String, Object>(), new HashMap<String, Object>());
    }

    public Study(int id, String name, String alias, String type, String creatorId, String creationDate,
                 String description, String status, long diskUsage, String cipher, List<Acl> acl,
                 List<Experiment> experiments, List<File> files, List<Analysis> analyses, URI uri,
                 Map<String, Object> stats, Map<String, Object> attributes) {
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
        this.analyses = analyses;
        this.uri = uri;
        this.stats = stats;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Study{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                ", type='" + type + '\'' +
                ", creatorId='" + creatorId + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", description='" + description + '\'' +
                ", status='" + status + '\'' +
                ", diskUsage=" + diskUsage +
                ", cipher='" + cipher + '\'' +
                ", acl=" + acl +
                ", experiments=" + experiments +
                ", files=" + files +
                ", analyses=" + analyses +
                ", uri=" + uri +
                ", stats=" + stats +
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

    public List<File> getFiles() {
        return files;
    }

    public void setFiles(List<File> files) {
        this.files = files;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public void setStats(Map<String, Object> stats) {
        this.stats = stats;
    }

    public List<Analysis> getAnalyses() {
        return analyses;
    }

    public void setAnalyses(List<Analysis> analyses) {
        this.analyses = analyses;
    }
}
