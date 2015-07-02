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
    private Type type;
    private String creatorId;
    private String creationDate;
    private String description;
    private String status;
    private String lastActivity;
    private long diskUsage;
    private String cipher;

    private List<Acl> acl;
    private List<Experiment> experiments;

    private List<File> files;
    private List<Job> jobs;
    private List<Sample> samples;

    private List<Dataset> datasets;
    private List<Cohort> cohorts;

    private List<VariableSet> variableSets;

    private URI uri;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    /**
     * To think about:
        public static final String STUDY_TYPE = "study_type";
        private List<Sample> files;
        private List<SampleAnnotationDescription> files;
     */

    public enum Type {
        CASE_CONTROL,
        CASE_SET,
        CONTROL_SET,
        PAIRED,
        PAIRED_TUMOR,
        AGGREGATE,
        TIME_SERIES,
        FAMILY,
        TRIO,
        COLLECTION
    }

    public Study() {
    }

    public Study(String name, String alias, Type type, String description, String status, URI uri) {
        this(-1, name, alias, type, null, TimeUtils.getTime(), description, status, null, 0, "",
                new ArrayList<Acl>(), new ArrayList<Experiment>(), new ArrayList<File>(), new LinkedList<Job>(),
                new LinkedList<Sample>(), new LinkedList<Dataset>(), new LinkedList<Cohort>(), new LinkedList<VariableSet>(), uri, new HashMap<String, Object>(), new HashMap<String, Object>());
    }

    public Study(int id, String name, String alias, Type type, String creatorId, String creationDate,
                 String description, String status, String lastActivity, long diskUsage, String cipher, List<Acl> acl,
                 List<Experiment> experiments, List<File> files, List<Job> jobs, List<Sample> samples, List<Dataset> datasets,
                 List<Cohort> cohorts, List<VariableSet> variableSets, URI uri,
                 Map<String, Object> stats, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.creatorId = creatorId;
        this.creationDate = creationDate;
        this.description = description;
        this.status = status;
        this.lastActivity = lastActivity;
        this.diskUsage = diskUsage;
        this.cipher = cipher;
        this.acl = acl;
        this.experiments = experiments;
        this.files = files;
        this.jobs = jobs;
        this.samples = samples;
        this.datasets = datasets;
        this.cohorts = cohorts;
        this.variableSets = variableSets;
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
                ", type=" + type +
                ", creatorId='" + creatorId + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", description='" + description + '\'' +
                ", status='" + status + '\'' +
                ", lastActivity='" + lastActivity + '\'' +
                ", diskUsage=" + diskUsage +
                ", cipher='" + cipher + '\'' +
                ", acl=" + acl +
                ", experiments=" + experiments +
                ", files=" + files +
                ", jobs=" + jobs +
                ", samples=" + samples +
                ", datasets=" + datasets +
                ", cohorts=" + cohorts +
                ", variableSets=" + variableSets +
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

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
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

    public List<Job> getJobs() {
        return jobs;
    }

    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public void setSamples(List<Sample> samples) {
        this.samples = samples;
    }

    public List<Dataset> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<Dataset> datasets) {
        this.datasets = datasets;
    }

    public List<Cohort> getCohorts() {
        return cohorts;
    }

    public void setCohorts(List<Cohort> cohorts) {
        this.cohorts = cohorts;
    }

    public List<VariableSet> getVariableSets() {
        return variableSets;
    }

    public void setVariableSets(List<VariableSet> variableSets) {
        this.variableSets = variableSets;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
