package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class FileSolrModel {

    @Field
    private long uid;

    @Field
    private String studyId;

    @Field
    private String name;

    @Field
    private String type;

    @Field
    private String format;

    @Field
    private String bioformat;

    @Field
    private int release;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private boolean external;

    @Field
    private long size;

    @Field
    private String software;

    @Field
    private String experiment;

    @Field
    private int samples;

    @Field
    private List<Long> relatedFiles;

    @Field
    private List<String> acl;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public FileSolrModel() {
        this.relatedFiles = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileSolrModel{");
        sb.append("uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", format='").append(format).append('\'');
        sb.append(", bioformat='").append(bioformat).append('\'');
        sb.append(", release=").append(release);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", external=").append(external);
        sb.append(", size=").append(size);
        sb.append(", software='").append(software).append('\'');
        sb.append(", experiment='").append(experiment).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public long getUid() {
        return uid;
    }

    public FileSolrModel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public FileSolrModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getName() {
        return name;
    }

    public FileSolrModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public FileSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public FileSolrModel setFormat(String format) {
        this.format = format;
        return this;
    }

    public String getBioformat() {
        return bioformat;
    }

    public FileSolrModel setBioformat(String bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public FileSolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FileSolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public FileSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public boolean isExternal() {
        return external;
    }

    public FileSolrModel setExternal(boolean external) {
        this.external = external;
        return this;
    }

    public long getSize() {
        return size;
    }

    public FileSolrModel setSize(long size) {
        this.size = size;
        return this;
    }

    public String getSoftware() {
        return software;
    }

    public FileSolrModel setSoftware(String software) {
        this.software = software;
        return this;
    }

    public String getExperiment() {
        return experiment;
    }

    public FileSolrModel setExperiment(String experiment) {
        this.experiment = experiment;
        return this;
    }

    public int getSamples() {
        return samples;
    }

    public FileSolrModel setSamples(int samples) {
        this.samples = samples;
        return this;
    }

    public List<Long> getRelatedFiles() {
        return relatedFiles;
    }

    public FileSolrModel setRelatedFiles(List<Long> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public FileSolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public FileSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}


