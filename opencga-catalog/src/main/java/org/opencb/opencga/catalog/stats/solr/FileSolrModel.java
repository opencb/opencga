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
    private int creationYear;

    @Field
    private String creationMonth;

    @Field
    private int creationDay;

    @Field
    private String creationDayOfWeek;

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
    private int numSamples;

    @Field
    private int numRelatedFiles;

    @Field
    private List<String> acl;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public FileSolrModel() {
        this.annotationSets = new ArrayList<>();
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
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", external=").append(external);
        sb.append(", size=").append(size);
        sb.append(", software='").append(software).append('\'');
        sb.append(", experiment='").append(experiment).append('\'');
        sb.append(", numSamples=").append(numSamples);
        sb.append(", numRelatedFiles=").append(numRelatedFiles);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
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

    public int getCreationYear() {
        return creationYear;
    }

    public FileSolrModel setCreationYear(int creationYear) {
        this.creationYear = creationYear;
        return this;
    }

    public String getCreationMonth() {
        return creationMonth;
    }

    public FileSolrModel setCreationMonth(String creationMonth) {
        this.creationMonth = creationMonth;
        return this;
    }

    public int getCreationDay() {
        return creationDay;
    }

    public FileSolrModel setCreationDay(int creationDay) {
        this.creationDay = creationDay;
        return this;
    }

    public String getCreationDayOfWeek() {
        return creationDayOfWeek;
    }

    public FileSolrModel setCreationDayOfWeek(String creationDayOfWeek) {
        this.creationDayOfWeek = creationDayOfWeek;
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

    public int getNumSamples() {
        return numSamples;
    }

    public FileSolrModel setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public int getNumRelatedFiles() {
        return numRelatedFiles;
    }

    public FileSolrModel setNumRelatedFiles(int numRelatedFiles) {
        this.numRelatedFiles = numRelatedFiles;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public FileSolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }

    public List<String> getAnnotationSets() {
        return annotationSets;
    }

    public FileSolrModel setAnnotationSets(List<String> annotationSets) {
        this.annotationSets = annotationSets;
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


