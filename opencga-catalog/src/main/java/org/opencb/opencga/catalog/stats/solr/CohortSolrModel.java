package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class CohortSolrModel {

    @Field
    private long uid;

    @Field
    private String studyId;

    @Field
    private String type;

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
    private int release;

    @Field
    private int numSamples;

    @Field
    private List<String> acl;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public CohortSolrModel() {
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortSolrModel{");
        sb.append("uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", creationYear='").append(creationYear).append('\'');
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay='").append(creationDay).append('\'');
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", release=").append(release);
        sb.append(", numSamples=").append(numSamples);
        sb.append(", acl=").append(acl);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public long getUid() {
        return uid;
    }

    public CohortSolrModel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public CohortSolrModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getType() {
        return type;
    }

    public CohortSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public int getCreationYear() {
        return creationYear;
    }

    public CohortSolrModel setCreationYear(int creationYear) {
        this.creationYear = creationYear;
        return this;
    }

    public String getCreationMonth() {
        return creationMonth;
    }

    public CohortSolrModel setCreationMonth(String creationMonth) {
        this.creationMonth = creationMonth;
        return this;
    }

    public int getCreationDay() {
        return creationDay;
    }

    public CohortSolrModel setCreationDay(int creationDay) {
        this.creationDay = creationDay;
        return this;
    }

    public String getCreationDayOfWeek() {
        return creationDayOfWeek;
    }

    public CohortSolrModel setCreationDayOfWeek(String creationDayOfWeek) {
        this.creationDayOfWeek = creationDayOfWeek;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public CohortSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public CohortSolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public CohortSolrModel setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public CohortSolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public CohortSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}
