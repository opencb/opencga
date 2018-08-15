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
    private String creationDate;

    @Field
    private String status;

    @Field
    private int release;

    @Field
    private int samples;

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
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", release=").append(release);
        sb.append(", samples=").append(samples);
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

    public String getCreationDate() {
        return creationDate;
    }

    public CohortSolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
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

    public int getSamples() {
        return samples;
    }

    public CohortSolrModel setSamples(int samples) {
        this.samples = samples;
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
