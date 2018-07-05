package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
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
    private String type;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private String familyUuid;

    @Field
    private List<String> familyMembersUuid;

    @Field
    private int release;

    @Field
    private int samples;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public CohortSolrModel() {
        this.familyMembersUuid = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortSolrModel{");
        sb.append("uid=").append(uid);
        sb.append(", type='").append(type).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", familyUuid='").append(familyUuid).append('\'');
        sb.append(", familyMembersUuid=").append(familyMembersUuid);
        sb.append(", release=").append(release);
        sb.append(", samplesUuid=").append(samples);
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

    public String getFamilyUuid() {
        return familyUuid;
    }

    public CohortSolrModel setFamilyUuid(String familyUuid) {
        this.familyUuid = familyUuid;
        return this;
    }

    public List<String> getFamilyMembersUuid() {
        return familyMembersUuid;
    }

    public CohortSolrModel setFamilyMembersUuid(List<String> familyMembersUuid) {
        this.familyMembersUuid = familyMembersUuid;
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

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public CohortSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }

}
