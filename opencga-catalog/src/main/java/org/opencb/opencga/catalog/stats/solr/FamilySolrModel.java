package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class FamilySolrModel {

    @Field
    private long uid;

    @Field
    private String studyId;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private List<String> phenotypes;

    @Field
    private int familyMembers;

    @Field
    private int release;

    @Field
    private int version;

    @Field
    private List<String> acl;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public FamilySolrModel() {
        this.annotations = new HashMap<>();
        this.phenotypes = new ArrayList<>();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilySolrModel{");
        sb.append("uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", familyMembers=").append(familyMembers);
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public long getUid() {
        return uid;
    }

    public FamilySolrModel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public FamilySolrModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FamilySolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public FamilySolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public FamilySolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public int getFamilyMembers() {
        return familyMembers;
    }

    public FamilySolrModel setFamilyMembers(int familyMembers) {
        this.familyMembers = familyMembers;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public FamilySolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public FamilySolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public FamilySolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public FamilySolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}
