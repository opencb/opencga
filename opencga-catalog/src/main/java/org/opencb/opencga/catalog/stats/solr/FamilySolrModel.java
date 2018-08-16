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
    private List<String> phenotypes;

    @Field
    private int numMembers;

    @Field
    private int expectedSize;

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
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", numMembers=").append(numMembers);
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", acl=").append(acl);
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

    public int getCreationYear() {
        return creationYear;
    }

    public FamilySolrModel setCreationYear(int creationYear) {
        this.creationYear = creationYear;
        return this;
    }

    public String getCreationMonth() {
        return creationMonth;
    }

    public FamilySolrModel setCreationMonth(String creationMonth) {
        this.creationMonth = creationMonth;
        return this;
    }

    public int getCreationDay() {
        return creationDay;
    }

    public FamilySolrModel setCreationDay(int creationDay) {
        this.creationDay = creationDay;
        return this;
    }

    public String getCreationDayOfWeek() {
        return creationDayOfWeek;
    }

    public FamilySolrModel setCreationDayOfWeek(String creationDayOfWeek) {
        this.creationDayOfWeek = creationDayOfWeek;
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

    public int getNumMembers() {
        return numMembers;
    }

    public FamilySolrModel setNumMembers(int numMembers) {
        this.numMembers = numMembers;
        return this;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public FamilySolrModel setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
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
