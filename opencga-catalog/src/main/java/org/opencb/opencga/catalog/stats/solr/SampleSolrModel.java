package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class SampleSolrModel {

    @Field
    private long uid;

    @Field
    private String studyId;

    @Field
    private String source;

    @Field
    private String individualUuid;

    @Field
    private String individualKaryotypicSex;

    @Field
    private String individualEthnicity;

    @Field
    private String individualPopulation;

    @Field
    private int release;

    @Field
    private int version;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private String type;

    @Field
    private boolean somatic;

    @Field
    private List<String> phenotypes;

    @Field
    private List<String> acl;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public SampleSolrModel() {
        this.phenotypes = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleSolrModel{");
        sb.append("uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", individualUuid='").append(individualUuid).append('\'');
        sb.append(", individualKaryotypicSex='").append(individualKaryotypicSex).append('\'');
        sb.append(", individualEthnicity='").append(individualEthnicity).append('\'');
        sb.append(", individualPopulation='").append(individualPopulation).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public long getUid() {
        return uid;
    }

    public SampleSolrModel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public SampleSolrModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getSource() {
        return source;
    }

    public SampleSolrModel setSource(String source) {
        this.source = source;
        return this;
    }

    public String getIndividualUuid() {
        return individualUuid;
    }

    public SampleSolrModel setIndividualUuid(String individualUuid) {
        this.individualUuid = individualUuid;
        return this;
    }

    public String getIndividualKaryotypicSex() {
        return individualKaryotypicSex;
    }

    public SampleSolrModel setIndividualKaryotypicSex(String individualKaryotypicSex) {
        this.individualKaryotypicSex = individualKaryotypicSex;
        return this;
    }

    public String getIndividualEthnicity() {
        return individualEthnicity;
    }

    public SampleSolrModel setIndividualEthnicity(String individualEthnicity) {
        this.individualEthnicity = individualEthnicity;
        return this;
    }

    public String getIndividualPopulation() {
        return individualPopulation;
    }

    public SampleSolrModel setIndividualPopulation(String individualPopulation) {
        this.individualPopulation = individualPopulation;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public SampleSolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public SampleSolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public SampleSolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public SampleSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getType() {
        return type;
    }

    public SampleSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public boolean isSomatic() {
        return somatic;
    }

    public SampleSolrModel setSomatic(boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public SampleSolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public SampleSolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public SampleSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}

