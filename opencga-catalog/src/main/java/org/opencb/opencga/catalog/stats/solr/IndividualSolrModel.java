package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class IndividualSolrModel {

    @Field
    private long uid;

    @Field
    private String studyId;

    @Field
    private String multipleTypeName;

    @Field
    private String sex;

    @Field
    private String karyotypicSex;

    @Field
    private String ethnicity;

    @Field
    private String population;

    @Field
    private int release;

    @Field
    private int version;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private String lifeStatus;

    @Field
    private String affectationStatus;

    @Field
    private List<String> phenotypes;

    @Field
    private int samples;

    @Field
    private boolean parentalConsanguinity;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public IndividualSolrModel() {
        this.phenotypes = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualSolrModel{");
        sb.append("uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", multipleTypeName='").append(multipleTypeName).append('\'');
        sb.append(", sex='").append(sex).append('\'');
        sb.append(", karyotypicSex='").append(karyotypicSex).append('\'');
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", population='").append(population).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", lifeStatus='").append(lifeStatus).append('\'');
        sb.append(", affectationStatus='").append(affectationStatus).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", samples=").append(samples);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public long getUid() {
        return uid;
    }

    public IndividualSolrModel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public IndividualSolrModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getMultipleTypeName() {
        return multipleTypeName;
    }

    public IndividualSolrModel setMultipleTypeName(String multipleTypeName) {
        this.multipleTypeName = multipleTypeName;
        return this;
    }

    public String getSex() {
        return sex;
    }

    public IndividualSolrModel setSex(String sex) {
        this.sex = sex;
        return this;
    }

    public String getKaryotypicSex() {
        return karyotypicSex;
    }

    public IndividualSolrModel setKaryotypicSex(String karyotypicSex) {
        this.karyotypicSex = karyotypicSex;
        return this;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public IndividualSolrModel setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public IndividualSolrModel setPopulation(String population) {
        this.population = population;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public IndividualSolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public IndividualSolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public IndividualSolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public IndividualSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getLifeStatus() {
        return lifeStatus;
    }

    public IndividualSolrModel setLifeStatus(String lifeStatus) {
        this.lifeStatus = lifeStatus;
        return this;
    }

    public String getAffectationStatus() {
        return affectationStatus;
    }

    public IndividualSolrModel setAffectationStatus(String affectationStatus) {
        this.affectationStatus = affectationStatus;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public IndividualSolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public int getSamples() {
        return samples;
    }

    public IndividualSolrModel setSamples(int samples) {
        this.samples = samples;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public IndividualSolrModel setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public IndividualSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}
