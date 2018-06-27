package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 27/06/18.
 */
public class IndividualSolrModel {

    @Field
    private String uuid;

    @Field
    private String name;

    @Field
    private String father;

    @Field
    private String mother;

    @Field
    private String multiple; /// ????

    @Field
    private String family;

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
    private List<String> sample;

    @Field
    private boolean parentalConsanguinity;

    public IndividualSolrModel() {
        this.phenotypes = new ArrayList<>();
        this.sample = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualSolrModel{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", father='").append(father).append('\'');
        sb.append(", mother='").append(mother).append('\'');
        sb.append(", multiple='").append(multiple).append('\'');
        sb.append(", family='").append(family).append('\'');
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
        sb.append(", sample=").append(sample);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public IndividualSolrModel setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getName() {
        return name;
    }

    public IndividualSolrModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getFather() {
        return father;
    }

    public IndividualSolrModel setFather(String father) {
        this.father = father;
        return this;
    }

    public String getMother() {
        return mother;
    }

    public IndividualSolrModel setMother(String mother) {
        this.mother = mother;
        return this;
    }

    public String getMultiple() {
        return multiple;
    }

    public IndividualSolrModel setMultiple(String multiple) {
        this.multiple = multiple;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public IndividualSolrModel setFamily(String family) {
        this.family = family;
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

    public List<String> getSample() {
        return sample;
    }

    public IndividualSolrModel setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public IndividualSolrModel setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }
}
