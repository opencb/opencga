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
    private String id;

    @Field
    private long uid;

    @Field
    private String studyId;

    @Field
    private boolean hasFather;

    @Field
    private boolean hasMother;

    @Field
    private int numMultiples;

    @Field
    private String multiplesType;

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
    private String lifeStatus;

    @Field
    private String affectationStatus;

    @Field
    private List<String> phenotypes;

    @Field
    private int numSamples;

    @Field
    private boolean parentalConsanguinity;

    @Field
    private List<String> acl;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public IndividualSolrModel() {
        this.annotationSets = new ArrayList<>();
        this.phenotypes = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", hasFather=").append(hasFather);
        sb.append(", hasMother=").append(hasMother);
        sb.append(", numMultiples=").append(numMultiples);
        sb.append(", multiplesType='").append(multiplesType).append('\'');
        sb.append(", sex='").append(sex).append('\'');
        sb.append(", karyotypicSex='").append(karyotypicSex).append('\'');
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", population='").append(population).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", lifeStatus='").append(lifeStatus).append('\'');
        sb.append(", affectationStatus='").append(affectationStatus).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", numSamples=").append(numSamples);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public IndividualSolrModel setId(String id) {
        this.id = id;
        return this;
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

    public boolean isHasFather() {
        return hasFather;
    }

    public IndividualSolrModel setHasFather(boolean hasFather) {
        this.hasFather = hasFather;
        return this;
    }

    public boolean isHasMother() {
        return hasMother;
    }

    public IndividualSolrModel setHasMother(boolean hasMother) {
        this.hasMother = hasMother;
        return this;
    }

    public int getNumMultiples() {
        return numMultiples;
    }

    public IndividualSolrModel setNumMultiples(int numMultiples) {
        this.numMultiples = numMultiples;
        return this;
    }

    public String getMultiplesType() {
        return multiplesType;
    }

    public IndividualSolrModel setMultiplesType(String multiplesType) {
        this.multiplesType = multiplesType;
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

    public int getCreationYear() {
        return creationYear;
    }

    public IndividualSolrModel setCreationYear(int creationYear) {
        this.creationYear = creationYear;
        return this;
    }

    public String getCreationMonth() {
        return creationMonth;
    }

    public IndividualSolrModel setCreationMonth(String creationMonth) {
        this.creationMonth = creationMonth;
        return this;
    }

    public int getCreationDay() {
        return creationDay;
    }

    public IndividualSolrModel setCreationDay(int creationDay) {
        this.creationDay = creationDay;
        return this;
    }

    public String getCreationDayOfWeek() {
        return creationDayOfWeek;
    }

    public IndividualSolrModel setCreationDayOfWeek(String creationDayOfWeek) {
        this.creationDayOfWeek = creationDayOfWeek;
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

    public int getNumSamples() {
        return numSamples;
    }

    public IndividualSolrModel setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public IndividualSolrModel setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public IndividualSolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }

    public List<String> getAnnotationSets() {
        return annotationSets;
    }

    public IndividualSolrModel setAnnotationSets(List<String> annotationSets) {
        this.annotationSets = annotationSets;
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
