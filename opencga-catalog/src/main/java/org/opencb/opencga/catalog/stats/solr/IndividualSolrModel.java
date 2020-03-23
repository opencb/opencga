package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class IndividualSolrModel extends CatalogSolrModel {

    @Field
    private boolean hasFather;

    @Field
    private boolean hasMother;

    @Field
    private String locationCity;

    @Field
    private String locationState;

    @Field
    private String locationCountry;

    @Field
    private int yearOfBirth;

    @Field
    private String monthOfBirth;

    @Field
    private int dayOfBirth;

    @Field
    private String sex;

    @Field
    private String karyotypicSex;

    @Field
    private String ethnicity;

    @Field
    private String population;

    @Field
    private int version;

    @Field
    private String lifeStatus;

    @Field
    private List<String> phenotypes;

    @Field
    private List<String> disorders;

    @Field
    private int numSamples;

    @Field
    private boolean parentalConsanguinity;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public IndividualSolrModel() {
        this.annotationSets = new ArrayList<>();
        this.phenotypes = new ArrayList<>();
        this.disorders = new ArrayList<>();
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
        sb.append(", locationCity='").append(locationCity).append('\'');
        sb.append(", locationState='").append(locationState).append('\'');
        sb.append(", locationCountry='").append(locationCountry).append('\'');
        sb.append(", yearOfBirth=").append(yearOfBirth);
        sb.append(", monthOfBirth='").append(monthOfBirth).append('\'');
        sb.append(", dayOfBirth=").append(dayOfBirth);
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
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", numSamples=").append(numSamples);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
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

    public String getLocationCity() {
        return locationCity;
    }

    public IndividualSolrModel setLocationCity(String locationCity) {
        this.locationCity = locationCity;
        return this;
    }

    public String getLocationState() {
        return locationState;
    }

    public IndividualSolrModel setLocationState(String locationState) {
        this.locationState = locationState;
        return this;
    }

    public String getLocationCountry() {
        return locationCountry;
    }

    public IndividualSolrModel setLocationCountry(String locationCountry) {
        this.locationCountry = locationCountry;
        return this;
    }

    public int getYearOfBirth() {
        return yearOfBirth;
    }

    public IndividualSolrModel setYearOfBirth(int yearOfBirth) {
        this.yearOfBirth = yearOfBirth;
        return this;
    }

    public String getMonthOfBirth() {
        return monthOfBirth;
    }

    public IndividualSolrModel setMonthOfBirth(String monthOfBirth) {
        this.monthOfBirth = monthOfBirth;
        return this;
    }

    public int getDayOfBirth() {
        return dayOfBirth;
    }

    public IndividualSolrModel setDayOfBirth(int dayOfBirth) {
        this.dayOfBirth = dayOfBirth;
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

    public int getVersion() {
        return version;
    }

    public IndividualSolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getLifeStatus() {
        return lifeStatus;
    }

    public IndividualSolrModel setLifeStatus(String lifeStatus) {
        this.lifeStatus = lifeStatus;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public IndividualSolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getDisorders() {
        return disorders;
    }

    public IndividualSolrModel setDisorders(List<String> disorders) {
        this.disorders = disorders;
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
