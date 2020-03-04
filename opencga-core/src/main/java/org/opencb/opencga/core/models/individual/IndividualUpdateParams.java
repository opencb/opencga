package org.opencb.opencga.core.models.individual;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class IndividualUpdateParams {

    private String id;
    private String name;

    private String father;
    private String mother;
    private Boolean parentalConsanguinity;
    private Multiples multiples;
    private Location location;
    private IndividualProperty.Sex sex;
    private String ethnicity;
    private IndividualPopulation population;
    private String dateOfBirth;
    private IndividualProperty.KaryotypicSex karyotypicSex;
    private IndividualProperty.LifeStatus lifeStatus;
    private IndividualProperty.AffectationStatus affectationStatus;
    private List<String> samples;
    private List<AnnotationSet> annotationSets;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private Map<String, Object> attributes;

    public IndividualUpdateParams() {
    }

    public IndividualUpdateParams(String id, String name, String father, String mother, Boolean parentalConsanguinity, Multiples multiples,
                                  Location location, IndividualProperty.Sex sex, String ethnicity, IndividualPopulation population,
                                  String dateOfBirth, IndividualProperty.KaryotypicSex karyotypicSex,
                                  IndividualProperty.LifeStatus lifeStatus, IndividualProperty.AffectationStatus affectationStatus,
                                  List<String> samples, List<AnnotationSet> annotationSets, List<Phenotype> phenotypes,
                                  List<Disorder> disorders, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = father;
        this.mother = mother;
        this.parentalConsanguinity = parentalConsanguinity;
        this.multiples = multiples;
        this.location = location;
        this.sex = sex;
        this.ethnicity = ethnicity;
        this.population = population;
        this.dateOfBirth = dateOfBirth;
        this.karyotypicSex = karyotypicSex;
        this.lifeStatus = lifeStatus;
        this.affectationStatus = affectationStatus;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.attributes = attributes;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        List<AnnotationSet> annotationSetList = this.annotationSets;
        this.annotationSets = null;

        ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));

        this.annotationSets = annotationSetList;
        if (this.annotationSets != null) {
            // We leave annotation sets as is so we don't need to make any more castings
            params.put("annotationSets", this.annotationSets);
        }

        return params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", father='").append(father).append('\'');
        sb.append(", mother='").append(mother).append('\'');
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", multiples=").append(multiples);
        sb.append(", location=").append(location);
        sb.append(", sex=").append(sex);
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", population=").append(population);
        sb.append(", dateOfBirth='").append(dateOfBirth).append('\'');
        sb.append(", karyotypicSex=").append(karyotypicSex);
        sb.append(", lifeStatus=").append(lifeStatus);
        sb.append(", affectationStatus=").append(affectationStatus);
        sb.append(", samples=").append(samples);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public IndividualUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public IndividualUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getFather() {
        return father;
    }

    public IndividualUpdateParams setFather(String father) {
        this.father = father;
        return this;
    }

    public String getMother() {
        return mother;
    }

    public IndividualUpdateParams setMother(String mother) {
        this.mother = mother;
        return this;
    }

    public Boolean getParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public IndividualUpdateParams setParentalConsanguinity(Boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public Multiples getMultiples() {
        return multiples;
    }

    public IndividualUpdateParams setMultiples(Multiples multiples) {
        this.multiples = multiples;
        return this;
    }

    public Location getLocation() {
        return location;
    }

    public IndividualUpdateParams setLocation(Location location) {
        this.location = location;
        return this;
    }

    public IndividualProperty.Sex getSex() {
        return sex;
    }

    public IndividualUpdateParams setSex(IndividualProperty.Sex sex) {
        this.sex = sex;
        return this;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public IndividualUpdateParams setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
        return this;
    }

    public IndividualPopulation getPopulation() {
        return population;
    }

    public IndividualUpdateParams setPopulation(IndividualPopulation population) {
        this.population = population;
        return this;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public IndividualUpdateParams setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
        return this;
    }

    public IndividualProperty.KaryotypicSex getKaryotypicSex() {
        return karyotypicSex;
    }

    public IndividualUpdateParams setKaryotypicSex(IndividualProperty.KaryotypicSex karyotypicSex) {
        this.karyotypicSex = karyotypicSex;
        return this;
    }

    public IndividualProperty.LifeStatus getLifeStatus() {
        return lifeStatus;
    }

    public IndividualUpdateParams setLifeStatus(IndividualProperty.LifeStatus lifeStatus) {
        this.lifeStatus = lifeStatus;
        return this;
    }

    public IndividualProperty.AffectationStatus getAffectationStatus() {
        return affectationStatus;
    }

    public IndividualUpdateParams setAffectationStatus(IndividualProperty.AffectationStatus affectationStatus) {
        this.affectationStatus = affectationStatus;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public IndividualUpdateParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public IndividualUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public IndividualUpdateParams setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public IndividualUpdateParams setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public IndividualUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
