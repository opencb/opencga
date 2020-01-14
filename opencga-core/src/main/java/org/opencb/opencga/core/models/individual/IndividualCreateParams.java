package org.opencb.opencga.core.models.individual;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndividualCreateParams {

    private String id;
    private String name;

    private String father;
    private String mother;
    private Multiples multiples;
    private Location location;
    private List<SampleCreateParams> samples;
    private IndividualProperty.Sex sex;
    private String ethnicity;
    private Boolean parentalConsanguinity;
    private Individual.Population population;
    private String dateOfBirth;
    private IndividualProperty.KaryotypicSex karyotypicSex;
    private IndividualProperty.LifeStatus lifeStatus;
    private IndividualProperty.AffectationStatus affectationStatus;
    private List<AnnotationSet> annotationSets;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private Map<String, Object> attributes;

    public IndividualCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", father='").append(father).append('\'');
        sb.append(", mother='").append(mother).append('\'');
        sb.append(", multiples=").append(multiples);
        sb.append(", location=").append(location);
        sb.append(", samples=").append(samples);
        sb.append(", sex=").append(sex);
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", population=").append(population);
        sb.append(", dateOfBirth='").append(dateOfBirth).append('\'');
        sb.append(", karyotypicSex=").append(karyotypicSex);
        sb.append(", lifeStatus=").append(lifeStatus);
        sb.append(", affectationStatus=").append(affectationStatus);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Individual toIndividual() {

        List<Sample> sampleList = null;
        if (samples != null) {
            sampleList = new ArrayList<>(samples.size());
            for (SampleCreateParams sample : samples) {
                sampleList.add(sample.toSample());
            }
        }

        String individualId = StringUtils.isEmpty(id) ? name : id;
        String individualName = StringUtils.isEmpty(name) ? individualId : name;
        return new Individual(individualId, individualName, new Individual().setId(father), new Individual().setId(mother), multiples,
                location, sex, karyotypicSex, ethnicity, population, lifeStatus, affectationStatus, dateOfBirth,
                sampleList, parentalConsanguinity != null ? parentalConsanguinity : false, 1, annotationSets, phenotypes, disorders)
                .setAttributes(attributes);
    }

    public String getId() {
        return id;
    }

    public IndividualCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public IndividualCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getFather() {
        return father;
    }

    public IndividualCreateParams setFather(String father) {
        this.father = father;
        return this;
    }

    public String getMother() {
        return mother;
    }

    public IndividualCreateParams setMother(String mother) {
        this.mother = mother;
        return this;
    }

    public Multiples getMultiples() {
        return multiples;
    }

    public IndividualCreateParams setMultiples(Multiples multiples) {
        this.multiples = multiples;
        return this;
    }

    public Location getLocation() {
        return location;
    }

    public IndividualCreateParams setLocation(Location location) {
        this.location = location;
        return this;
    }

    public List<SampleCreateParams> getSamples() {
        return samples;
    }

    public IndividualCreateParams setSamples(List<SampleCreateParams> samples) {
        this.samples = samples;
        return this;
    }

    public IndividualProperty.Sex getSex() {
        return sex;
    }

    public IndividualCreateParams setSex(IndividualProperty.Sex sex) {
        this.sex = sex;
        return this;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public IndividualCreateParams setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
        return this;
    }

    public Boolean getParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public IndividualCreateParams setParentalConsanguinity(Boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public Individual.Population getPopulation() {
        return population;
    }

    public IndividualCreateParams setPopulation(Individual.Population population) {
        this.population = population;
        return this;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public IndividualCreateParams setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
        return this;
    }

    public IndividualProperty.KaryotypicSex getKaryotypicSex() {
        return karyotypicSex;
    }

    public IndividualCreateParams setKaryotypicSex(IndividualProperty.KaryotypicSex karyotypicSex) {
        this.karyotypicSex = karyotypicSex;
        return this;
    }

    public IndividualProperty.LifeStatus getLifeStatus() {
        return lifeStatus;
    }

    public IndividualCreateParams setLifeStatus(IndividualProperty.LifeStatus lifeStatus) {
        this.lifeStatus = lifeStatus;
        return this;
    }

    public IndividualProperty.AffectationStatus getAffectationStatus() {
        return affectationStatus;
    }

    public IndividualCreateParams setAffectationStatus(IndividualProperty.AffectationStatus affectationStatus) {
        this.affectationStatus = affectationStatus;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public IndividualCreateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public IndividualCreateParams setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public IndividualCreateParams setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public IndividualCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
