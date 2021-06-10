/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.models.individual;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class IndividualUpdateParams {

    private String id;
    private String name;

    private String father;
    private String mother;
    private Boolean parentalConsanguinity;
    private Location location;
    private IndividualProperty.Sex sex;
    private String ethnicity;
    private IndividualPopulation population;
    private String dateOfBirth;
    private IndividualProperty.KaryotypicSex karyotypicSex;
    private IndividualProperty.LifeStatus lifeStatus;
    private IndividualProperty.AffectationStatus affectationStatus;
    private List<SampleReferenceParam> samples;
    private List<AnnotationSet> annotationSets;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private CustomStatusParams status;
    private IndividualQualityControl qualityControl;
    private Map<String, Object> attributes;

    public IndividualUpdateParams() {
    }

    public IndividualUpdateParams(String id, String name, String father, String mother, Boolean parentalConsanguinity,
                                  Location location, IndividualProperty.Sex sex, String ethnicity, IndividualPopulation population,
                                  String dateOfBirth, IndividualProperty.KaryotypicSex karyotypicSex,
                                  IndividualProperty.LifeStatus lifeStatus, IndividualProperty.AffectationStatus affectationStatus,
                                  List<SampleReferenceParam> samples, List<AnnotationSet> annotationSets, List<Phenotype> phenotypes,
                                  List<Disorder> disorders, CustomStatusParams status, IndividualQualityControl qualityControl,
                                  Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = father;
        this.mother = mother;
        this.parentalConsanguinity = parentalConsanguinity;
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
        this.status = status;
        this.qualityControl = qualityControl;
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

    @JsonIgnore
    public Individual toIndividual() {
        return new Individual(id, name,
                StringUtils.isNotEmpty(father) ? new Individual().setId(father) : null,
                StringUtils.isNotEmpty(mother) ? new Individual().setId(mother) : null,
                location, qualityControl, sex, karyotypicSex, ethnicity, population, dateOfBirth, 1, 1, TimeUtils.getTime(), lifeStatus,
                phenotypes, disorders,
                samples != null
                        ? samples.stream().map(s -> new Sample().setId(s.getId()).setUuid(s.getUuid())).collect(Collectors.toList())
                        : null,
                parentalConsanguinity, annotationSets, status.toCustomStatus(), new IndividualInternal(), attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", father='").append(father).append('\'');
        sb.append(", mother='").append(mother).append('\'');
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
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
        sb.append(", status=").append(status);
        sb.append(", qualityControl=").append(qualityControl);
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

    public List<SampleReferenceParam> getSamples() {
        return samples;
    }

    public IndividualUpdateParams setSamples(List<SampleReferenceParam> samples) {
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

    public CustomStatusParams getStatus() {
        return status;
    }

    public IndividualUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public IndividualQualityControl getQualityControl() {
        return qualityControl;
    }

    public IndividualUpdateParams setQualityControl(IndividualQualityControl qualityControl) {
        this.qualityControl = qualityControl;
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
