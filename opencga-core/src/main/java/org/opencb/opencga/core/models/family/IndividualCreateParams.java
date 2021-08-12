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

package org.opencb.opencga.core.models.family;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPopulation;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;
import org.opencb.opencga.core.models.individual.Location;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IndividualCreateParams {

    private String id;
    private String name;

    private String creationDate;
    private String modificationDate;
    private IndividualReferenceParam father;
    private IndividualReferenceParam mother;
    private Location location;

    private IndividualProperty.Sex sex;
    private String ethnicity;
    private Boolean parentalConsanguinity;
    private IndividualPopulation population;
    private String dateOfBirth;
    private IndividualProperty.KaryotypicSex karyotypicSex;
    private IndividualProperty.LifeStatus lifeStatus;
    private List<AnnotationSet> annotationSets;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private CustomStatusParams status;
    private Map<String, Object> attributes;

    public IndividualCreateParams() {
    }

    public IndividualCreateParams(String id, String name, String creationDate, String modificationDate, IndividualReferenceParam father,
                                  IndividualReferenceParam mother, Location location, IndividualProperty.Sex sex, String ethnicity,
                                  Boolean parentalConsanguinity, IndividualPopulation population, String dateOfBirth,
                                  IndividualProperty.KaryotypicSex karyotypicSex, IndividualProperty.LifeStatus lifeStatus,
                                  List<AnnotationSet> annotationSets, List<Phenotype> phenotypes, List<Disorder> disorders,
                                  CustomStatusParams status, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.father = father;
        this.mother = mother;
        this.location = location;
        this.sex = sex;
        this.ethnicity = ethnicity;
        this.parentalConsanguinity = parentalConsanguinity;
        this.population = population;
        this.dateOfBirth = dateOfBirth;
        this.karyotypicSex = karyotypicSex;
        this.lifeStatus = lifeStatus;
        this.annotationSets = annotationSets;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.status = status;
        this.attributes = attributes;
    }

    public static IndividualCreateParams of(Individual individual) {
        return new IndividualCreateParams(individual.getId(), individual.getName(), individual.getCreationDate(),
                individual.getModificationDate(),
                individual.getFather() != null
                        ? new IndividualReferenceParam(individual.getFather().getId(), individual.getFather().getUuid())
                        : null,
                individual.getMother() != null
                        ? new IndividualReferenceParam(individual.getMother().getId(), individual.getMother().getUuid())
                        : null,
                individual.getLocation(),
                individual.getSex(), individual.getEthnicity(), individual.isParentalConsanguinity(), individual.getPopulation(),
                individual.getDateOfBirth(), individual.getKaryotypicSex(), individual.getLifeStatus(),
                individual.getAnnotationSets(), individual.getPhenotypes(), individual.getDisorders(),
                CustomStatusParams.of(individual.getStatus()), individual.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", location=").append(location);
        sb.append(", sex=").append(sex);
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", population=").append(population);
        sb.append(", dateOfBirth='").append(dateOfBirth).append('\'');
        sb.append(", karyotypicSex=").append(karyotypicSex);
        sb.append(", lifeStatus=").append(lifeStatus);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", status=").append(status);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Individual toIndividual() {
        String individualId = StringUtils.isEmpty(id) ? name : id;
        String individualName = StringUtils.isEmpty(name) ? individualId : name;
        Individual father = this.father != null
                ? new Individual().setId(this.father.getId()).setUuid(this.father.getUuid())
                : null;
        Individual mother = this.mother != null
                ? new Individual().setId(this.mother.getId()).setUuid(this.mother.getUuid())
                : null;
        return new Individual(individualId, individualName, father, mother, Collections.emptyList(), location, null, sex,
                karyotypicSex, ethnicity, population, dateOfBirth, 1, 1, creationDate, modificationDate, lifeStatus, phenotypes, disorders,
                Collections.emptyList(), parentalConsanguinity != null ? parentalConsanguinity : false,
                annotationSets, status != null ? status.toCustomStatus() : new CustomStatus(), null, attributes);
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

    public IndividualReferenceParam getFather() {
        return father;
    }

    public IndividualCreateParams setFather(IndividualReferenceParam father) {
        this.father = father;
        return this;
    }

    public IndividualReferenceParam getMother() {
        return mother;
    }

    public IndividualCreateParams setMother(IndividualReferenceParam mother) {
        this.mother = mother;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public IndividualCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public IndividualCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Location getLocation() {
        return location;
    }

    public IndividualCreateParams setLocation(Location location) {
        this.location = location;
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

    public IndividualPopulation getPopulation() {
        return population;
    }

    public IndividualCreateParams setPopulation(IndividualPopulation population) {
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

    public CustomStatusParams getStatus() {
        return status;
    }

    public IndividualCreateParams setStatus(CustomStatusParams status) {
        this.status = status;
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
