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
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class IndividualUpdateParams {

    private String id;
    private String name;

    private IndividualReferenceParam father;
    private IndividualReferenceParam mother;
    private String creationDate;
    private String modificationDate;
    private Boolean parentalConsanguinity;
    private Location location;
    private SexOntologyTermAnnotation sex;
    private OntologyTermAnnotation ethnicity;
    private IndividualPopulation population;
    private String dateOfBirth;
    private IndividualProperty.KaryotypicSex karyotypicSex;
    private IndividualProperty.LifeStatus lifeStatus;
    private List<SampleReferenceParam> samples;
    private List<AnnotationSet> annotationSets;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private StatusParams status;
    private Map<String, Object> attributes;

    private static final String QUALITY_CONTROL_STATUS_KEY = "qualityControlStatus";
    private static final String INTERNAL_QUALITY_CONTROL_STATUS_KEY = "internal.qualityControlStatus";

    public IndividualUpdateParams() {
    }

    public IndividualUpdateParams(String id, String name, IndividualReferenceParam father, IndividualReferenceParam mother,
                                  String creationDate, String modificationDate, Boolean parentalConsanguinity, Location location,
                                  SexOntologyTermAnnotation sex, OntologyTermAnnotation ethnicity, IndividualPopulation population,
                                  String dateOfBirth, IndividualProperty.KaryotypicSex karyotypicSex,
                                  IndividualProperty.LifeStatus lifeStatus, List<SampleReferenceParam> samples,
                                  List<AnnotationSet> annotationSets, List<Phenotype> phenotypes, List<Disorder> disorders,
                                  StatusParams status, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = father;
        this.mother = mother;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.parentalConsanguinity = parentalConsanguinity;
        this.location = location;
        this.sex = sex;
        this.ethnicity = ethnicity;
        this.population = population;
        this.dateOfBirth = dateOfBirth;
        this.karyotypicSex = karyotypicSex;
        this.lifeStatus = lifeStatus;
        this.samples = samples;
        this.annotationSets = annotationSets;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.status = status;
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
                father != null ? new Individual().setId(father.getId()).setUuid(father.getUuid()) : null,
                mother != null ? new Individual().setId(mother.getId()).setUuid(mother.getUuid()) : null,
                Collections.emptyList(), location, null, sex, karyotypicSex, ethnicity, population, dateOfBirth, 1, 1,
                creationDate, modificationDate, lifeStatus, phenotypes, disorders,
                samples != null
                        ? samples.stream().map(s -> new Sample().setId(s.getId()).setUuid(s.getUuid())).collect(Collectors.toList())
                        : null, parentalConsanguinity != null && parentalConsanguinity, annotationSets,
                status != null ? status.toStatus() : null, new IndividualInternal(),
                attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", location=").append(location);
        sb.append(", sex=").append(sex);
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", population=").append(population);
        sb.append(", dateOfBirth='").append(dateOfBirth).append('\'');
        sb.append(", karyotypicSex=").append(karyotypicSex);
        sb.append(", lifeStatus=").append(lifeStatus);
        sb.append(", samples=").append(samples);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", status=").append(status);
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

    public IndividualReferenceParam getFather() {
        return father;
    }

    public IndividualUpdateParams setFather(IndividualReferenceParam father) {
        this.father = father;
        return this;
    }

    public IndividualReferenceParam getMother() {
        return mother;
    }

    public IndividualUpdateParams setMother(IndividualReferenceParam mother) {
        this.mother = mother;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public IndividualUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public IndividualUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
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

    public SexOntologyTermAnnotation getSex() {
        return sex;
    }

    public IndividualUpdateParams setSex(SexOntologyTermAnnotation sex) {
        this.sex = sex;
        return this;
    }

    public OntologyTermAnnotation getEthnicity() {
        return ethnicity;
    }

    public IndividualUpdateParams setEthnicity(OntologyTermAnnotation ethnicity) {
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

    public StatusParams getStatus() {
        return status;
    }

    public IndividualUpdateParams setStatus(StatusParams status) {
        this.status = status;
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
