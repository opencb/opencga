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

import org.apache.commons.lang3.ObjectUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty.KaryotypicSex;
import org.opencb.biodata.models.pedigree.IndividualProperty.LifeStatus;
import org.opencb.biodata.models.pedigree.IndividualProperty.Sex;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Individual extends Annotable {

    private String id;
    private String name;
    private String uuid;

    private Individual father;
    private Individual mother;
    private Location location;

    private IndividualQualityControl qualityControl;

    private Sex sex;
    private KaryotypicSex karyotypicSex;
    private String ethnicity;
    private IndividualPopulation population;
    private String dateOfBirth;
    private int release;
    private int version;
    private String creationDate;
    private String modificationDate;
    private LifeStatus lifeStatus;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private List<Sample> samples;
    private boolean parentalConsanguinity;

    private CustomStatus status;

    private IndividualInternal internal;
    private Map<String, Object> attributes;

    public Individual() {
    }

    public Individual(String id, String name, Sex sex, String ethnicity, IndividualPopulation population, int release,
                      List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this(id, name, new Individual(), new Individual(), new Location(), sex, null, ethnicity, population, "", release,
                1, TimeUtils.getTime(), LifeStatus.UNKNOWN, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                false, annotationSets, new CustomStatus(), new IndividualInternal(new Status()), attributes);
    }

    public Individual(String id, String name, Individual father, Individual mother, Location location, Sex sex,
                      KaryotypicSex karyotypicSex, String ethnicity, IndividualPopulation population, LifeStatus lifeStatus,
                      String dateOfBirth, List<Sample> samples, boolean parentalConsanguinity, int release,
                      List<AnnotationSet> annotationSets, List<Phenotype> phenotypeList, List<Disorder> disorders) {
        this(id, name, father, mother, location, sex, karyotypicSex, ethnicity, population, dateOfBirth, release, 1,
                TimeUtils.getTime(), lifeStatus, phenotypeList, disorders, samples, parentalConsanguinity,
                annotationSets, new CustomStatus(), new IndividualInternal(new Status()), Collections.emptyMap());
    }

    public Individual(String id, String name, Individual father, Individual mother, Location location, Sex sex,
                      KaryotypicSex karyotypicSex, String ethnicity, IndividualPopulation population, String dateOfBirth, int release,
                      int version, String creationDate, LifeStatus lifeStatus, List<Phenotype> phenotypes, List<Disorder> disorders,
                      List<Sample> samples, boolean parentalConsanguinity, List<AnnotationSet> annotationSets, CustomStatus status,
                      IndividualInternal internal, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = ObjectUtils.defaultIfNull(father, new Individual());
        this.mother = ObjectUtils.defaultIfNull(mother, new Individual());
        this.location = location;
        this.sex = sex;
        this.karyotypicSex = karyotypicSex;
        this.ethnicity = ethnicity;
        this.population = ObjectUtils.defaultIfNull(population, new IndividualPopulation());
        this.dateOfBirth = dateOfBirth;
        this.release = release;
        this.version = version;
        this.creationDate = ObjectUtils.defaultIfNull(creationDate, TimeUtils.getTime());
        this.lifeStatus = lifeStatus;
        this.phenotypes = ObjectUtils.defaultIfNull(phenotypes, new ArrayList<>());
        this.disorders = ObjectUtils.defaultIfNull(disorders, new ArrayList<>());
        this.samples = ObjectUtils.defaultIfNull(samples, new ArrayList<>());
        this.parentalConsanguinity = parentalConsanguinity;
        this.annotationSets = annotationSets;
        this.status = status;
        this.internal = internal;
        this.attributes = ObjectUtils.defaultIfNull(attributes, new HashMap<>());
    }

    public Individual(String id, String name, String uuid, Individual father, Individual mother, Location location,
                      IndividualQualityControl qualityControl, Sex sex, KaryotypicSex karyotypicSex, String ethnicity,
                      IndividualPopulation population, String dateOfBirth, int release, int version, String creationDate,
                      String modificationDate, LifeStatus lifeStatus, List<Phenotype> phenotypes, List<Disorder> disorders,
                      List<Sample> samples, boolean parentalConsanguinity, List<AnnotationSet> annotationSets, CustomStatus status,
                      IndividualInternal internal, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = ObjectUtils.defaultIfNull(father, new Individual());
        this.mother = ObjectUtils.defaultIfNull(mother, new Individual());
        this.location = location;
        this.qualityControl = qualityControl;
        this.sex = sex;
        this.karyotypicSex = karyotypicSex;
        this.ethnicity = ethnicity;
        this.population = ObjectUtils.defaultIfNull(population, new IndividualPopulation());
        this.dateOfBirth = dateOfBirth;
        this.release = release;
        this.version = version;
        this.creationDate = ObjectUtils.defaultIfNull(creationDate, TimeUtils.getTime());
        this.lifeStatus = lifeStatus;
        this.phenotypes = ObjectUtils.defaultIfNull(phenotypes, new ArrayList<>());
        this.disorders = ObjectUtils.defaultIfNull(disorders, new ArrayList<>());
        this.samples = ObjectUtils.defaultIfNull(samples, new ArrayList<>());
        this.parentalConsanguinity = parentalConsanguinity;
        this.annotationSets = annotationSets;
        this.status = status;
        this.internal = internal;
        this.attributes = ObjectUtils.defaultIfNull(attributes, new HashMap<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Individual{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", location=").append(location);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", sex=").append(sex);
        sb.append(", karyotypicSex=").append(karyotypicSex);
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", population=").append(population);
        sb.append(", dateOfBirth='").append(dateOfBirth).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", lifeStatus=").append(lifeStatus);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", samples=").append(samples);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Individual)){
            return false;
        }

        Individual that = (Individual) o;
        return release == that.release
                && version == that.version
                && parentalConsanguinity == that.parentalConsanguinity
                && Objects.equals(uuid, that.uuid)
                && Objects.equals(id, that.id)
                && Objects.equals(name, that.name)
                && Objects.equals(father, that.father)
                && Objects.equals(mother, that.mother)
                && sex == that.sex
                && karyotypicSex == that.karyotypicSex
                && Objects.equals(ethnicity, that.ethnicity)
                && Objects.equals(population, that.population)
                && Objects.equals(dateOfBirth, that.dateOfBirth)
                && Objects.equals(creationDate, that.creationDate)
                && lifeStatus == that.lifeStatus
                && Objects.equals(phenotypes, that.phenotypes)
                && Objects.equals(samples, that.samples)
                && Objects.equals(status, that.status)
                && Objects.equals(internal, that.internal)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, id, name, father, mother, sex, karyotypicSex, ethnicity, population, dateOfBirth, release,
                version, creationDate, lifeStatus, internal, phenotypes, samples, parentalConsanguinity, status, attributes);
    }

    @Override
    public Individual setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public Individual setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Individual setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Individual setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Individual setName(String name) {
        this.name = name;
        return this;
    }

    public Individual getFather() {
        return father;
    }

    public Individual setFather(Individual father) {
        this.father = father;
        return this;
    }

    public Individual getMother() {
        return mother;
    }

    public Individual setMother(Individual mother) {
        this.mother = mother;
        return this;
    }

    public Location getLocation() {
        return location;
    }

    public Individual setLocation(Location location) {
        this.location = location;
        return this;
    }

    public IndividualQualityControl getQualityControl() {
        return qualityControl;
    }

    public Individual setQualityControl(IndividualQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public Sex getSex() {
        return sex;
    }

    public Individual setSex(Sex sex) {
        this.sex = sex;
        return this;
    }

    public KaryotypicSex getKaryotypicSex() {
        return karyotypicSex;
    }

    public Individual setKaryotypicSex(KaryotypicSex karyotypicSex) {
        this.karyotypicSex = karyotypicSex;
        return this;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public Individual setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
        return this;
    }

    public IndividualPopulation getPopulation() {
        return population;
    }

    public Individual setPopulation(IndividualPopulation population) {
        this.population = population;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Individual setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Individual setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public Individual setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
        return this;
    }

    public IndividualInternal getInternal() {
        return internal;
    }

    public Individual setInternal(IndividualInternal internal) {
        this.internal = internal;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Individual setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Individual setVersion(int version) {
        this.version = version;
        return this;
    }

    public LifeStatus getLifeStatus() {
        return lifeStatus;
    }

    public Individual setLifeStatus(LifeStatus lifeStatus) {
        this.lifeStatus = lifeStatus;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public Individual setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public Individual setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public Individual setSamples(List<Sample> samples) {
        this.samples = samples;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public Individual setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public CustomStatus getStatus() {
        return status;
    }

    public Individual setStatus(CustomStatus status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Individual setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
