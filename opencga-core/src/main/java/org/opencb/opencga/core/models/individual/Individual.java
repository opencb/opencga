/*
 * Copyright 2015-2017 OpenCB
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

import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty.AffectationStatus;
import org.opencb.biodata.models.pedigree.IndividualProperty.KaryotypicSex;
import org.opencb.biodata.models.pedigree.IndividualProperty.LifeStatus;
import org.opencb.biodata.models.pedigree.IndividualProperty.Sex;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;

import static org.opencb.opencga.core.common.FieldUtils.defaultObject;

/**
 * Created by jacobo on 11/09/14.
 */
public class Individual extends Annotable {

    private String id;
    private String name;
    private String uuid;

    private Individual father;
    private Individual mother;
    private Multiples multiples;
    private Location location;

    private Sex sex;
    private KaryotypicSex karyotypicSex;
    private String ethnicity;
    private Population population;
    private String dateOfBirth;
    private int release;
    private int version;
    private String creationDate;
    private String modificationDate;
    private Status status;
    private LifeStatus lifeStatus;
    @Deprecated
    private AffectationStatus affectationStatus;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private List<Sample> samples;
    private boolean parentalConsanguinity;

//    private List<AnnotationSet> annotationSets;

    private Map<String, Object> attributes;

    public Individual() {
    }

    public Individual(String id, String name, Sex sex, String ethnicity, Population population, int release,
                      List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this(id, name, new Individual(), new Individual(), new Multiples(), new Location(), sex, null,
                ethnicity, population, "", release, 1, TimeUtils.getTime(), new Status(), LifeStatus.UNKNOWN,
                AffectationStatus.UNKNOWN, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, annotationSets,
                attributes);
    }

    public Individual(String id, String name, Individual father, Individual mother, Multiples multiples, Location location, Sex sex,
                      KaryotypicSex karyotypicSex, String ethnicity, Population population, LifeStatus lifeStatus,
                      AffectationStatus affectationStatus, String dateOfBirth, List<Sample> samples, boolean parentalConsanguinity,
                      int release, List<AnnotationSet> annotationSets, List<Phenotype> phenotypeList, List<Disorder> disorders) {
        this(id, name, father, mother, multiples, location, sex, karyotypicSex, ethnicity, population,
                dateOfBirth, release, 1, TimeUtils.getTime(), new Status(), lifeStatus, affectationStatus, phenotypeList, disorders,
                samples, parentalConsanguinity, annotationSets, Collections.emptyMap());
    }

    public Individual(String id, String name, Individual father, Individual mother, Multiples multiples, Location location, Sex sex,
                      KaryotypicSex karyotypicSex, String ethnicity, Population population, String dateOfBirth, int release, int version,
                      String creationDate, Status status, LifeStatus lifeStatus, AffectationStatus affectationStatus,
                      List<Phenotype> phenotypes, List<Disorder> disorders, List<Sample> samples, boolean parentalConsanguinity,
                      List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = defaultObject(father, Individual::new);
        this.mother = defaultObject(mother, Individual::new);
        this.multiples = defaultObject(multiples, Multiples::new);
        this.location = location;
        this.sex = sex;
        this.karyotypicSex = karyotypicSex;
        this.ethnicity = ethnicity;
        this.population = defaultObject(population, Population::new);
        this.dateOfBirth = dateOfBirth;
        this.release = release;
        this.version = version;
        this.creationDate = defaultObject(creationDate, TimeUtils::getTime);
        this.status = defaultObject(status, Status::new);
        this.lifeStatus = lifeStatus;
        this.affectationStatus = affectationStatus;
        this.phenotypes = defaultObject(phenotypes, ArrayList::new);
        this.disorders = defaultObject(disorders, ArrayList::new);
        this.samples = defaultObject(samples, ArrayList::new);
        this.parentalConsanguinity = parentalConsanguinity;
        this.annotationSets = annotationSets;
        this.attributes = defaultObject(attributes, HashMap::new);
    }

    public static class Population {

        private String name;
        private String subpopulation;
        private String description;


        public Population() {
            this.name = "";
            this.subpopulation = "";
            this.description = "";
        }

        public Population(String name, String subpopulation, String description) {
            this.name = name;
            this.subpopulation = subpopulation;
            this.description = description;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Population{");
            sb.append("name='").append(name).append('\'');
            sb.append(", subpopulation='").append(subpopulation).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSubpopulation() {
            return subpopulation;
        }

        public void setSubpopulation(String subpopulation) {
            this.subpopulation = subpopulation;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Individual{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", multiples=").append(multiples);
        sb.append(", location=").append(location);
        sb.append(", sex=").append(sex);
        sb.append(", karyotypicSex=").append(karyotypicSex);
        sb.append(", ethnicity='").append(ethnicity).append('\'');
        sb.append(", population=").append(population);
        sb.append(", dateOfBirth='").append(dateOfBirth).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", lifeStatus=").append(lifeStatus);
        sb.append(", affectationStatus=").append(affectationStatus);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", samples=").append(samples);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
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
                && Objects.equals(multiples, that.multiples)
                && sex == that.sex
                && karyotypicSex == that.karyotypicSex
                && Objects.equals(ethnicity, that.ethnicity)
                && Objects.equals(population, that.population)
                && Objects.equals(dateOfBirth, that.dateOfBirth)
                && Objects.equals(creationDate, that.creationDate)
                && Objects.equals(status, that.status)
                && lifeStatus == that.lifeStatus
                && affectationStatus == that.affectationStatus
                && Objects.equals(phenotypes, that.phenotypes)
                && Objects.equals(samples, that.samples)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, id, name, father, mother, multiples, sex, karyotypicSex, ethnicity, population, dateOfBirth, release,
                version, creationDate, status, lifeStatus, affectationStatus, phenotypes, samples, parentalConsanguinity, attributes);
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

    public Multiples getMultiples() {
        return multiples;
    }

    public Individual setMultiples(Multiples multiples) {
        this.multiples = multiples;
        return this;
    }

    public Location getLocation() {
        return location;
    }

    public Individual setLocation(Location location) {
        this.location = location;
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

    public Population getPopulation() {
        return population;
    }

    public Individual setPopulation(Population population) {
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

    public Status getStatus() {
        return status;
    }

    public Individual setStatus(Status status) {
        this.status = status;
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

    @Deprecated
    public AffectationStatus getAffectationStatus() {
        return affectationStatus;
    }

    @Deprecated
    public Individual setAffectationStatus(AffectationStatus affectationStatus) {
        this.affectationStatus = affectationStatus;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Individual setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    // Acl params to communicate the WS and the sample manager
    public static class IndividualAclParams extends AclParams {

        private String sample;
        private boolean propagate;

        public IndividualAclParams() {

        }

        public IndividualAclParams(String permissions, Action action, String sample, boolean propagate) {
            super(permissions, action);
            this.sample = sample;
            this.propagate = propagate;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("IndividualAclParams{");
            sb.append("permissions='").append(permissions).append('\'');
            sb.append(", action=").append(action);
            sb.append(", sample='").append(sample).append('\'');
            sb.append(", propagate=").append(propagate);
            sb.append('}');
            return sb.toString();
        }

        public String getSample() {
            return sample;
        }

        public IndividualAclParams setSample(String sample) {
            this.sample = sample;
            return this;
        }

        public boolean isPropagate() {
            return propagate;
        }

        public IndividualAclParams setPropagate(boolean propagate) {
            this.propagate = propagate;
            return this;
        }
    }

}
