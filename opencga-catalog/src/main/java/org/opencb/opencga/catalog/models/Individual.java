/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.IndividualAclEntry;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;

/**
 * Created by jacobo on 11/09/14.
 */
public class Individual extends Annotable {

    private long id;
    private String name;
    private long fatherId;
    private long motherId;
    private String family;
    private Gender gender;
    private SexualKaryotype sexualKaryotype;
    private String race;
    private Species species;
    private Population population;
    private String creationDate;
    private Status status;
    private LifeStatus lifeStatus;
    private AffectationStatus affectationStatus;
    private List<OntologyTerm> ontologyTerms;

    private List<IndividualAclEntry> acl;
//    private List<AnnotationSet> annotationSets;

    private Map<String, Object> attributes;

    public enum Gender {
        MALE, FEMALE, UNKNOWN
    }

    public enum LifeStatus {
        ALIVE, ABORTED, DECEASED, UNBORN, STILLBORN, MISCARRIAGE, UNKNOWN
    }

    public enum AffectationStatus {
        AFFECTED, UNAFFECTED, UNKNOWN
    }

    public enum SexualKaryotype {
        UNKNOWN, XX, XY, XO, XXY, XXX, XXYY, XXXY, XXXX, XYY, OTHER
    }

    public Individual() {
    }

    public Individual(long id, String name, long fatherId, long motherId, String family, Gender gender, String race, Species species,
                      Population population, List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this(id, name, fatherId, motherId, family, gender, race, species, population, new Status(), Collections.emptyList(), annotationSets,
                attributes);
    }

    public Individual(long id, String name, long fatherId, long motherId, String family, Gender gender, String race, Species species,
                      Population population, Status status, List<IndividualAclEntry> acl, List<AnnotationSet> annotationSets,
                      Map<String, Object> attributes) {
        this(id, name, fatherId, motherId, family, gender, SexualKaryotype.UNKNOWN, race, species, population, TimeUtils.getTime(), status,
                LifeStatus.UNKNOWN, AffectationStatus.UNKNOWN, Collections.emptyList(), acl, annotationSets, attributes);

        if (gender == null) {
            this.gender = Gender.UNKNOWN;
        }

        if (this.gender.equals(Gender.MALE)) {
            this.sexualKaryotype = SexualKaryotype.XY;
        } else if (this.gender.equals(Gender.FEMALE)) {
            this.sexualKaryotype = SexualKaryotype.XX;
        } else {
            this.sexualKaryotype = SexualKaryotype.UNKNOWN;
        }
    }

    public Individual(long id, String name, long fatherId, long motherId, String family, Gender gender, SexualKaryotype sexualKaryotype,
                      String race, Species species, Population population, String creationDate, Status status, LifeStatus lifeStatus,
                      AffectationStatus affectationStatus, List<OntologyTerm> ontologyTerms, List<IndividualAclEntry> acl,
                      List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.fatherId = fatherId;
        this.motherId = motherId;
        this.family = family;
        this.gender = gender;
        this.sexualKaryotype = sexualKaryotype;
        this.race = race;
        this.species = species;
        this.population = population;
        this.creationDate = creationDate;
        this.status = status;
        this.lifeStatus = lifeStatus;
        this.affectationStatus = affectationStatus;
        this.ontologyTerms = ontologyTerms;
        this.acl = acl;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Individual)) {
            return false;
        }

        Individual that = (Individual) o;

        if (id != that.id) {
            return false;
        }
        if (fatherId != that.fatherId) {
            return false;
        }
        if (motherId != that.motherId) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (family != null ? !family.equals(that.family) : that.family != null) {
            return false;
        }
        if (gender != that.gender) {
            return false;
        }
        if (race != null ? !race.equals(that.race) : that.race != null) {
            return false;
        }
        if (species != null ? !species.equals(that.species) : that.species != null) {
            return false;
        }
        if (population != null ? !population.equals(that.population) : that.population != null) {
            return false;
        }
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        long result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + fatherId;
        result = 31 * result + motherId;
        result = 31 * result + (family != null ? family.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (race != null ? race.hashCode() : 0);
        result = 31 * result + (species != null ? species.hashCode() : 0);
        result = 31 * result + (population != null ? population.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return toIntExact(result);
    }


    public static class Species {

        private String taxonomyCode;
        private String scientificName;
        private String commonName;


        public Species() {
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Species{");
            sb.append("taxonomyCode='").append(taxonomyCode).append('\'');
            sb.append(", scientificName='").append(scientificName).append('\'');
            sb.append(", commonName='").append(commonName).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Species)) {
                return false;
            }

            Species species = (Species) o;

            if (taxonomyCode != null ? !taxonomyCode.equals(species.taxonomyCode) : species.taxonomyCode != null) {
                return false;
            }
            if (scientificName != null ? !scientificName.equals(species.scientificName) : species.scientificName != null) {
                return false;
            }
            return !(commonName != null ? !commonName.equals(species.commonName) : species.commonName != null);

        }

        @Override
        public int hashCode() {
            int result = taxonomyCode != null ? taxonomyCode.hashCode() : 0;
            result = 31 * result + (scientificName != null ? scientificName.hashCode() : 0);
            result = 31 * result + (commonName != null ? commonName.hashCode() : 0);
            return result;
        }

        public String getTaxonomyCode() {
            return taxonomyCode;
        }

        public void setTaxonomyCode(String taxonomyCode) {
            this.taxonomyCode = taxonomyCode;
        }

        public String getScientificName() {
            return scientificName;
        }

        public void setScientificName(String scientificName) {
            this.scientificName = scientificName;
        }

        public String getCommonName() {
            return commonName;
        }

        public void setCommonName(String commonName) {
            this.commonName = commonName;
        }
    }


    public static class Population {

        private String name;
        private String subpopulation;
        private String description;


        public Population() {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Population)) {
                return false;
            }

            Population that = (Population) o;

            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            if (subpopulation != null ? !subpopulation.equals(that.subpopulation) : that.subpopulation != null) {
                return false;
            }
            return !(description != null ? !description.equals(that.description) : that.description != null);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (subpopulation != null ? subpopulation.hashCode() : 0);
            result = 31 * result + (description != null ? description.hashCode() : 0);
            return result;
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
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", fatherId=").append(fatherId);
        sb.append(", motherId=").append(motherId);
        sb.append(", family='").append(family).append('\'');
        sb.append(", gender=").append(gender);
        sb.append(", race='").append(race).append('\'');
        sb.append(", species=").append(species);
        sb.append(", population=").append(population);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", ontologyTerms=").append(ontologyTerms);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Individual setId(long id) {
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

    public long getFatherId() {
        return fatherId;
    }

    public Individual setFatherId(long fatherId) {
        this.fatherId = fatherId;
        return this;
    }

    public long getMotherId() {
        return motherId;
    }

    public Individual setMotherId(long motherId) {
        this.motherId = motherId;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public Individual setFamily(String family) {
        this.family = family;
        return this;
    }

    public Gender getGender() {
        return gender;
    }

    public Individual setGender(Gender gender) {
        this.gender = gender;
        return this;
    }

    public SexualKaryotype getSexualKaryotype() {
        return sexualKaryotype;
    }

    public Individual setSexualKaryotype(SexualKaryotype sexualKaryotype) {
        this.sexualKaryotype = sexualKaryotype;
        return this;
    }

    public String getRace() {
        return race;
    }

    public Individual setRace(String race) {
        this.race = race;
        return this;
    }

    public Species getSpecies() {
        return species;
    }

    public Individual setSpecies(Species species) {
        this.species = species;
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

    public Status getStatus() {
        return status;
    }

    public Individual setStatus(Status status) {
        this.status = status;
        return this;
    }


    public LifeStatus getLifeStatus() {
        return lifeStatus;
    }

    public Individual setLifeStatus(LifeStatus lifeStatus) {
        this.lifeStatus = lifeStatus;
        return this;
    }

    public AffectationStatus getAffectationStatus() {
        return affectationStatus;
    }

    public Individual setAffectationStatus(AffectationStatus affectationStatus) {
        this.affectationStatus = affectationStatus;
        return this;
    }

    public List<OntologyTerm> getOntologyTerms() {
        return ontologyTerms;
    }

    public Individual setOntologyTerms(List<OntologyTerm> ontologyTerms) {
        this.ontologyTerms = ontologyTerms;
        return this;
    }

    public List<IndividualAclEntry> getAcl() {
        return acl;
    }

    public Individual setAcl(List<IndividualAclEntry> acl) {
        this.acl = acl;
        return this;
    }

//    public List<AnnotationSet> getAnnotationSets() {
//        return annotationSets;
//    }
//
//    public Individual setAnnotationSets(List<AnnotationSet> annotationSets) {
//        this.annotationSets = annotationSets;
//        return this;
//    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Individual setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
