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

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.IndividualAcl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
/**
 * Created by jacobo on 11/09/14.
 */
public class Individual {

    private long id;
    private String name;
    private long fatherId;
    private long motherId;
    private String family;
    private Gender gender;
    private String race;
    private Species species;
    private Population population;
    private Status status;
    private List<IndividualAcl> acls;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    public Individual() {
    }

    public Individual(long id, String name, long fatherId, long motherId, String family, Gender gender, String race, Species species,
                      Population population, List<AnnotationSet> annotationSets, Map<String, Object> attributes) throws CatalogException {
        this.id = id;
        this.name = name;
        this.fatherId = fatherId;
        this.motherId = motherId;
        this.family = family;
        this.gender = gender;
        this.race = race;
        this.species = species;
        this.population = population;
        this.annotationSets = annotationSets;
        this.acls = new ArrayList<>();
        this.status = new Status();
        this.attributes = attributes;
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
        sb.append(", status=").append(status);
        sb.append(", acls=").append(acls);
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

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Individual setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Individual setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<IndividualAcl> getAcls() {
        return acls;
    }

    public Individual setAcls(List<IndividualAcl> acls) {
        this.acls = acls;
        return this;
    }

    public enum Gender {
        MALE, FEMALE, UNKNOWN
    }

    // internal class
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
}
