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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Individual {

    public enum Gender {
        MALE, FEMALE, UNKNOWN
    }

    private int id;
    private String name;
    private int father;
    private int mother;
    private String family;
    private Gender gender;


    private String race;
    private Species species;
    private Population population;

    // internal class
    public static class Species {
        private String taxonomyCode;
        private String scientificName;
        private String commonName;

        public Species() {
        }

        @Override
        public String toString() {
            return "Species {" +
                    "\"taxonomyCode\": " + '\"' + taxonomyCode + '\"' +
                    ", \"scientificName\": " + '\"' + scientificName + '\"' +
                    ", \"commonName\": " + '\"' + commonName + '\"' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Species)) return false;

            Species species = (Species) o;

            if (taxonomyCode != null ? !taxonomyCode.equals(species.taxonomyCode) : species.taxonomyCode != null)
                return false;
            if (scientificName != null ? !scientificName.equals(species.scientificName) : species.scientificName != null)
                return false;
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
            return "Population {" +
                    "\"name\": " + '\"' + name + '\"' +
                    ", \"subpopulation\": " + '\"' + subpopulation + '\"' +
                    ", \"description\": " + '\"' + description + '\"' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Population)) return false;

            Population that = (Population) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            if (subpopulation != null ? !subpopulation.equals(that.subpopulation) : that.subpopulation != null)
                return false;
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

    private Map<String, Object> attributes;

    public Individual() {
    }

    public Individual(int id, String name, int father, int mother, String family, Gender gender, String race, Species species, Population population, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = father;
        this.mother = mother;
        this.family = family;
        this.gender = gender;
        this.race = race;
        this.species = species;
        this.population = population;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Individual {" +
                "\"id\": " + id +
                ", \"name\": " + '\"' + name + '\"' +
                ", \"father\": " + father +
                ", \"mother\": " + mother +
                ", \"family\": " + '\"' + family + '\"' +
                ", \"gender\": " + gender +
                ", \"race\": " + '\"' + race + '\"' +
                ", \"species\": " + species +
                ", \"population\": " + population +
                ", \"attributes\": " + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public Individual setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Individual)) return false;

        Individual that = (Individual) o;

        if (id != that.id) return false;
        if (father != that.father) return false;
        if (mother != that.mother) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (family != null ? !family.equals(that.family) : that.family != null) return false;
        if (gender != that.gender) return false;
        if (race != null ? !race.equals(that.race) : that.race != null) return false;
        if (species != null ? !species.equals(that.species) : that.species != null) return false;
        if (population != null ? !population.equals(that.population) : that.population != null) return false;
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + father;
        result = 31 * result + mother;
        result = 31 * result + (family != null ? family.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (race != null ? race.hashCode() : 0);
        result = 31 * result + (species != null ? species.hashCode() : 0);
        result = 31 * result + (population != null ? population.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    public Individual setName(String name) {
        this.name = name;
        return this;

    }

    public int getFather() {
        return father;
    }

    public Individual setFather(int father) {
        this.father = father;
        return this;
    }

    public int getMother() {
        return mother;
    }

    public Individual setMother(int mother) {
        this.mother = mother;
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
}
