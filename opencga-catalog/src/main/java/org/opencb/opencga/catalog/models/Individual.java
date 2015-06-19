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
    private Integer father;
    private Integer mother;
    private String family;
    private Gender gender;


    private String race;
    private Species species;
    private Population population;

    // internal class
    class Species {
        private String taxonomyCode;
        private String scientificName;
        private String commonName;
    }

    class Population {
        private String name;
        private String subpopulation;
        private String description;
    }

    private Map<String, Object> attributes;

    public Individual() {
    }

    public Individual(int id, String name, Integer father, Integer mother, String family, Gender gender, String race, Species species, Population population, Map<String, Object> attributes) {
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

    public Individual setName(String name) {
        this.name = name;
        return this;
    }

    public Integer getFather() {
        return father;
    }

    public Individual setFather(Integer father) {
        this.father = father;
        return this;
    }

    public Integer getMother() {
        return mother;
    }

    public Individual setMother(Integer mother) {
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
