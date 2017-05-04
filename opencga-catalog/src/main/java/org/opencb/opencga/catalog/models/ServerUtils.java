package org.opencb.opencga.catalog.models;

import java.util.List;

/**
 * Created by pfurio on 04/05/17.
 * This class will contain all the different data models that does not have a full correspondence with the catalog data models and that
 * will be used as the main input of the entry points of the webservices.
 */
public class ServerUtils {

    public static class IndividualParameters {
        private String name;
        private String family;
        private long fatherId;
        private long motherId;
        private Individual.Sex sex;
        private String ethnicity;
        private Individual.Species species;
        private Individual.Population population;
        private Individual.KaryotypicSex karyotypicSex;
        private Individual.LifeStatus lifeStatus;
        private Individual.AffectationStatus affectationStatus;

        private List<Sample> samples;

        public String getName() {
            return name;
        }

        public IndividualParameters setName(String name) {
            this.name = name;
            return this;
        }

        public String getFamily() {
            return family;
        }

        public IndividualParameters setFamily(String family) {
            this.family = family;
            return this;
        }

        public long getFatherId() {
            return fatherId;
        }

        public IndividualParameters setFatherId(long fatherId) {
            this.fatherId = fatherId;
            return this;
        }

        public long getMotherId() {
            return motherId;
        }

        public IndividualParameters setMotherId(long motherId) {
            this.motherId = motherId;
            return this;
        }

        public Individual.Sex getSex() {
            return sex;
        }

        public IndividualParameters setSex(Individual.Sex sex) {
            this.sex = sex;
            return this;
        }

        public String getEthnicity() {
            return ethnicity;
        }

        public IndividualParameters setEthnicity(String ethnicity) {
            this.ethnicity = ethnicity;
            return this;
        }

        public Individual.Species getSpecies() {
            return species;
        }

        public IndividualParameters setSpecies(Individual.Species species) {
            this.species = species;
            return this;
        }

        public Individual.Population getPopulation() {
            return population;
        }

        public IndividualParameters setPopulation(Individual.Population population) {
            this.population = population;
            return this;
        }

        public Individual.KaryotypicSex getKaryotypicSex() {
            return karyotypicSex;
        }

        public IndividualParameters setKaryotypicSex(Individual.KaryotypicSex karyotypicSex) {
            this.karyotypicSex = karyotypicSex;
            return this;
        }

        public Individual.LifeStatus getLifeStatus() {
            return lifeStatus;
        }

        public IndividualParameters setLifeStatus(Individual.LifeStatus lifeStatus) {
            this.lifeStatus = lifeStatus;
            return this;
        }

        public Individual.AffectationStatus getAffectationStatus() {
            return affectationStatus;
        }

        public IndividualParameters setAffectationStatus(Individual.AffectationStatus affectationStatus) {
            this.affectationStatus = affectationStatus;
            return this;
        }

        public List<Sample> getSamples() {
            return samples;
        }

        public IndividualParameters setSamples(List<Sample> samples) {
            this.samples = samples;
            return this;
        }
    }
}
