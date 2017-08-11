package org.opencb.opencga.catalog.models;

import java.util.Collections;
import java.util.List;

public class Relatives {

    private Individual individual;
    private Individual father;
    private Individual mother;
    private List<String> diseases;
    private boolean parentalConsanguinity;

    public Relatives() {
    }

    public Relatives(Individual individual, Individual father, Individual mother) {
        this(individual, father, mother, Collections.emptyList(), false);
    }

    public Relatives(Individual individual, Individual father, Individual mother, List<String> diseases, boolean parentalConsanguinity) {
        this.individual = individual;
        this.father = father;
        this.mother = mother;
        this.diseases = diseases;
        this.parentalConsanguinity = parentalConsanguinity;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Relatives{");
        sb.append("individual=").append(individual);
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", diseases=").append(diseases);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append('}');
        return sb.toString();
    }


    public Individual getIndividual() {
        return individual;
    }

    public Relatives setIndividual(Individual individual) {
        this.individual = individual;
        return this;
    }

    public Individual getFather() {
        return father;
    }

    public Relatives setFather(Individual father) {
        this.father = father;
        return this;
    }

    public Individual getMother() {
        return mother;
    }

    public Relatives setMother(Individual mother) {
        this.mother = mother;
        return this;
    }

    public List<String> getDiseases() {
        return diseases;
    }

    public Relatives setDiseases(List<String> diseases) {
        this.diseases = diseases;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public Relatives setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }
}
