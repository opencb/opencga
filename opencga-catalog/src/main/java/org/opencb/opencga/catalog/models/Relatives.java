package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.utils.ParamUtils;

import java.util.Collections;
import java.util.List;

public class Relatives {

    private Individual member;
    private Individual father;
    private Individual mother;
    private List<String> diseases;
    private List<String> carrier;
    private boolean parentalConsanguinity;
    private Multiples multiples;

    public Relatives() {
    }

    public Relatives(Individual member, Individual father, Individual mother) {
        this(member, father, mother, null, null, null, false);
    }

    public Relatives(Individual member, Individual father, Individual mother, List<String> diseases, List<String> carrier,
                     Multiples multiples, boolean parentalConsanguinity) {
        this.member = member;
        this.father = father;
        this.mother = mother;
        this.diseases = ParamUtils.defaultObject(diseases, Collections::emptyList);
        this.carrier = ParamUtils.defaultObject(carrier, Collections::emptyList);
        this.multiples = multiples;
        this.parentalConsanguinity = parentalConsanguinity;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Relatives{");
        sb.append("member=").append(member);
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", diseases=").append(diseases);
        sb.append(", carrier=").append(carrier);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", multiples=").append(multiples);
        sb.append('}');
        return sb.toString();
    }


    public Individual getMember() {
        return member;
    }

    public Relatives setMember(Individual member) {
        this.member = member;
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

    public List<String> getCarrier() {
        return carrier;
    }

    public Relatives setCarrier(List<String> carrier) {
        this.carrier = carrier;
        return this;
    }

    public Multiples getMultiples() {
        return multiples;
    }

    public Relatives setMultiples(Multiples multiples) {
        this.multiples = multiples;
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
