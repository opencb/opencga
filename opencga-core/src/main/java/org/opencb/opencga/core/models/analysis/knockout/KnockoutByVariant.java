package org.opencb.opencga.core.models.analysis.knockout;

import java.util.List;

public class KnockoutByVariant {

    private String id;
    private int numIndividuals;
    private List<KnockoutByIndividual> individuals;

    public KnockoutByVariant() {
    }

    public KnockoutByVariant(String id, List<KnockoutByIndividual> individuals) {
        this.id = id;
        this.numIndividuals = individuals != null ? individuals.size() : 0;
        this.individuals = individuals;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByVariant{");
        sb.append("id='").append(id).append('\'');
        sb.append(", numIndividuals=").append(numIndividuals);
        sb.append(", individuals=").append(individuals);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutByVariant setId(String id) {
        this.id = id;
        return this;
    }

    public int getNumIndividuals() {
        return numIndividuals;
    }

    public KnockoutByVariant setNumIndividuals(int numIndividuals) {
        this.numIndividuals = numIndividuals;
        return this;
    }

    public List<KnockoutByIndividual> getIndividuals() {
        return individuals;
    }

    public KnockoutByVariant setIndividuals(List<KnockoutByIndividual> individuals) {
        this.individuals = individuals;
        return this;
    }
}
