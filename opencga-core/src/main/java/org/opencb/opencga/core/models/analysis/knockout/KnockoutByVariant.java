package org.opencb.opencga.core.models.analysis.knockout;

import java.util.List;

public class KnockoutByVariant {

    private String id;
    private List<KnockoutByIndividual> individuals;

    public KnockoutByVariant() {
    }

    public KnockoutByVariant(String id, List<KnockoutByIndividual> individuals) {
        this.id = id;
        this.individuals = individuals;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByVariant{");
        sb.append("id='").append(id).append('\'');
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

    public List<KnockoutByIndividual> getIndividuals() {
        return individuals;
    }

    public KnockoutByVariant setIndividuals(List<KnockoutByIndividual> individuals) {
        this.individuals = individuals;
        return this;
    }
}
