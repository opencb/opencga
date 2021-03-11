package org.opencb.opencga.core.models.analysis.knockout;

import java.util.List;

public class KnockoutByVariant {

    private String id;
    private String chromosome;
    private int start;
    private int end;
    private int length;
    private String reference;
    private String alternate;

    private int numIndividuals;
    private List<KnockoutByIndividual> individuals;

    public KnockoutByVariant() {
    }

    public KnockoutByVariant(String id, String chromosome, int start, int end, int length, String reference, String alternate,
                             List<KnockoutByIndividual> individuals) {
        this.id = id;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.numIndividuals = individuals != null ? individuals.size() : 0;
        this.individuals = individuals;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByVariant{");
        sb.append("id='").append(id).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", length=").append(length);
        sb.append(", reference='").append(reference).append('\'');
        sb.append(", alternate='").append(alternate).append('\'');
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

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutByVariant setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutByVariant setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutByVariant setEnd(int end) {
        this.end = end;
        return this;
    }

    public int getLength() {
        return length;
    }

    public KnockoutByVariant setLength(int length) {
        this.length = length;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public KnockoutByVariant setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public KnockoutByVariant setAlternate(String alternate) {
        this.alternate = alternate;
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
