package org.opencb.opencga.core.models.analysis.knockout;

public class RgaKnockoutByGene extends KnockoutByGene {

    private int numIndividuals;
    private boolean hasNextIndividual;

    public RgaKnockoutByGene() {
    }

    public RgaKnockoutByGene(int numIndividuals, boolean hasNextIndividual) {
        this.numIndividuals = numIndividuals;
        this.hasNextIndividual = hasNextIndividual;
    }

    public int getNumIndividuals() {
        return numIndividuals;
    }

    public RgaKnockoutByGene setNumIndividuals(int numIndividuals) {
        this.numIndividuals = numIndividuals;
        return this;
    }

    public boolean isHasNextIndividual() {
        return hasNextIndividual;
    }

    public RgaKnockoutByGene setHasNextIndividual(boolean hasNextIndividual) {
        this.hasNextIndividual = hasNextIndividual;
        return this;
    }
}
