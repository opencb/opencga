package org.opencb.opencga.core.models.analysis.knockout;

public class RgaKnockoutByGene extends KnockoutByGene {

    private int numIndividuals;

    public RgaKnockoutByGene() {
    }

    public RgaKnockoutByGene(int numIndividuals) {
        this.numIndividuals = numIndividuals;
    }

    public int getNumIndividuals() {
        return numIndividuals;
    }

    public RgaKnockoutByGene setNumIndividuals(int numIndividuals) {
        this.numIndividuals = numIndividuals;
        return this;
    }
}
