package org.opencb.opencga.core.models.analysis.knockout;

public class VariantKnockoutStats extends KnockoutStats {

    private int numPairedCompHet;
    private int numPairedDelOverlap;

    public VariantKnockoutStats() {
    }

    public VariantKnockoutStats(int count, int numHomAlt, int numCompHet, int numPairedCompHet, int numPairedDelOverlap, int numHetAlt,
                                int numDelOverlap) {
        super(count, numHomAlt, numCompHet, numHetAlt, numDelOverlap);
        this.numPairedCompHet = numPairedCompHet;
        this.numPairedDelOverlap = numPairedDelOverlap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantKnockoutStats{");
        sb.append("numPairedCompHet=").append(numPairedCompHet);
        sb.append(", numPairedDelOverlap=").append(numPairedDelOverlap);
        sb.append('}');
        return sb.toString();
    }

    public int getNumPairedCompHet() {
        return numPairedCompHet;
    }

    public VariantKnockoutStats setNumPairedCompHet(int numPairedCompHet) {
        this.numPairedCompHet = numPairedCompHet;
        return this;
    }

    public int getNumPairedDelOverlap() {
        return numPairedDelOverlap;
    }

    public VariantKnockoutStats setNumPairedDelOverlap(int numPairedDelOverlap) {
        this.numPairedDelOverlap = numPairedDelOverlap;
        return this;
    }
}
