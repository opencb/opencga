package org.opencb.opencga.core.models.analysis.knockout;

public class IndividualKnockoutStats extends KnockoutStats {

    private int numHomAltCompHet;
    private int numCompHetDelOverlap;

    public IndividualKnockoutStats() {
    }

    public IndividualKnockoutStats(int count, int numHomAlt, int numCompHet, int numHetAlt, int numDelOverlap, int numHomAltCompHet,
                                   int numCompHetDelOverlap) {
        super(count, numHomAlt, numCompHet, numHetAlt, numDelOverlap);
        this.numHomAltCompHet = numHomAltCompHet;
        this.numCompHetDelOverlap = numCompHetDelOverlap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualKnockoutStats{");
        sb.append("count=").append(count);
        sb.append(", numHomAlt=").append(numHomAlt);
        sb.append(", numCompHet=").append(numCompHet);
        sb.append(", numHetAlt=").append(numHetAlt);
        sb.append(", numDelOverlap=").append(numDelOverlap);
        sb.append(", numHomAltCompHet=").append(numHomAltCompHet);
        sb.append(", numCompHetDelOverlap=").append(numCompHetDelOverlap);
        sb.append('}');
        return sb.toString();
    }

    public int getNumHomAltCompHet() {
        return numHomAltCompHet;
    }

    public void setNumHomAltCompHet(int numHomAltCompHet) {
        this.numHomAltCompHet = numHomAltCompHet;
    }

    public int getNumCompHetDelOverlap() {
        return numCompHetDelOverlap;
    }

    public IndividualKnockoutStats setNumCompHetDelOverlap(int numCompHetDelOverlap) {
        this.numCompHetDelOverlap = numCompHetDelOverlap;
        return this;
    }
}
