package org.opencb.opencga.core.models.analysis.knockout;

public class IndividualStats extends VariantStats {

    private VariantStats missingParents;
    private VariantStats singleParent;
    private VariantStats bothParents;

    public IndividualStats() {
    }

    public IndividualStats(VariantStats missingParents, VariantStats singleParent, VariantStats bothParents) {
        super(missingParents.getCount() + singleParent.getCount() + bothParents.getCount(),
                missingParents.getNumHomAlt() + singleParent.getNumHomAlt() + bothParents.getNumHomAlt(),
                missingParents.getNumCompHet() + singleParent.getNumCompHet() + bothParents.getNumCompHet(),
                missingParents.getNumHetAlt() + singleParent.getNumHetAlt() + bothParents.getNumHetAlt(),
                missingParents.getNumDelOverlap() + singleParent.getNumDelOverlap() + bothParents.getNumDelOverlap());
        this.missingParents = missingParents;
        this.singleParent = singleParent;
        this.bothParents = bothParents;
    }

    public IndividualStats(int count, int numHomAlt, int numCompHet, int numHetAlt, int numDelOverlap, VariantStats missingParents,
                           VariantStats singleParent, VariantStats bothParents) {
        super(count, numHomAlt, numCompHet, numHetAlt, numDelOverlap);
        this.missingParents = missingParents;
        this.singleParent = singleParent;
        this.bothParents = bothParents;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualStats{");
        sb.append("missingParents=").append(missingParents);
        sb.append(", singleParent=").append(singleParent);
        sb.append(", bothParents=").append(bothParents);
        sb.append('}');
        return sb.toString();
    }

    public VariantStats getMissingParents() {
        return missingParents;
    }

    public IndividualStats setMissingParents(VariantStats missingParents) {
        this.missingParents = missingParents;
        return this;
    }

    public VariantStats getSingleParent() {
        return singleParent;
    }

    public IndividualStats setSingleParent(VariantStats singleParent) {
        this.singleParent = singleParent;
        return this;
    }

    public VariantStats getBothParents() {
        return bothParents;
    }

    public IndividualStats setBothParents(VariantStats bothParents) {
        this.bothParents = bothParents;
        return this;
    }
}
