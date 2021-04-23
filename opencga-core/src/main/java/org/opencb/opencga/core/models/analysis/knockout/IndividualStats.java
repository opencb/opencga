package org.opencb.opencga.core.models.analysis.knockout;

public class IndividualStats extends VariantStats {

    private VariantStats noParents;
    private VariantStats singleParent;
    private VariantStats bothParents;

    public IndividualStats() {
    }

    public IndividualStats(VariantStats noParents, VariantStats singleParent, VariantStats bothParents) {
        super(noParents.getCount() + singleParent.getCount() + bothParents.getCount(),
                noParents.getNumHomAlt() + singleParent.getNumHomAlt() + bothParents.getNumHomAlt(),
                noParents.getNumCompHet() + singleParent.getNumCompHet() + bothParents.getNumCompHet(),
                noParents.getNumHetAlt() + singleParent.getNumHetAlt() + bothParents.getNumHetAlt(),
                noParents.getNumDelOverlap() + singleParent.getNumDelOverlap() + bothParents.getNumDelOverlap());
        this.noParents = noParents;
        this.singleParent = singleParent;
        this.bothParents = bothParents;
    }

    public IndividualStats(int count, int numHomAlt, int numCompHet, int numHetAlt, int numDelOverlap, VariantStats noParents,
                           VariantStats singleParent, VariantStats bothParents) {
        super(count, numHomAlt, numCompHet, numHetAlt, numDelOverlap);
        this.noParents = noParents;
        this.singleParent = singleParent;
        this.bothParents = bothParents;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualStats{");
        sb.append("noParents=").append(noParents);
        sb.append(", singleParent=").append(singleParent);
        sb.append(", bothParents=").append(bothParents);
        sb.append('}');
        return sb.toString();
    }

    public VariantStats getNoParents() {
        return noParents;
    }

    public IndividualStats setNoParents(VariantStats noParents) {
        this.noParents = noParents;
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
