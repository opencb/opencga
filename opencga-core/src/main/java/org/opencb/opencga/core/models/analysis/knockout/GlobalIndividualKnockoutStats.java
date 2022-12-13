package org.opencb.opencga.core.models.analysis.knockout;

public class GlobalIndividualKnockoutStats extends IndividualKnockoutStats {

    private IndividualKnockoutStats missingParents;
    private IndividualKnockoutStats singleParent;
    private IndividualKnockoutStats bothParents;

    public GlobalIndividualKnockoutStats() {
    }

    public GlobalIndividualKnockoutStats(IndividualKnockoutStats missingParents, IndividualKnockoutStats singleParent,
                                         IndividualKnockoutStats bothParents) {
        super(missingParents.getCount() + singleParent.getCount() + bothParents.getCount(),
                missingParents.getNumHomAlt() + singleParent.getNumHomAlt() + bothParents.getNumHomAlt(),
                missingParents.getNumCompHet() + singleParent.getNumCompHet() + bothParents.getNumCompHet(),
                missingParents.getNumHetAlt() + singleParent.getNumHetAlt() + bothParents.getNumHetAlt(),
                missingParents.getNumDelOverlap() + singleParent.getNumDelOverlap() + bothParents.getNumDelOverlap(),
                missingParents.getNumHomAltCompHet() + singleParent.getNumHomAltCompHet() + bothParents.getNumHomAltCompHet(),
                missingParents.getNumCompHetDelOverlap() + singleParent.getNumCompHetDelOverlap() + bothParents.getNumCompHetDelOverlap()
        );
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

    public IndividualKnockoutStats getMissingParents() {
        return missingParents;
    }

    public GlobalIndividualKnockoutStats setMissingParents(IndividualKnockoutStats missingParents) {
        this.missingParents = missingParents;
        return this;
    }

    public IndividualKnockoutStats getSingleParent() {
        return singleParent;
    }

    public GlobalIndividualKnockoutStats setSingleParent(IndividualKnockoutStats singleParent) {
        this.singleParent = singleParent;
        return this;
    }

    public IndividualKnockoutStats getBothParents() {
        return bothParents;
    }

    public GlobalIndividualKnockoutStats setBothParents(IndividualKnockoutStats bothParents) {
        this.bothParents = bothParents;
        return this;
    }
}
