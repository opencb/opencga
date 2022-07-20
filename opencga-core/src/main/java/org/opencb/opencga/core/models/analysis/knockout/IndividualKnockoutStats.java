package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class IndividualKnockoutStats extends KnockoutStats {

    @DataField(description = ParamConstants.INDIVIDUAL_KNOCKOUT_STATS_MISSING_PARENTS_DESCRIPTION)
    private KnockoutStats missingParents;
    @DataField(description = ParamConstants.INDIVIDUAL_KNOCKOUT_STATS_SINGLE_PARENT_DESCRIPTION)
    private KnockoutStats singleParent;
    @DataField(description = ParamConstants.INDIVIDUAL_KNOCKOUT_STATS_BOTH_PARENTS_DESCRIPTION)
    private KnockoutStats bothParents;

    public IndividualKnockoutStats() {
    }

    public IndividualKnockoutStats(KnockoutStats missingParents, KnockoutStats singleParent, KnockoutStats bothParents) {
        super(missingParents.getCount() + singleParent.getCount() + bothParents.getCount(),
                missingParents.getNumHomAlt() + singleParent.getNumHomAlt() + bothParents.getNumHomAlt(),
                missingParents.getNumCompHet() + singleParent.getNumCompHet() + bothParents.getNumCompHet(),
                missingParents.getNumHetAlt() + singleParent.getNumHetAlt() + bothParents.getNumHetAlt(),
                missingParents.getNumDelOverlap() + singleParent.getNumDelOverlap() + bothParents.getNumDelOverlap());
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

    public KnockoutStats getMissingParents() {
        return missingParents;
    }

    public IndividualKnockoutStats setMissingParents(KnockoutStats missingParents) {
        this.missingParents = missingParents;
        return this;
    }

    public KnockoutStats getSingleParent() {
        return singleParent;
    }

    public IndividualKnockoutStats setSingleParent(KnockoutStats singleParent) {
        this.singleParent = singleParent;
        return this;
    }

    public KnockoutStats getBothParents() {
        return bothParents;
    }

    public IndividualKnockoutStats setBothParents(KnockoutStats bothParents) {
        this.bothParents = bothParents;
        return this;
    }
}
