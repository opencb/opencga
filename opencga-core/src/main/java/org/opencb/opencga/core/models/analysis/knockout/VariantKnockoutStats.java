package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantKnockoutStats extends KnockoutStats {

    @DataField(description = ParamConstants.VARIANT_KNOCKOUT_STATS_NUM_PAIRED_COMP_HET_DESCRIPTION)
    private int numPairedCompHet;

    public VariantKnockoutStats() {
    }

    public VariantKnockoutStats(int count, int numHomAlt, int numCompHet, int numPairedCompHet, int numHetAlt, int numDelOverlap) {
        super(count, numHomAlt, numCompHet, numHetAlt, numDelOverlap);
        this.numPairedCompHet = numPairedCompHet;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantKnockoutStats{");
        sb.append("numPairedCompHet=").append(numPairedCompHet);
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
}
