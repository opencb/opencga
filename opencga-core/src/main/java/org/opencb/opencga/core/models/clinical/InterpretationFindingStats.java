package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;

import java.util.HashMap;
import java.util.Map;

public class InterpretationFindingStats {

    private int numVariants;
    private TierCountStats tierCount;
    private Map<ClinicalVariant.Status, Integer> variantStatusCount;

    public InterpretationFindingStats() {
    }

    public InterpretationFindingStats(int numVariants, TierCountStats tierCount, Map<ClinicalVariant.Status, Integer> variantStatusCount) {
        this.numVariants = numVariants;
        this.tierCount = tierCount;
        this.variantStatusCount = variantStatusCount;
    }

    public static InterpretationFindingStats init() {
        Map<ClinicalVariant.Status, Integer> variantStatusCount = new HashMap<>();
        for (ClinicalVariant.Status value : ClinicalVariant.Status.values()) {
            variantStatusCount.put(value, 0);
        }

        return new InterpretationFindingStats(0, TierCountStats.init(), variantStatusCount);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStats{");
        sb.append("numVariants=").append(numVariants);
        sb.append(", tierCount=").append(tierCount);
        sb.append(", variantStatusCount=").append(variantStatusCount);
        sb.append('}');
        return sb.toString();
    }

    public int getNumVariants() {
        return numVariants;
    }

    public InterpretationFindingStats setNumVariants(int numVariants) {
        this.numVariants = numVariants;
        return this;
    }

    public TierCountStats getTierCount() {
        return tierCount;
    }

    public InterpretationFindingStats setTierCount(TierCountStats tierCount) {
        this.tierCount = tierCount;
        return this;
    }

    public Map<ClinicalVariant.Status, Integer> getVariantStatusCount() {
        return variantStatusCount;
    }

    public InterpretationFindingStats setVariantStatusCount(Map<ClinicalVariant.Status, Integer> variantStatusCount) {
        this.variantStatusCount = variantStatusCount;
        return this;
    }

    public static class TierCountStats {

        private int tier1;
        private int tier2;
        private int tier3;

        public TierCountStats() {
        }

        public TierCountStats(int tier1, int tier2, int tier3) {
            this.tier1 = tier1;
            this.tier2 = tier2;
            this.tier3 = tier3;
        }

        public static TierCountStats init() {
            return new TierCountStats(0, 0, 0);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TierCountStats{");
            sb.append("tier1=").append(tier1);
            sb.append(", tier2=").append(tier2);
            sb.append(", tier3=").append(tier3);
            sb.append('}');
            return sb.toString();
        }

        public int getTier1() {
            return tier1;
        }

        public TierCountStats setTier1(int tier1) {
            this.tier1 = tier1;
            return this;
        }

        public int getTier2() {
            return tier2;
        }

        public TierCountStats setTier2(int tier2) {
            this.tier2 = tier2;
            return this;
        }

        public int getTier3() {
            return tier3;
        }

        public TierCountStats setTier3(int tier3) {
            this.tier3 = tier3;
            return this;
        }
    }

}
