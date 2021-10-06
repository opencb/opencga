package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;

import java.util.HashMap;
import java.util.Map;

public class InterpretationFindingStats {

    private int numVariants;
    private Map<String, Integer> tierCount;
    private Map<ClinicalVariant.Status, Integer> variantStatusCount;
    private Map<String, Integer> geneCount;

    public InterpretationFindingStats() {
    }

    public InterpretationFindingStats(int numVariants, Map<String, Integer> tierCount,
                                      Map<ClinicalVariant.Status, Integer> variantStatusCount, Map<String, Integer> geneCount) {
        this.numVariants = numVariants;
        this.tierCount = tierCount;
        this.variantStatusCount = variantStatusCount;
        this.geneCount = geneCount;
    }

    public static InterpretationFindingStats init() {
        Map<ClinicalVariant.Status, Integer> variantStatusCount = new HashMap<>();
        for (ClinicalVariant.Status value : ClinicalVariant.Status.values()) {
            variantStatusCount.put(value, 0);
        }

        return new InterpretationFindingStats(0, new HashMap<>(), variantStatusCount, new HashMap<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStats{");
        sb.append("numVariants=").append(numVariants);
        sb.append(", tierCount=").append(tierCount);
        sb.append(", variantStatusCount=").append(variantStatusCount);
        sb.append(", geneCount=").append(geneCount);
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

    public Map<String, Integer> getTierCount() {
        return tierCount;
    }

    public InterpretationFindingStats setTierCount(Map<String, Integer> tierCount) {
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

    public Map<String, Integer> getGeneCount() {
        return geneCount;
    }

    public InterpretationFindingStats setGeneCount(Map<String, Integer> geneCount) {
        this.geneCount = geneCount;
        return this;
    }

}
