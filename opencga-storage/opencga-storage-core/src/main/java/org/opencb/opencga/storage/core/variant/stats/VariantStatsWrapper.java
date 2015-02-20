package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 * Created by jmmut on 3/02/15.
 */
public class VariantStatsWrapper {
    private String chromosome;
    private int position;
    private VariantStats variantStats;

    public VariantStatsWrapper() {
        this.chromosome = null;
        this.position = -1;
        this.variantStats = null;
    }

    public VariantStatsWrapper(String chromosome, int position, VariantStats variantStats) {
        this.chromosome = chromosome;
        this.position = position;
        this.variantStats = variantStats;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public VariantStats getVariantStats() {
        return variantStats;
    }

    public void setVariantStats(VariantStats variantStats) {
        this.variantStats = variantStats;
    }
}
