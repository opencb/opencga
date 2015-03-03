package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.Map;

/**
 * Class to link a VariantStats with its variant, using just the chromosome and the position.
 */
public class VariantStatsWrapper {
    private String chromosome;
    private int position;
    private Map<String, VariantStats> cohortStats;

    public VariantStatsWrapper() {
        this.chromosome = null;
        this.position = -1;
        this.cohortStats = null;
    }

    public VariantStatsWrapper(String chromosome, int position, Map<String, VariantStats> cohortStats) {
        this.chromosome = chromosome;
        this.position = position;
        this.cohortStats = cohortStats;
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

    public Map<String, VariantStats> getCohortStats() {
        return cohortStats;
    }

    public void setCohortStats(Map<String, VariantStats> cohortStats) {
        this.cohortStats = cohortStats;
    }
}
