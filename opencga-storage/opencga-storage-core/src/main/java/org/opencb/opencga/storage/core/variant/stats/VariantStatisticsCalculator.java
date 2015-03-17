package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.*;

/**
 * Created by jmmut on 28/01/15.
 */
public class VariantStatisticsCalculator {
    private int skippedFiles;
    private boolean overwrite;

    public VariantStatisticsCalculator() {
        this(false);
    }

    public VariantStatisticsCalculator(boolean overwrite) {
        this.overwrite = overwrite;
        skippedFiles = 0;
    }

    public int getSkippedFiles() {
        return skippedFiles;
    }

    public void setSkippedFiles(int skippedFiles) {
        this.skippedFiles = skippedFiles;
    }

    /**
     * Creates another map with the intersection of the parameters.
     * @param allSamples Map that contains the values we want a subset of
     * @param samplesToKeep set of names of samples
     * @return variant with just the samples in 'samplesToKeep'
     */
    public <T> Map<String, T> filterSamples(Map<String, T> allSamples, Set<String> samplesToKeep) {
        Map<String, T> filtered = new HashMap<>();
        if (samplesToKeep != null) {
            for (String sampleName : allSamples.keySet()) {
                if (samplesToKeep.contains(sampleName)) {
                    filtered.put(sampleName, allSamples.get(sampleName));
                }
            }
        }
        return filtered;
    }

    /**
     * computes the VariantStats for each subset of samples.
     * @param variants
     * @param studyId needed to choose the VariantSourceEntry in the variants
     * @param fileId  needed to choose the VariantSourceEntry in the variants
     * @param samples keys are cohort names, values are sets of samples names. groups of samples (cohorts) for each to compute VariantStats.
     * @return list of VariantStatsWrapper. may be shorter than the list of variants if there is no source for some variant
     */
    public List<VariantStatsWrapper> calculateBatch(List<Variant> variants, String studyId, String fileId
            , Map<String, Set<String>> samples) {
        List<VariantStatsWrapper> variantStatsWrappers = new ArrayList<>(variants.size());

        for (Variant variant : variants) {
            VariantSourceEntry file = variant.getSourceEntry(studyId, fileId);
            if (file == null) {
                skippedFiles++;
                continue;
            }
            if (samples != null) {
                for (Map.Entry<String, Set<String>> cohort : samples.entrySet()) {
                    if (overwrite || file.getCohortStats(cohort.getKey()) == null) {
                        VariantStats variantStats = new VariantStats(variant);

                        Map<String, Map<String, String>> samplesData = filterSamples(file.getSamplesData(), cohort.getValue());
                        file.getCohortStats().put(cohort.getKey()
                                , variantStats.calculate(samplesData, file.getAttributes(), null));
                    }
                }
            }
            if (overwrite || file.getStats() == null) {
                VariantStats allVariantStats = new VariantStats(variant);
                file.setCohortStats(VariantSourceEntry.DEFAULT_COHORT
                        , allVariantStats.calculate(file.getSamplesData(), file.getAttributes(), null));

            }
                variantStatsWrappers.add(
                        new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), file.getCohortStats()));
        }
        return variantStatsWrappers;
    }
}
