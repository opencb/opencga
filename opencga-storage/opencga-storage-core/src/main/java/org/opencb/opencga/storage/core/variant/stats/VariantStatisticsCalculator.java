package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmmut on 28/01/15.
 */
public class VariantStatisticsCalculator {
    private int skippedFiles;

    public VariantStatisticsCalculator() {
        skippedFiles = 0;
    }

    public int getSkippedFiles() {
        return skippedFiles;
    }

    public void setSkippedFiles(int skippedFiles) {
        this.skippedFiles = skippedFiles;
    }

    public List<VariantStatsWrapper> calculateBatch(List<Variant> variants, VariantSource variantSource) {
        List<VariantStatsWrapper> variantStatsWrappers = new ArrayList<>(variants.size());

        for (Variant variant : variants) {
            VariantSourceEntry file = variant.getSourceEntry(variantSource.getFileId(), variantSource.getStudyId());
            if (file == null) {
                skippedFiles++;
                continue;
            }
            VariantStats variantStats = new VariantStats(variant);
            file.setStats(variantStats.calculate(file.getSamplesData(), file.getAttributes(), null));
            variantStatsWrappers.add(new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), variantStats));
        }
        return variantStatsWrappers;
    }
}
