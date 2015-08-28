/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedEVSStatsCalculator;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedExacStatsCalculator;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;

import java.util.*;

/**
 * Created by jmmut on 28/01/15.
 */
public class VariantStatisticsCalculator {
    private int skippedFiles;
    private boolean overwrite;
    private VariantAggregatedStatsCalculator aggregatedCalculator;
    private VariantSource.Aggregation aggregation;

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
     * if the study is aggregated i.e. it doesn't have sample data, call this before calculate. It is not needed if the 
     * study does have samples.
     * @param aggregation see org.opencb.biodata.models.variant.VariantSource.Aggregation
     * @param tagmap nullable, see org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator()
     * @param cohorts nullable, set of cohort names that appear as prefixes in the variant attributes
     */
    public void setAggregationType(VariantSource.Aggregation aggregation, Properties tagmap, Set<String> cohorts) {
        aggregatedCalculator = null;
        this.aggregation = aggregation;
        boolean usingTagMap = tagmap != null;
        switch (this.aggregation) {
            case NONE:
                aggregatedCalculator = null;
                break;
            case BASIC:
                if (usingTagMap) {
                    aggregatedCalculator = new VariantAggregatedStatsCalculator(tagmap);
                } else {
                    aggregatedCalculator = new VariantAggregatedStatsCalculator(cohorts);
                }
                break;
            case EVS:
                if (usingTagMap) {
                    aggregatedCalculator = new VariantAggregatedEVSStatsCalculator(tagmap);
                } else {
                    aggregatedCalculator = new VariantAggregatedEVSStatsCalculator(cohorts);
                }
                break;
            case EXAC:
                if (usingTagMap) {
                    aggregatedCalculator = new VariantAggregatedExacStatsCalculator(tagmap);
                } else {
                    aggregatedCalculator = new VariantAggregatedExacStatsCalculator(cohorts);
                }
                break;
        }
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
            VariantSourceEntry file = null;
            for (Map.Entry<String, VariantSourceEntry> entry : variant.getSourceEntries().entrySet()) {
                if (entry.getValue().getStudyId().equals(studyId)) {
                    file = entry.getValue();
                    break;
                }
            }
            if (file == null) {
                skippedFiles++;
                continue;
            }
            
            if (VariantSource.Aggregation.NONE.equals(aggregation) && samples != null) {
                for (Map.Entry<String, Set<String>> cohort : samples.entrySet()) {
                    if (overwrite || file.getCohortStats(cohort.getKey()) == null) {

                        Map<String, Map<String, String>> samplesData = filterSamples(file.getSamplesData(), cohort.getValue());
                        VariantStats variantStats = new VariantStats(variant);
                        VariantStatsCalculator.calculate(samplesData, file.getAttributes(), null, variantStats);
                        file.getCohortStats().put(cohort.getKey(), variantStats);

                    }
                }
            } else if (aggregatedCalculator != null) { // another way to say that the study is aggregated (!VariantSource.Aggregation.NONE.equals(aggregation))
                aggregatedCalculator.calculate(variant, file);
            }
//            if (overwrite || file.getStats() == null) {
//                VariantStats allVariantStats = new VariantStats(variant);
//                file.setCohortStats(VariantSourceEntry.DEFAULT_COHORT
//                        , allVariantStats.calculate(file.getSamplesData(), file.getAttributes(), null));
//
//            }
                variantStatsWrappers.add(
                        new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), file.getCohortStats()));
        }
        return variantStatsWrappers;
    }
}
