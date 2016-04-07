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

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedEVSStatsCalculator;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedExacStatsCalculator;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;

import java.util.*;

import static org.opencb.biodata.models.variant.VariantSource.Aggregation.isAggregated;

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
     *
     * @param aggregation see org.opencb.biodata.models.variant.VariantSource.Aggregation
     * @param tagmap      nullable, see org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator()
     */
    public void setAggregationType(VariantSource.Aggregation aggregation, Properties tagmap) {
        aggregatedCalculator = null;
        this.aggregation = aggregation;
        switch (this.aggregation) {
            case NONE:
                aggregatedCalculator = null;
                break;
            case BASIC:
                aggregatedCalculator = new VariantAggregatedStatsCalculator(tagmap);
                break;
            case EVS:
                aggregatedCalculator = new VariantAggregatedEVSStatsCalculator(tagmap);
                break;
            case EXAC:
                aggregatedCalculator = new VariantAggregatedExacStatsCalculator(tagmap);
                break;
            default:
                break;
        }
    }

    /**
     * Creates another map with the intersection of the parameters.
     *
     * @param allSamples    Map that contains the values we want a subset of
     * @param samplesToKeep set of names of samples
     * @param <T> Type
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
     *
     * @param variants variants to to calculate stats from
     * @param studyId  needed to choose the VariantSourceEntry in the variants
     * @param fileId   needed to choose the VariantSourceEntry in the variants
     * @param samples  keys are cohort names, values are sets of samples names. groups of samples (cohorts) for each to compute
     *                 VariantStats.
     * @return list of VariantStatsWrapper. may be shorter than the list of variants if there is no source for some variant
     */
    public List<VariantStatsWrapper> calculateBatch(List<Variant> variants, String studyId, String fileId,
                                                    Map<String, Set<String>> samples) {
        List<VariantStatsWrapper> variantStatsWrappers = new ArrayList<>(variants.size());

        for (Variant variant : variants) {
            StudyEntry study = null;
            for (StudyEntry entry : variant.getStudies()) {
                if (entry.getStudyId().equals(studyId)) {
                    study = entry;
                    break;
                }
            }
            if (study == null) {
                skippedFiles++;
                continue;
            }

            if (!isAggregated(aggregation) && samples != null) {
                for (Map.Entry<String, Set<String>> cohort : samples.entrySet()) {
                    if (overwrite || study.getStats(cohort.getKey()) == null) {

                        Map<String, Map<String, String>> samplesData = filterSamples(study.getSamplesDataAsMap(), cohort.getValue());
                        VariantStats variantStats = new VariantStats(variant);
                        VariantStatsCalculator.calculate(samplesData, study.getAttributes(), null, variantStats);
                        study.setStats(cohort.getKey(), variantStats);

                    }
                }
            } else if (aggregatedCalculator != null) { // another way to say that the study is aggregated (!VariantSource.Aggregation
                // .NONE.equals(aggregation))
//                study.setAttributes(removeAttributePrefix(study.getAttributes()));
                aggregatedCalculator.calculate(variant, study);
            }
//            if (overwrite || file.getStats() == null) {
//                VariantStats allVariantStats = new VariantStats(variant);
//                file.setCohortStats(VariantSourceEntry.DEFAULT_COHORT
//                        , allVariantStats.calculate(file.getSamplesData(), file.getAttributes(), null));
//
//            }
            variantStatsWrappers.add(
                    new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), study.getStats()));
        }
        return variantStatsWrappers;
    }

    @Deprecated
    public static Map<String, String> removeAttributePrefix(Map<String, String> attributes)
            throws IllegalArgumentException {
        Map<String, String> newAttributes = new LinkedHashMap<>(attributes.size());
        Set<String> prefixSet = new LinkedHashSet<>();
        for (String key : attributes.keySet()) {
            String[] split = key.split("_", 2);
            prefixSet.add(split[0]);
            newAttributes.put(split[1], attributes.get(key));
        }

        if (prefixSet.size() > 1) {
            throw new IllegalArgumentException("attributes should contain only one fileId prefix, and there are: "
                    + prefixSet.toString());
        }
        return newAttributes;
    }
}
