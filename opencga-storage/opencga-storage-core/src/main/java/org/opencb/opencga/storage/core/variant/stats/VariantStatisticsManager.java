/*
 * Copyright 2015-2017 OpenCB
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

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.STATS_DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.AND;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.NOT;

/**
 * Created on 02/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantStatisticsManager {

    private static Logger logger = LoggerFactory.getLogger(VariantStatisticsManager.class);

    /**
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param options   Other options
     *                  {@link VariantStorageOptions#STATS_AGGREGATION_MAPPING_FILE}
     *                  {@link VariantStorageOptions#STATS_OVERWRITE}
     *                  {@link VariantStorageOptions#STATS_UPDATE}
     *                  {@link VariantStorageOptions#LOAD_THREADS}
     *                  {@link VariantStorageOptions#LOAD_BATCH_SIZE}
     *                  {@link org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam#REGION}
     *
     * @throws StorageEngineException      If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    public abstract void calculateStatistics(String study, List<String> cohorts, QueryOptions options)
            throws IOException, StorageEngineException;


    public void preCalculateStats(
            VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, List<String> cohorts,
            boolean overwrite, boolean updateStats, ObjectMap options) throws StorageEngineException {

        Map<String, Set<String>> cohortsWithSamples = new HashMap<>();
        for (String cohort : cohorts) {
            CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(studyMetadata.getId(), cohort);
            Set<String> samples = new HashSet<>();
            for (Integer sample : cohortMetadata.getSamples()) {
                samples.add(metadataManager.getSampleName(studyMetadata.getId(), sample));
            }
            cohortsWithSamples.put(cohortMetadata.getName(), samples);
        }
        preCalculateStats(metadataManager, studyMetadata, cohortsWithSamples, overwrite, updateStats, options);
    }

    public void preCalculateStats(
            VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, Map<String, Set<String>> cohorts,
            boolean overwrite, boolean updateStats, ObjectMap options) throws StorageEngineException {

        Collection<Integer> cohortIds = metadataManager.registerCohorts(studyMetadata.getName(), cohorts).values();
        checkCohorts(metadataManager, studyMetadata, cohorts, overwrite, updateStats, getAggregation(studyMetadata, options));

        metadataManager.updateStudyMetadata(studyMetadata.getName(), sm -> {
            for (Integer cohortId : cohortIds) {
                metadataManager.updateCohortMetadata(studyMetadata.getId(), cohortId,
                        cohort -> cohort.setStatsStatus(TaskMetadata.Status.RUNNING));
            }
            return sm;
        });
    }

    public void postCalculateStats(
            VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, Collection<String> cohorts, boolean error)
            throws StorageEngineException {

        TaskMetadata.Status status = error ? TaskMetadata.Status.ERROR : TaskMetadata.Status.READY;
        for (String cohortName : cohorts) {
            Integer cohortId = metadataManager.getCohortId(studyMetadata.getId(), cohortName);
            metadataManager.updateCohortMetadata(studyMetadata.getId(), cohortId,
                    cohort -> cohort.setStatsStatus(status));
        }
    }

    public static void checkAndUpdateCalculatedCohorts(
            VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, Collection<String> cohorts, boolean updateStats)
            throws StorageEngineException {
        for (String cohortName : cohorts) {
//            if (cohortName.equals(VariantSourceEntry.DEFAULT_COHORT)) {
//                continue;
//            }
            int studyId = studyMetadata.getId();
            Integer cohortId = metadataManager.getCohortId(studyMetadata.getId(), cohortName);
            metadataManager.updateCohortMetadata(studyId, cohortId, cohort -> {
                if (cohort.isInvalid()) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
                    LoggerFactory.getLogger(VariantStatisticsManager.class)
                            .debug("Cohort \"" + cohortName + "\" stats calculated and INVALID. Set as calculated");
                }
                if (cohort.isStatsReady()) {
                    if (!updateStats) {
                        throw new StorageEngineException("Cohort \"" + cohortName + "\" stats already calculated");
                    }
                }
                cohort.setStatsStatus(TaskMetadata.Status.RUNNING);
                return cohort;
            });
        }
    }

    /*
     * Check that all SampleIds are in the StudyMetadata.
     * <p>
     * If some cohort does not have samples, reads the content from StudyMetadata.
     * If there is no cohortId for come cohort, reads the content from StudyMetadata or auto-generate a cohortId
     * If some cohort has a different number of samples, check if this cohort is invalid.
     * <p>
     * Do not update the "calculatedStats" array. Just check that the provided cohorts are not calculated or invalid.
     * <p>
     * new requirements:
     * * an empty cohort is not an error if the study is aggregated
     * * there may be several empty cohorts, not just the ALL, because there may be several aggregated files with different sets of
     * hidden samples.
     * * if a cohort is already calculated, it is not an error if overwrite was provided
     *
     */
    protected static List<Integer> checkCohorts(
            VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, Map<String, Set<String>> cohorts,
            boolean overwrite, boolean updateStats, Aggregation aggregation) throws StorageEngineException {

        List<Integer> cohortIdList = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Set<String> samples = entry.getValue();
            CohortMetadata cohort = metadataManager.getCohortMetadata(studyMetadata.getId(), cohortName);
            final int cohortId = cohort.getId();

            final Collection<Integer> sampleIds;
            if (samples == null || samples.isEmpty()) {
                //There are not provided samples for this cohort. Take samples from StudyMetadata
                boolean aggregated = AggregationUtils.isAggregated(aggregation);
                if (aggregated) {
                    samples = Collections.emptySet();
                    sampleIds = Collections.emptySet();
                } else {
                    sampleIds = cohort.getSamples();
                    if (sampleIds == null || sampleIds.isEmpty()) {
//                if (sampleIds == null || (sampleIds.isEmpty()
//                        && Aggregation.NONE.equals(studyMetadata.getAggregation()))) {
                        //ERROR: StudyMetadata does not have samples for this cohort, and it is not an aggregated study
                        throw new StorageEngineException("Cohort \"" + cohortName + "\" is empty");
                    }
                    samples = new HashSet<>();
                    for (Integer sampleId : sampleIds) {
                        samples.add(metadataManager.getSampleName(studyMetadata.getId(), sampleId));
                    }
                }
                cohorts.put(cohortName, samples);
            } else {
                sampleIds = new HashSet<>(samples.size());
                for (String sample : samples) {
                    Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sample);
                    if (sampleId == null) {
                        //ERROR Sample not found
                        throw new StorageEngineException("Sample " + sample + " not found in the StudyMetadata");
                    } else {
                        sampleIds.add(sampleId);
                    }
                }
                if (sampleIds.size() != samples.size()) {
                    throw new StorageEngineException("Duplicated samples in cohort " + cohortName + ":" + cohortId);
                }

                if (!sampleIds.equals(new HashSet<>(cohort.getSamples()))) {
                    if (!cohort.isInvalid() && cohort.isStatsReady()) {
                        //If provided samples are different than the stored in the StudyMetadata, and the cohort was not invalid.
                        throw new StorageEngineException("Different samples in cohort " + cohortName + ":" + cohortId + ". "
                                + "Samples in the StudyMetadata: " + cohort.getSamples().size() + ". "
                                + "Samples provided " + samples.size() + ". Invalidate stats to continue.");
                    }
                }
            }

//            if (studyMetadata.getInvalidStats().contains(cohortId)) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
//            }
            if (cohort.isStatsReady()) {
                if (!overwrite) {
                    if (updateStats) {
                        logger.debug("Cohort \"" + cohortName + "\" stats already calculated. Calculate only for missing positions");
                    } else {
                        throw new StorageEngineException("Cohort \"" + cohortName + "\" stats already calculated");
                    }
                }
            }

            cohortIdList.add(cohortId);
        }
        return cohortIdList;
    }

    public static QueryOptions buildIncludeExclude() {
        return new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(VariantField.ANNOTATION, VariantField.STUDIES_STATS));
    }

    public static Query buildInputQuery(VariantStorageMetadataManager metadataManager, StudyMetadata study,
                                        Collection<?> cohorts, boolean overwrite, boolean updateStats,
                                        ObjectMap options) {
        return buildInputQuery(metadataManager, study, cohorts, overwrite, updateStats, options, getAggregation(study, options));
    }

    public static Query buildInputQuery(VariantStorageMetadataManager metadataManager, StudyMetadata study,
                                        Collection<?> cohorts, boolean overwrite, boolean updateStats,
                                        ObjectMap options, Aggregation aggregation) {
        int studyId = study.getId();
        Query readerQuery = new Query(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), studyId);
        if (options.containsKey(VariantQueryParam.REGION.key())) {
            Object region = options.get(VariantQueryParam.REGION.key());
            readerQuery.put(VariantQueryParam.REGION.key(), region);
        }
        if (updateStats && !overwrite) {
            //Get all variants that not contain any of the required cohorts
            readerQuery.append(VariantQueryParam.COHORT.key(),
                    cohorts.stream().map((cohort) -> NOT + study.getName() + ":" + cohort).collect(Collectors
                            .joining(AND)));
        }

        Set<Integer> sampleIds = new HashSet<>();
        for (Object cohort : cohorts) {
            Integer cohortId = metadataManager.getCohortId(studyId, cohort);
            CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(studyId, cohortId);
            sampleIds.addAll(cohortMetadata.getSamples());
        }

        readerQuery.put(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds);
        if (AggregationUtils.isAggregated(aggregation) || sampleIds.isEmpty()) {
            readerQuery.put(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL);
        }
        readerQuery.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);
        readerQuery.append(VariantQueryParam.UNKNOWN_GENOTYPE.key(),
                getUnknownGenotype(options));
        return readerQuery;
    }

    protected static String getUnknownGenotype(ObjectMap options) {
        return options.getString(STATS_DEFAULT_GENOTYPE.key(), STATS_DEFAULT_GENOTYPE.defaultValue());
    }

    public static Properties getAggregationMappingProperties(QueryOptions options) {
        return options.get(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), Properties.class, null);
    }

    protected static Aggregation getAggregation(StudyMetadata studyMetadata, ObjectMap options) {
        return AggregationUtils.valueOf(options.getString(VariantStorageOptions.STATS_AGGREGATION.key(),
                studyMetadata.getAggregation() == null ? Aggregation.NONE.name() : studyMetadata.getAggregation().toString()));
    }

    protected static boolean isAggregated(StudyMetadata studyMetadata, ObjectMap options) {
        return AggregationUtils.isAggregated(getAggregation(studyMetadata, options));
    }

}
