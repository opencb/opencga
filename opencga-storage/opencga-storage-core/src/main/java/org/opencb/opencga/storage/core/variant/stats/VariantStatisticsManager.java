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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.AND;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NOT;

/**
 * Created on 02/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStatisticsManager {

    String UNKNOWN_GENOTYPE = ".";

    /**
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param options   Other options
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#AGGREGATION_MAPPING_PROPERTIES}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#OVERWRITE_STATS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#UPDATE_STATS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#LOAD_THREADS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#LOAD_BATCH_SIZE}
     *                  {@link org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam#REGION}
     *
     * @throws StorageEngineException      If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    void calculateStatistics(String study, List<String> cohorts, QueryOptions options) throws IOException, StorageEngineException;


    static void checkAndUpdateCalculatedCohorts(StudyConfiguration studyConfiguration, Collection<String> cohorts, boolean updateStats)
            throws StorageEngineException {
        for (String cohortName : cohorts) {
//            if (cohortName.equals(VariantSourceEntry.DEFAULT_COHORT)) {
//                continue;
//            }
            Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
            if (studyConfiguration.getInvalidStats().contains(cohortId)) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
                LoggerFactory.getLogger(VariantStatisticsManager.class)
                        .debug("Cohort \"" + cohortName + "\" stats calculated and INVALID. Set as calculated");
                studyConfiguration.getInvalidStats().remove(cohortId);
            }
            if (studyConfiguration.getCalculatedStats().contains(cohortId)) {
                if (!updateStats) {
                    throw new StorageEngineException("Cohort \"" + cohortName + "\" stats already calculated");
                }
            } else {
                studyConfiguration.getCalculatedStats().add(cohortId);
            }
        }
    }

    static QueryOptions buildIncludeExclude() {
        return new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(VariantField.ANNOTATION, VariantField.STUDIES_STATS));
    }

    static Query buildInputQuery(VariantStorageMetadataManager metadataManager, StudyMetadata study,
                                 Collection<?> cohorts, boolean overwrite, boolean updateStats,
                                 ObjectMap options) {
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
        readerQuery.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);
        readerQuery.append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), UNKNOWN_GENOTYPE);
        return readerQuery;
    }

    static Properties getAggregationMappingProperties(QueryOptions options) {
        return options.get(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), Properties.class, null);
    }

}
