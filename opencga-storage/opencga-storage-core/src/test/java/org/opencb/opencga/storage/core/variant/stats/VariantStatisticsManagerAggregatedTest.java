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

import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by hpccoll1 on 01/06/15.
 */
@Ignore
public abstract class VariantStatisticsManagerAggregatedTest extends VariantStorageBaseTest {

    public static final String VCF_TEST_FILE_NAME = "variant-test-aggregated-file.vcf.gz";
    protected StudyMetadata studyMetadata;
    protected VariantDBAdaptor dbAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Override
    @Before
    public void before() throws Exception {
        studyMetadata = newStudyMetadata();
        studyMetadata.setAggregation(getAggregationType());
        clearDB(DB_NAME);
        inputUri = getInputUri();
        runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false));
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
    }

    protected URI getInputUri() throws IOException {
        return getResourceUri(VCF_TEST_FILE_NAME);
    }

    protected Aggregation getAggregationType() {
        return Aggregation.BASIC;
    }

    protected Properties getAggregationMappingFile() {
        return null;
    }

    @Test
    public void calculateAggregatedStatsTest() throws Exception {
        calculateAggregatedStatsTest(new QueryOptions());
    }

    @Test
    public void calculateAggregatedStatsNonAggregatedStudyTest() throws Exception {
        dbAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata.getName(), sm -> {
            sm.setAggregation(Aggregation.NONE);
            return sm;
        });
        calculateAggregatedStatsTest(new QueryOptions(VariantStorageOptions.STATS_AGGREGATION.key(), getAggregationType()));
    }

    protected void calculateAggregatedStatsTest(QueryOptions options) throws Exception {
        //Calculate stats for 2 cohorts at one time
        VariantStatisticsManager vsm = variantStorageEngine.newVariantStatisticsManager();

        checkAggregatedCohorts(dbAdaptor, studyMetadata);

        options.put(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100);
        options.put(DefaultVariantStatisticsManager.OUTPUT, outputUri.resolve("aggregated.stats").getPath());
        if (getAggregationMappingFile() != null) {
            options.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), getAggregationMappingFile());
        }

        //Calculate stats
        List<String> cohorts = Collections.singletonList(StudyEntry.DEFAULT_COHORT);
        vsm.calculateStatistics(studyMetadata.getName(), cohorts, options);

        checkAggregatedCohorts(dbAdaptor, studyMetadata);
    }

    protected void checkAggregatedCohorts(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata) {
        List<CohortMetadata> cohorts = Arrays.asList(
                Iterators.toArray(
                        Iterators.filter(
                                dbAdaptor.getMetadataManager().cohortIterator(studyMetadata.getId()),
                                CohortMetadata::isStatsReady),
                        CohortMetadata.class));
        for (Variant variant : dbAdaptor.iterable(new VariantQuery().includeSampleAll(), new QueryOptions())) {
            for (StudyEntry study : variant.getStudies()) {
                assertNotNull(study.getFiles().get(0));
                Map<String, VariantStats> cohortStats = study.getStats()
                        .stream()
                        .collect(Collectors.toMap(VariantStats::getCohortId, i -> i));
                String calculatedCohorts = cohortStats.keySet().toString();
                cohorts.forEach(cohort -> {
                    String cohortName = cohort.getName();
                    assertTrue("CohortStats should contain stats for cohort " + cohortName
                                    + ". Only contains stats for " + calculatedCohorts,
                            cohortStats.containsKey(cohortName));    //Check stats are calculated

                    assertValidStats(variant, cohortStats.get(cohortName));
                });
            }
        }
    }

    protected void assertValidStats(Variant variant, VariantStats variantStats) {
        assertNotEquals("Stats seem with no valid values, for instance (chr=" + variant.getChromosome()
                        + ", start=" + variant.getStart() + ", ref=" + variant.getReference() + ", alt="
                        + variant.getAlternate() + "), maf=" + variantStats.getMaf(),
                -1,
                variantStats.getMaf(), 0.001);
    }
}
