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

import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;

/**
 * Created by hpccoll1 on 01/06/15.
 */
@Ignore
public abstract class VariantStatisticsManagerTest extends VariantStorageBaseTest {

    public static final String VCF_TEST_FILE_NAME = "variant-test-file.vcf.gz";
    private StudyConfiguration studyConfiguration;
    private VariantDBAdaptor dbAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws IOException {
        Path rootDir = getTmpRootDir();
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Files.copy(VariantStorageEngineTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath,
                StandardCopyOption.REPLACE_EXISTING);
        inputUri = inputPath.toUri();
    }

    @Override
    @Before
    public void before() throws Exception {
        studyConfiguration = newStudyConfiguration();
        clearDB(DB_NAME);
        runDefaultETL(inputUri, getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), false));
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
    }

    @Test
    public void calculateStatsMultiCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts at one time
        checkCohorts(dbAdaptor, studyConfiguration);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageEngine.Options.LOAD_BATCH_SIZE.key(), 100);
        Iterator<String> iterator = studyConfiguration.getSampleIds().keySet().iterator();

        /** Create cohorts **/
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(iterator.next());
        cohort1.add(iterator.next());

        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(iterator.next());
        cohort2.add(iterator.next());

        Map<String, Set<String>> cohorts = new HashMap<>();
        cohorts.put("cohort1", cohort1);
        cohorts.put("cohort2", cohort2);

        //Calculate stats
        stats(options, studyConfiguration, cohorts, outputUri.resolve("cohort1.cohort2.stats"));

        checkCohorts(dbAdaptor, studyConfiguration);
    }

    @Test
    public void calculateStatsSeparatedCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts separately

        String studyName = studyConfiguration.getStudyName();
        QueryOptions options = new QueryOptions();
        options.put(VariantStorageEngine.Options.LOAD_BATCH_SIZE.key(), 100);
        Iterator<String> iterator = studyConfiguration.getSampleIds().keySet().iterator();
        StudyConfiguration studyConfiguration;

        /** Create first cohort **/
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, QueryOptions.empty()).first();
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(iterator.next());
        cohort1.add(iterator.next());

        Map<String, Set<String>> cohorts;

        cohorts = new HashMap<>();
        cohorts.put("cohort1", cohort1);

        //Calculate stats for cohort1
        studyConfiguration = stats(options, studyConfiguration, cohorts, outputUri.resolve("cohort1.stats"));

        int cohort1Id = studyConfiguration.getCohortIds().get("cohort1");
        assertThat(studyConfiguration.getCalculatedStats(), hasItem(cohort1Id));
        checkCohorts(dbAdaptor, studyConfiguration);

        /** Create second cohort **/
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, QueryOptions.empty()).first();
        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(iterator.next());
        cohort2.add(iterator.next());

        cohorts = new HashMap<>();
        cohorts.put("cohort2", cohort2);

        //Calculate stats for cohort2
        studyConfiguration = stats(options, studyConfiguration, cohorts, outputUri.resolve("cohort2.stats"));

        int cohort2Id = studyConfiguration.getCohortIds().get("cohort2");
        assertThat(studyConfiguration.getCalculatedStats(), hasItem(cohort1Id));
        assertThat(studyConfiguration.getCalculatedStats(), hasItem(cohort2Id));

        checkCohorts(dbAdaptor, studyConfiguration);

        //Try to recalculate stats for cohort2. Will fail
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, QueryOptions.empty()).first();
        thrown.expect(StorageEngineException.class);
        stats(options, studyConfiguration, cohorts, outputUri.resolve("cohort2.stats"));

    }

    public StudyConfiguration stats(QueryOptions options, StudyConfiguration studyConfiguration, Map<String, Set<String>> cohorts,
                                    URI output) throws IOException, StorageEngineException {
        options.put(DefaultVariantStatisticsManager.OUTPUT, output.toString());
        variantStorageEngine.calculateStats(studyConfiguration.getStudyName(), cohorts, options);
        return dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
    }

    public static StudyConfiguration stats(VariantStatisticsManager vsm, QueryOptions options, StudyConfiguration studyConfiguration,
                                           Map<String, Set<String>> cohorts, Map<String, Integer> cohortIds, VariantDBAdaptor dbAdaptor,
                                           URI resolve) throws IOException, StorageEngineException {
        if (vsm instanceof DefaultVariantStatisticsManager) {
            DefaultVariantStatisticsManager dvsm = (DefaultVariantStatisticsManager) vsm;
            URI stats = dvsm.createStats(dbAdaptor, resolve, cohorts, cohortIds, studyConfiguration, options);
            dvsm.loadStats(dbAdaptor, stats, studyConfiguration, options);
        } else {
            studyConfiguration.getCohortIds().putAll(cohortIds);
            cohorts.forEach((cohort, samples) -> {
                Set<Integer> sampleIds = samples.stream().map(studyConfiguration.getSampleIds()::get).collect(Collectors.toSet());
                studyConfiguration.getCohorts().put(cohortIds.get(cohort), sampleIds);
            });
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
            vsm.calculateStatistics(studyConfiguration.getStudyName(), new ArrayList<>(cohorts.keySet()), options);
        }
        return dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
    }

    static void checkCohorts(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration) {
        for (VariantDBIterator iterator = dbAdaptor.iterator(new Query(), null);
             iterator.hasNext(); ) {
            Variant variant = iterator.next();
            for (StudyEntry sourceEntry : variant.getStudies()) {
                Map<String, VariantStats> cohortsStats = sourceEntry.getStats();
                String calculatedCohorts = cohortsStats.keySet().toString();
                for (Map.Entry<String, Integer> entry : studyConfiguration.getCohortIds().entrySet()) {
                    if (!studyConfiguration.getCalculatedStats().contains(entry.getValue())) {
                        continue;
                    }
                    assertTrue("CohortStats should contain stats for cohort " + entry.getKey() + ". Only contains stats for " +
                                    calculatedCohorts,
                            cohortsStats.containsKey(entry.getKey()));    //Check stats are calculated

                    VariantStats cohortStats = cohortsStats.get(entry.getKey());
                    assertEquals("Stats for cohort " + entry.getKey() + " have less genotypes than expected. "
                                    + cohortStats.getGenotypesCount(),
                            studyConfiguration.getCohorts().get(entry.getValue()).size(),  //Check numGenotypes are correct (equals to
                            // the number of samples)
                            cohortStats.getGenotypesCount().values().stream().reduce(0, (a, b) -> a + b).intValue());

                    HashMap<Genotype, Integer> genotypeCount = new HashMap<>();
                    for (Integer sampleId : studyConfiguration.getCohorts().get(entry.getValue())) {
                        String sampleName = studyConfiguration.getSampleIds().inverse().get(sampleId);
                        String gt = sourceEntry.getSampleData(sampleName, "GT");
                        genotypeCount.compute(new Genotype(gt), (key, value) -> value == null ? 1 : value + 1);
                    }

                    VariantStats stats = VariantStatsCalculator.calculate(variant, genotypeCount);

                    stats.getGenotypesCount().entrySet().removeIf(e -> e.getValue() == 0);
                    stats.getGenotypesFreq().entrySet().removeIf(e -> e.getValue() == 0);
                    cohortStats.getGenotypesCount().entrySet().removeIf(e -> e.getValue() == 0);
                    cohortStats.getGenotypesFreq().entrySet().removeIf(e -> e.getValue() == 0);

                    assertEquals(stats.getGenotypesCount(), cohortStats.getGenotypesCount());
                    assertEquals(stats.getGenotypesFreq(), cohortStats.getGenotypesFreq());
                    assertEquals(stats.getMaf(), cohortStats.getMaf());
                    if (StringUtils.isNotEmpty(stats.getMafAllele()) || StringUtils.isNotEmpty(cohortStats.getMafAllele())) {
                        assertEquals(stats.getMafAllele(), cohortStats.getMafAllele());
                    }
                    assertEquals(stats.getMgf(), cohortStats.getMgf());
                    assertEquals(stats.getRefAlleleFreq(), cohortStats.getRefAlleleFreq());
                    assertEquals(stats.getAltAlleleFreq(), cohortStats.getAltAlleleFreq());
                }
            }
        }
    }

}
