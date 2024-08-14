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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by hpccoll1 on 01/06/15.
 */
@Ignore
public abstract class VariantStatisticsManagerTest extends VariantStorageBaseTest {

    public static final String VCF_TEST_FILE_NAME = "variant-test-file.vcf.gz";
//    public static final String VCF_TEST_FILE_NAME = "variant-test-somatic.vcf";
    protected StudyMetadata studyMetadata;
    protected VariantDBAdaptor dbAdaptor;

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
        studyMetadata = newStudyMetadata();
        clearDB(DB_NAME);
        runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
    }

    @Test
    public void calculateStatsMultiCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts at one time
        checkCohorts(dbAdaptor, studyMetadata);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100);
        Iterator<SampleMetadata> iterator = metadataManager.sampleMetadataIterator(studyMetadata.getId());

        /** Create cohorts **/
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(iterator.next().getName());
        cohort1.add(iterator.next().getName());

        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(iterator.next().getName());
        cohort2.add(iterator.next().getName());

        Map<String, Set<String>> cohorts = new HashMap<>();
        cohorts.put("cohort1", cohort1);
        cohorts.put("cohort2", cohort2);

        //Calculate stats
        stats(options, studyMetadata.getName(), cohorts, outputUri.resolve("cohort1.cohort2.stats"));

        checkCohorts(dbAdaptor, studyMetadata);
    }

    @Test
    public void queryInvalidStats() throws Exception {
        //Calculate stats for 2 cohorts at one time
        checkCohorts(dbAdaptor, studyMetadata);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100);
        Iterator<SampleMetadata> iterator = metadataManager.sampleMetadataIterator(studyMetadata.getId());

        /** Create cohorts **/
        HashSet<String> cohort1Samples = new HashSet<>();
        cohort1Samples.add(iterator.next().getName());
        cohort1Samples.add(iterator.next().getName());

        HashSet<String> cohort2Samples = new HashSet<>();
        cohort2Samples.add(iterator.next().getName());
        cohort2Samples.add(iterator.next().getName());

        Map<String, Set<String>> cohorts = new HashMap<>();
        cohorts.put("cohort1", cohort1Samples);
        cohorts.put("cohort2", cohort2Samples);

        // Just cohort ALL is expected
        VariantQueryResult<Variant> result = variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.LIMIT, 1));
        assertEquals(1, result.first().getStudies().get(0).getStats().size());
        assertEquals(0, result.getEvents().size());

        metadataManager.registerCohort(studyMetadata.getName(), "cohort1", cohort1Samples);

        // Still just cohort ALL is expected, as cohort1 is not ready nor partial
        result = variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.LIMIT, 1));
        assertEquals(1, result.first().getStudies().get(0).getStats().size());
        assertEquals(0, result.getEvents().size());

        //Calculate stats
        stats(options, studyMetadata.getName(), cohorts, outputUri.resolve("cohort1.cohort2.stats"));

        checkCohorts(dbAdaptor, studyMetadata);

        // All 3 cohorts are ready and expected
        result = variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.LIMIT, 1));
        assertEquals(3, result.first().getStudies().get(0).getStats().size());
        assertEquals(0, result.getEvents().size());

        List<Integer> cohort1SampleIds = metadataManager.getCohortMetadata(studyMetadata.getId(), "cohort1").getSamples();
        CohortMetadata cohort2 = metadataManager.addSamplesToCohort(studyMetadata.getId(), "cohort2", cohort1SampleIds);
        assertTrue(cohort2.isInvalid());

        // Cohort2 is invalid, but still all cohorts are expected, but with a warning event
        result = variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.LIMIT, 1));
        assertEquals(3, result.first().getStudies().get(0).getStats().size());
        assertEquals(1, result.getEvents().size());
        assertEquals("Please note that the Cohort Stats for '1000g:cohort2' are currently outdated." +
                " The statistics have been calculated with 2 samples, while the total number of samples in the cohort is 4." +
                " To display updated statistics, please execute variant-stats-index.", result.getEvents().get(0).getMessage());

        VariantStorageEngine engineMock = Mockito.spy(variantStorageEngine);
        VariantStatisticsManager statsManagerMock = Mockito.spy(variantStorageEngine.newVariantStatisticsManager());
        Mockito.doReturn(statsManagerMock).when(engineMock).newVariantStatisticsManager();
        Mockito.doAnswer(invocation -> {
            invocation.callRealMethod();
            throw new StorageEngineException("Mock error calculating stats");
        }).when(statsManagerMock).preCalculateStats(Mockito.any(), Mockito.any(), Mockito.anyList(), Mockito.anyBoolean(), Mockito.any());

        options.put(DefaultVariantStatisticsManager.OUTPUT, outputUri.resolve("stats_mock_fail").toString());
        try {
            engineMock.calculateStats(studyMetadata.getName(), Collections.singletonList(cohort2.getName()), options);
            fail("Expected to fail mock");
        } catch (Exception e) {
            assertEquals("Mock error calculating stats", e.getMessage());
        }

        cohort2 = metadataManager.getCohortMetadata(studyMetadata.getId(), cohort2.getName());
        assertEquals(TaskMetadata.Status.RUNNING, cohort2.getStatsStatus());

        result = variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.LIMIT, 1));
        assertEquals(3, result.first().getStudies().get(0).getStats().size());
        assertEquals(1, result.getEvents().size());
        assertEquals("Please note that the Cohort Stats for '1000g:cohort2' are currently being calculated.",
                result.getEvents().get(0).getMessage());
    }

    @Test
    public void calculateStatsSeparatedCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts separately

        String studyName = studyMetadata.getName();
        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100);
        Iterator<SampleMetadata> iterator = metadataManager.sampleMetadataIterator(studyMetadata.getId());
        StudyMetadata studyMetadata;

        /** Create first cohort **/
        studyMetadata = metadataManager.getStudyMetadata(studyName);
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(iterator.next().getName());
        cohort1.add(iterator.next().getName());

        Map<String, Set<String>> cohorts;

        cohorts = new HashMap<>();
        cohorts.put("cohort1", cohort1);

        //Calculate stats for cohort1
        stats(options, studyMetadata.getName(), cohorts, outputUri.resolve("cohort1.stats"));

        CohortMetadata cohort1Metadata = metadataManager.getCohortMetadata(studyMetadata.getId(), "cohort1");
//        assertThat(studyMetadata.getCalculatedStats(), hasItem(cohort1Id));
        assertTrue(cohort1Metadata.isStatsReady());
        checkCohorts(dbAdaptor, studyMetadata);

        /** Create second cohort **/
        studyMetadata = metadataManager.getStudyMetadata(studyName);
        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(iterator.next().getName());
        cohort2.add(iterator.next().getName());

        cohorts = new HashMap<>();
        cohorts.put("cohort2", cohort2);

        //Calculate stats for cohort2
        stats(options, studyMetadata.getName(), cohorts, outputUri.resolve("cohort2.stats"));

        cohort1Metadata = metadataManager.getCohortMetadata(studyMetadata.getId(), "cohort1");
        CohortMetadata cohort2Metadata = metadataManager.getCohortMetadata(studyMetadata.getId(), "cohort1");
        assertTrue(cohort1Metadata.isStatsReady());
        assertTrue(cohort2Metadata.isStatsReady());

        checkCohorts(dbAdaptor, studyMetadata);

        //Try to recalculate stats for cohort2. Will fail
        studyMetadata = metadataManager.getStudyMetadata(studyName);
        thrown.expect(StorageEngineException.class);
        stats(options, studyMetadata.getName(), cohorts, outputUri.resolve("cohort2.stats"));

    }

    public void stats(QueryOptions options, String study, Map<String, Set<String>> cohorts,
                      URI output) throws IOException, StorageEngineException {
        options.put(DefaultVariantStatisticsManager.OUTPUT, output.toString());
        variantStorageEngine.calculateStats(study, cohorts, options);
    }

    public static StudyMetadata stats(VariantStatisticsManager vsm, QueryOptions options, StudyMetadata studyMetadata,
                                           Map<String, Set<String>> cohorts, Map<String, Integer> cohortIds, VariantDBAdaptor dbAdaptor,
                                           URI resolve) throws IOException, StorageEngineException {
        if (vsm instanceof DefaultVariantStatisticsManager) {
            DefaultVariantStatisticsManager dvsm = (DefaultVariantStatisticsManager) vsm;
            long startTime = System.currentTimeMillis();
            URI stats = dvsm.createStats(dbAdaptor, resolve, cohorts, cohortIds, studyMetadata, options);
            dvsm.loadStats(stats, studyMetadata, options, startTime);
        } else {
            dbAdaptor.getMetadataManager().registerCohorts(studyMetadata.getName(), cohorts);
//            studyMetadata.getCohortIds().putAll(cohortIds);
//            cohorts.forEach((cohort, samples) -> {
//                Set<Integer> sampleIds = samples.stream().map(studyMetadata.getSampleIds()::get).collect(Collectors.toSet());
//                studyMetadata.getCohorts().put(cohortIds.get(cohort), sampleIds);
//            });
//            dbAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata);
            vsm.calculateStatistics(studyMetadata.getName(), new ArrayList<>(cohorts.keySet()), options);
        }
        return dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
    }

    protected static void checkCohorts(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata) {
        Map<String, CohortMetadata> cohorts = new HashMap<>();
        dbAdaptor.getMetadataManager().cohortIterator(studyMetadata.getId()).forEachRemaining(cohort -> {
            cohorts.put(cohort.getName(), cohort);
        });
        for (VariantDBIterator iterator = dbAdaptor.iterator(new VariantQuery().unknownGenotype("0/0").includeSampleAll(), null);
             iterator.hasNext(); ) {
            Variant variant = iterator.next();
            for (StudyEntry studyEntry : variant.getStudies()) {
                Map<String, VariantStats> cohortsStats = studyEntry.getStats()
                        .stream()
                        .collect(Collectors.toMap(VariantStats::getCohortId, i -> i));
                String calculatedCohorts = cohortsStats.keySet().toString();
                for (Map.Entry<String, CohortMetadata> entry : cohorts.entrySet()) {
                    CohortMetadata cohort = entry.getValue();
                    if (!cohort.isStatsReady()) {
                        continue;
                    }
                    assertTrue("CohortStats should contain stats for cohort '" + entry.getKey() + "'. Contains stats for " +
                                    calculatedCohorts,
                            cohortsStats.containsKey(entry.getKey()));    //Check stats are calculated

                    VariantStats cohortStats = cohortsStats.get(entry.getKey());
//                    assertEquals("Stats for cohort " + entry.getKey() + " have less genotypes than expected in variant " + variant + "."
//                                    + cohortStats.getGenotypeCount(),
//                            cohort.getSamples().size(),  //Check numGenotypes are correct (equals to
//                            // the number of samples)
//                            cohortStats.getGenotypeCount().values().stream().reduce(0, (a, b) -> a + b).intValue());
//
                    HashMap<Genotype, Integer> genotypeCount = new HashMap<>();
                    for (Integer sampleId : cohort.getSamples()) {
                        String sampleName = dbAdaptor.getMetadataManager().getSampleName(studyMetadata.getId(), sampleId);
                        String gt = studyEntry.getSampleData(sampleName, "GT");
                        genotypeCount.merge(new Genotype(gt), 1, Integer::sum);
                    }

                    VariantStats stats = VariantStatsCalculator.calculate(variant, genotypeCount, false);
                    stats.setCohortId(cohort.getName());
                    int numFiles = 0;
                    int numQualFiles = 0;
                    double qualSum = 0;
                    for (Integer file : cohort.getFiles()) {
                        String fileName = dbAdaptor.getMetadataManager().getFileName(studyMetadata.getId(), file);
                        FileEntry fileEntry = studyEntry.getFile(fileName);
                        if (fileEntry != null) {
                            if (fileEntry.getCall() != null) {
//                                System.out.println("fileEntry.getCall().getVariantId() = " + fileEntry.getCall().getVariantId());
                                Variant v = new Variant(fileEntry.getCall().getVariantId());
                                if (v.getType().equals(VariantType.NO_VARIATION)) {
                                    continue;
                                }
                                v = new VariantNormalizer().apply(Collections.singletonList(v)).get(fileEntry.getCall().getAlleleIndex());
                                if (!v.sameGenomicVariant(variant)) {
                                    System.out.println("variant   = " + variant);
                                    System.out.println("file call = " + v);
                                    continue;
                                }
                            }
                            VariantStatsCalculator.addFileFilter(fileEntry.getData().get(StudyEntry.FILTER), stats.getFilterCount());
                            numFiles++;
                            String q = fileEntry.getData().get(StudyEntry.QUAL);
                            if (q != null && !q.isEmpty() && !q.equals(".")) {
                                numQualFiles++;
                                qualSum += Double.parseDouble(q);
                            }
                        }
                    }
                    VariantStatsCalculator.calculateFilterFreq(stats, numFiles);
                    stats.setQualityAvg(((float) (qualSum / numQualFiles)));
                    stats.setQualityCount(numQualFiles);

                    stats.getGenotypeCount().entrySet().removeIf(e -> e.getValue() == 0);
                    stats.getGenotypeFreq().entrySet().removeIf(e -> e.getValue() == 0);
                    cohortStats.setGenotypeCount(new HashMap<>(cohortStats.getGenotypeCount())); // Make mutable
                    cohortStats.getGenotypeCount().entrySet().removeIf(e -> e.getValue() == 0);
                    cohortStats.setGenotypeFreq(new HashMap<>(cohortStats.getGenotypeFreq())); // Make mutable
                    cohortStats.getGenotypeFreq().entrySet().removeIf(e -> e.getValue() == 0);

//                    assertEquals(variant.toString(), stats.getGenotypeCount(), cohortStats.getGenotypeCount());
//                    assertEquals(variant.toString(), stats.getGenotypeFreq(), cohortStats.getGenotypeFreq());
//                    assertEquals(variant.toString(), stats.getMaf(), cohortStats.getMaf());
//                    if (StringUtils.isNotEmpty(stats.getMafAllele()) || StringUtils.isNotEmpty(cohortStats.getMafAllele())) {
//                        assertEquals(variant.toString(), stats.getMafAllele(), cohortStats.getMafAllele());
//                    }
//                    assertEquals(variant.toString(), stats.getMgf(), cohortStats.getMgf());
//                    assertEquals(variant.toString() + "-- " + cohortStats.getImpl(), stats.getRefAlleleFreq(), cohortStats.getRefAlleleFreq());
//                    assertEquals(variant.toString() + "-- " + cohortStats.getImpl(), stats.getAltAlleleFreq(), cohortStats.getAltAlleleFreq());
//
//                    assertEquals(variant.toString(), stats.getSamplesCount(), cohortStats.getSamplesCount());
//                    assertEquals(variant.toString(), stats.getFilesCount(), cohortStats.getFilesCount());
//
//                    assertEquals(variant.toString(), stats.getFilterCount(), cohortStats.getFilterCount());
//                    assertEquals(variant.toString(), stats.getFilterFreq(), cohortStats.getFilterFreq());

                    assertEquals(variant.toString(), stats, cohortStats);
                }
            }
        }
    }

}
