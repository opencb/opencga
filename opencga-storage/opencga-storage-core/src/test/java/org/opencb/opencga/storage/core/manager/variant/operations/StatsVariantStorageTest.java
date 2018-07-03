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

package org.opencb.opencga.storage.core.manager.variant.operations;

import org.junit.After;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;
/**
 *
 * Created by hpccoll1 on 08/07/15.
 */
public class StatsVariantStorageTest extends AbstractVariantStorageOperationTest {

    Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);
    private String all;
    private String[] coh = new String[5];

    public void before () throws Exception {

        File file = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        for (int i = 0; i < coh.length; i++) {
            Cohort cohort = catalogManager.getCohortManager().create(studyId, "coh" + i, Study.Type.CONTROL_SET, "", file.getSamples().subList(file.getSamples()
                    .size() / coh.length * i, file.getSamples().size() / coh.length * (i + 1)), null, null, sessionId).first();
            coh[i] = cohort.getId();
        }
        QueryOptions queryOptions = new QueryOptions(VariantStorageEngine.Options.ANNOTATE.key(), false);
        queryOptions.putIfNotNull(StorageOperation.CATALOG_PATH, outputId);
        variantManager.index(studyId, file.getId(), createTmpOutdir(file), queryOptions, sessionId);

        all = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                DEFAULT_COHORT), new QueryOptions(), sessionId).first().getId();
    }

    public File beforeAggregated(String fileName, Aggregation aggregation) throws Exception {

        Map<String, Object> attributes;
        if (aggregation != null) {
            attributes = Collections.singletonMap(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), aggregation);
        } else {
            attributes = Collections.emptyMap();
        }
        catalogManager.getStudyManager().update(studyId, new ObjectMap(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes), null, sessionId);

        File file1 = opencga.createFile(studyId, fileName, sessionId);

//        coh0 = catalogManager.createCohort(studyId, "coh0", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();

        QueryOptions queryOptions = new QueryOptions(VariantStorageEngine.Options.ANNOTATE.key(), false);
        queryOptions.putIfNotNull(StorageOperation.CATALOG_PATH, outputId);
        variantManager.index(studyId, file1.getId(), createTmpOutdir(file1), queryOptions, sessionId);
        return file1;
    }

    public String createTmpOutdir(File file) throws CatalogException, IOException {
        return createTmpOutdir("_FILE_" + file.getUid());
    }

    public String createTmpOutdir(String sufix) throws CatalogException, IOException {
        return opencga.createTmpOutdir(studyId, sufix, sessionId);
    }


    public static List<Cohort> createCohorts(String sessionId, String studyId, String tagmapPath, CatalogManager catalogManager, Logger
            logger) throws IOException, CatalogException {
        List<Cohort> queryResults = new ArrayList<>();
        Properties tagmap = new Properties();
        tagmap.load(new FileInputStream(tagmapPath));
        Map<String, Cohort> cohorts = catalogManager.getCohortManager().get(studyId, new Query(), null, sessionId)
                .getResult().stream().collect(Collectors.toMap(Cohort::getId, c->c));
        Set<String> catalogCohorts = cohorts.keySet();
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
            if (!catalogCohorts.contains(cohortName)) {
                QueryResult<Cohort> cohort = catalogManager.getCohortManager().create(studyId, new Cohort()
                        .setId(cohortName)
                        .setName(cohortName)
                        .setSamples(Collections.emptyList())
                        .setType(Study.Type.COLLECTION), null, sessionId);
                queryResults.add(cohort.first());
            } else {
                logger.warn("cohort {} was already created", cohortName);
                queryResults.add(cohorts.get(cohortName));
            }
        }
        return queryResults;
    }

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @After
    public void after () throws Exception {
//        catalogManagerTest.tearDown();
    }

    @Test
    public void testCalculateStatsOneByOne() throws Exception {
        before();
        
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(coh[0]);
        cohorts.put(coh[0], catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
//        cohorts.put("all", null);
        checkCalculatedStats(cohorts);

//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions(ExecutorManager.EXECUTE, true)).first();
//        assertEquals(Status.READY, job.getStatus().getName());
        cohorts.put(coh[1], catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first());
        calculateStats(coh[1]);
        checkCalculatedStats(cohorts);

        calculateStats(coh[2]);
        cohorts.put(coh[2], catalogManager.getCohortManager().get(studyId, coh[2], null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(coh[3]);
        cohorts.put(coh[3], catalogManager.getCohortManager().get(studyId, coh[3], null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(coh[4]);
        cohorts.put(coh[4], catalogManager.getCohortManager().get(studyId, coh[4], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    public void calculateStats(String cohortId) throws Exception {
        calculateStats(cohortId, new QueryOptions());
    }

    public void calculateStats(String cohortId, QueryOptions options) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + cohortId);
        List<String> cohortIds = Collections.singletonList(cohortId);
        options.put(StorageOperation.CATALOG_PATH, outputId);
        variantManager.stats(studyId, cohortIds, tmpOutdir, options, sessionId);
    }

    public void calculateStats(QueryOptions options, String... cohortIds) throws Exception {
        calculateStats(options, Arrays.stream(cohortIds).collect(Collectors.toList()));
    }

    public void calculateStats(QueryOptions options, List<String> cohorts) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + cohorts.stream().collect(Collectors.joining("_")));
        options.put(StorageOperation.CATALOG_PATH, outputId);
        variantManager.stats(studyId, cohorts, tmpOutdir, options, sessionId);
    }

    @Test
    public void testCalculateStatsGroups() throws Exception {
        before();
        
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(new QueryOptions(), coh[0], coh[1], coh[2]);
        cohorts.put(coh[0], catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
        cohorts.put(coh[1], catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first());
        cohorts.put(coh[2], catalogManager.getCohortManager().get(studyId, coh[2], null, sessionId).first());
        checkCalculatedStats(cohorts);

        try {
            calculateStats(new QueryOptions(), all, coh[3], "-" + coh[4]);
            fail();
        } catch (CatalogException e) {
            logger.info("received expected exception. this is OK, there is no cohort " + ("-" + coh[4]) + '\n');
        }
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, "ALL", null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, coh[3], null, sessionId).first().getStatus().getName());

        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, coh[4], null, sessionId).first().getStatus().getName());

        calculateStats(new QueryOptions(), all, coh[3], coh[4]);
        cohorts.put(DEFAULT_COHORT, catalogManager.getCohortManager().get(studyId, DEFAULT_COHORT, null, sessionId).first());
        cohorts.put(coh[3], catalogManager.getCohortManager().get(studyId, coh[3], null, sessionId).first());
        cohorts.put(coh[4], catalogManager.getCohortManager().get(studyId, coh[4], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStats() throws Exception {
        before();


        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getStatus().getName());

        calculateStats(coh[0]);
        // TODO: Check status "CALCULATING"
//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh0), sessionId, new QueryOptions()).first();
//        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getCohort(coh0, null, sessionId).first().getStatus().getName());
//        runStorageJob(job, sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getStatus().getName());

        Map<String, Cohort> cohorts = new HashMap<>();
        cohorts.put("coh0", catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
        checkCalculatedStats(cohorts);

        catalogManager.getCohortManager().update(studyId, coh[0], new ObjectMap("description", "NewDescription"), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getStatus().getName());

        List<String> newCohort = catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getSamples().stream()
                .map(Sample::getId)
                .skip(10).limit(100)
                .collect(Collectors.toList());
        catalogManager.getCohortManager().update(studyId, coh[0], new ObjectMap("samples", newCohort), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getStatus().getName());

        calculateStats(coh[0]);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getStatus().getName());
        cohorts.put("coh0", catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }


    @Test
    public void testCalculateInvalidStats() throws Exception {
        before();

        calculateStats(coh[0]);

        DummyVariantStorageEngine vsm = mockVariantStorageManager();
        String message = "Error";
        doThrow(new StorageEngineException(message)).when(vsm).calculateStats(any(), any(List.class), any());
        doThrow(new StorageEngineException(message)).when(vsm).calculateStats(any(), any(Map.class), any());

        try {
            calculateStats(coh[1]);
            fail();
        } catch (StorageEngineException e) {
            assertEquals(message, e.getCause().getMessage());
        }

        Cohort coh1 = catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first();
        assertEquals(Cohort.CohortStatus.INVALID, coh1.getStatus().getName());

        vsm = mockVariantStorageManager();
        calculateStats(coh[1]);
    }

    @Test
    public void testResumeCalculateStats() throws Exception {
        before();

        calculateStats(coh[0]);

        catalogManager.getCohortManager().setStatus(studyId, coh[1], Cohort.CohortStatus.CALCULATING, "", sessionId);
        Cohort coh1 = catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first();
        Exception expected = VariantStatsStorageOperation.unableToCalculateCohortCalculating(coh1);
        try {
            calculateStats(coh[1]);
            fail();
        } catch (Exception e) {
            assertThat(e, instanceOf(expected.getClass()));
            assertThat(e, hasMessage(is(expected.getMessage())));
        }

        calculateStats(coh[1], new QueryOptions(VariantStorageEngine.Options.RESUME.key(), true));

    }

    @Test
    public void testCalculateAggregatedStats() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", Aggregation.BASIC);

        calculateAggregatedStats(new QueryOptions());
    }

    @Test
    public void testCalculateAggregatedStatsWithoutCohorts() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", Aggregation.BASIC);

        calculateStats(new QueryOptions());
    }

    @Test
    public void testCalculateAggregatedStatsNonAggregatedStudy() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", null);

        calculateAggregatedStats(new QueryOptions(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), Aggregation.BASIC));

        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();

        String agg = study.getAttributes().get(VariantStorageEngine.Options.AGGREGATED_TYPE.key()).toString();
        assertNotNull(agg);
        assertEquals(Aggregation.BASIC.toString(), agg);
    }

    public void calculateAggregatedStats(QueryOptions options) throws Exception {
//        coh0 = catalogManager.createCohort(studyId, "ALL", Cohort.Type.COLLECTION, "", file.getSampleIds(), null, sessionId).first().getId();

        String cohId = catalogManager.getCohortManager().get(studyId, (Query) null, null, sessionId).first().getId();

        calculateStats(cohId, options);

        checkCalculatedAggregatedStats(Collections.singleton(DEFAULT_COHORT), dbName);
    }

    @Test
    public void testCalculateAggregatedExacStats() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();
        List<String> cohortIds = createCohorts(sessionId, studyId, tagMap, catalogManager, logger)
                .stream().map(Cohort::getId).map(Object::toString).collect(Collectors.toList());

        QueryOptions options = new QueryOptions(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(options, cohortIds);

        List<Cohort> cohorts = catalogManager.getCohortManager().get(studyId, (Query) null, null, sessionId).getResult();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
//        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsExplicitCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(options, Arrays.asList("AFR", "ALL", "AMR", "EAS", "FIN", "NFE", "OTH", "SAS"));

        List<Cohort> cohorts = catalogManager.getCohortManager().get(studyId, (Query) null, null, sessionId).getResult();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
//        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsWrongExplicitCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);

        thrown.expectMessage(VariantStatsStorageOperation.differentCohortsThanMappingFile().getMessage());
        calculateStats(options, Arrays.asList("AFR", "ALL"));
    }

    @Test
    public void testCalculateAggregatedExacMissingAggregationMappingFile() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        QueryOptions options = new QueryOptions();

        thrown.expectMessage(VariantStatsStorageOperation.missingAggregationMappingFile(Aggregation.EXAC).getMessage());
        calculateStats(options, Collections.emptyList());
    }

    @Test
    public void testCalculateNonAggregatedWithAggregationMappingFile() throws Exception {
        before();

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);

        thrown.expectMessage(VariantStatsStorageOperation.nonAggregatedWithMappingFile().getMessage());
        calculateStats(options, Arrays.asList("ALL"));
    }

    @Test
    public void testCalculateAggregatedExacStatsWithoutCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(options);

        List<Cohort> cohorts = catalogManager.getCohortManager().get(studyId, (Query) null, null, sessionId).getResult();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
//            checkCalculatedAggregatedStats(cohorts, dbName);

    }


    public void checkCalculatedStats(Map<String, Cohort> cohorts) throws Exception {
        checkCalculatedStats(studyId, cohorts, catalogManager, dbName, sessionId);
    }

    public static void checkCalculatedStats(String studyId, Map<String, Cohort> cohorts, CatalogManager catalogManager, String dbName, String sessionId) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageEngineFactory.get().getVariantStorageEngine(null, dbName).getDBAdaptor();

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals("In variant " + variant.toString(), cohorts.size(), sourceEntry.getStats().size());
                for (Map.Entry<String, VariantStats> entry : sourceEntry.getStats().entrySet()) {
                    assertTrue("In variant " + variant.toString(), cohorts.containsKey(entry.getKey()));
                    if (cohorts.get(entry.getKey()) != null) {
                        assertEquals("Variant: " + variant.toString() + " does not have the correct number of samples in cohort '" + entry.getKey() + "'. jsonVariant: " + variant.toJson() ,
                                cohorts.get(entry.getKey()).getSamples().size(),
                                entry.getValue().getGenotypesCount().values().stream().reduce(Integer::sum).orElse(0).intValue());
                    }
                }
            }
        }
        for (Cohort cohort : cohorts.values()) {
            cohort = catalogManager.getCohortManager().get(studyId, cohort.getId(), null, sessionId).first();
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
    }

    public static void checkCalculatedAggregatedStats(Set<String> cohortNames, String dbName) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageEngineFactory.get().getVariantStorageEngine(null, dbName).getDBAdaptor();

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(cohortNames, sourceEntry.getStats().keySet());
                for (Map.Entry<String, VariantStats> entry : sourceEntry.getStats().entrySet()) {
                    assertTrue(cohortNames.contains(entry.getKey()));
                }
            }
        }
    }
    /**
     * Do not execute Job using its command line, won't find the opencga-storage.sh
     * Call directly to the OpenCGAStorageMain
     */
    private Job runStorageJob(Job storageJob, String sessionId) throws IOException, CatalogException {
//        storageJob.setCommandLine(storageJob.getCommandLine() + " --job-id " + storageJob.getId());
        Job job = opencga.runStorageJob(storageJob, sessionId);
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
        return job;
    }

}