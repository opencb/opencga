/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local.variant.operations;

import org.junit.After;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.local.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;
/**
 * Created by hpccoll1 on 08/07/15.
 */
public class StatsVariantStorageTest extends AbstractVariantStorageOperationTest {

    Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);
    private long all;
    private long[] coh = new long[5];

    private final String userId = "user";
    private final String dbName = "opencga_variants_test";

    public void before () throws Exception {

        File file = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        List<Long> sampleIds = file.getSampleIds();

        for (int i = 0; i < coh.length; i++) {
            coh[i] = catalogManager.createCohort(studyId, "coh" + i, Study.Type.CONTROL_SET, "",
                    sampleIds.subList(sampleIds.size() / coh.length * i, sampleIds.size() / coh.length * (i + 1)), null, sessionId).first().getId();
        }
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        variantManager.index(String.valueOf(file.getId()), createTmpOutdir(file), String.valueOf(outputId), queryOptions, sessionId);


        all = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT),
                new QueryOptions(), sessionId).first().getId();

    }

    public File beforeAggregated(String fileName, VariantSource.Aggregation aggregation) throws Exception {

        Map<String, Object> attributes;
        if (aggregation != null) {
            attributes = Collections.singletonMap(VariantStorageManager.Options.AGGREGATED_TYPE.key(), aggregation);
        } else {
            attributes = Collections.emptyMap();
        }
        catalogManager.modifyStudy(studyId, new ObjectMap(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes), sessionId);

        File file1 = opencga.createFile(studyId, fileName, sessionId);

//        coh0 = catalogManager.createCohort(studyId, "coh0", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();

        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        variantManager.index(String.valueOf(file1.getId()), createTmpOutdir(file1), String.valueOf(outputId), queryOptions, sessionId);
        return file1;
    }

    public String createTmpOutdir(File file) throws CatalogException {
        return createTmpOutdir("_FILE_" + file.getId());
    }

    public String createTmpOutdir(String sufix) throws CatalogException {
        return opencga.createTmpOutdir(studyId, sufix, sessionId);
    }


    public static List<Cohort> createCohorts(String sessionId, long studyId, String tagmapPath, CatalogManager catalogManager, Logger logger) throws IOException, CatalogException {
        List<Cohort> queryResults = new ArrayList<>();
        Properties tagmap = new Properties();
        tagmap.load(new FileInputStream(tagmapPath));
        Map<String, Cohort> cohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult().stream().collect(Collectors.toMap(Cohort::getName, c->c));
        Set<String> catalogCohorts = cohorts.keySet();
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
            if (!catalogCohorts.contains(cohortName)) {
                QueryResult<Cohort> cohort = catalogManager.createCohort(studyId, cohortName, Study.Type.COLLECTION, "", Collections.emptyList(), null, sessionId);
                queryResults.add(cohort.first());
            } else {
                logger.warn("cohort {} was already created", cohortName);
                queryResults.add(cohorts.get(cohortName));
            }
        }
        return queryResults;
    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    private void clearDB(String dbName) {
        logger.info("Cleaning MongoDB {}" , dbName);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager("localhost", 27017);
        MongoDataStore mongoDataStore = mongoManager.get(dbName);
        mongoManager.drop(dbName);
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
        cohorts.put("coh0", catalogManager.getCohort(coh[0], null, sessionId).first());
//        cohorts.put("all", null);
        checkCalculatedStats(cohorts);

//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions(ExecutorManager.EXECUTE, true)).first();
//        assertEquals(Status.READY, job.getStatus().getName());
        cohorts.put("coh1", catalogManager.getCohort(coh[1], null, sessionId).first());
        calculateStats(coh[1]);
        checkCalculatedStats(cohorts);

        calculateStats(coh[2]);
        cohorts.put("coh2", catalogManager.getCohort(coh[2], null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(coh[3]);
        cohorts.put("coh3", catalogManager.getCohort(coh[3], null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(coh[4]);
        cohorts.put("coh4", catalogManager.getCohort(coh[4], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    public void calculateStats(long cohortId) throws Exception {
        calculateStats(cohortId, new QueryOptions());
    }

    public void calculateStats(long cohortId, QueryOptions options) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + cohortId);
        List<String> cohortIds = Collections.singletonList(String.valueOf(cohortId));
        variantManager.stats(String.valueOf(catalogManager.getStudyIdByCohortId(cohortId)), cohortIds, tmpOutdir, String.valueOf(outputId),
                options, sessionId);
    }

    public void calculateStats(List<Long> cohortIds, QueryOptions options) throws Exception {
        calculateStats(options, cohortIds.stream().map(Object::toString).collect(Collectors.toList()));
    }

    public void calculateStats(QueryOptions options, List<String> cohorts) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + cohorts.stream().collect(Collectors.joining("_")));
        variantManager.stats(String.valueOf(studyId), cohorts, tmpOutdir, String.valueOf(outputId), options, sessionId);
    }

    @Test
    public void testCalculateStatsGroups() throws Exception {
        before();
        
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(Arrays.asList(coh[0], coh[1], coh[2]), new QueryOptions());
        cohorts.put("coh0", catalogManager.getCohort(coh[0], null, sessionId).first());
        cohorts.put("coh1", catalogManager.getCohort(coh[1], null, sessionId).first());
        cohorts.put("coh2", catalogManager.getCohort(coh[2], null, sessionId).first());
        checkCalculatedStats(cohorts);

        try {
            calculateStats(Arrays.asList(all, coh[3], -coh[4]), new QueryOptions());
            fail();
        } catch (CatalogException e) {
            logger.info("received expected exception. this is OK, there is no cohort " + (-coh[4]) + "\n");
        }
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(all, null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh[3], null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh[4], null, sessionId).first().getStatus().getName());

        calculateStats(Arrays.asList(all, coh[3], coh[4]), new QueryOptions());
        cohorts.put(DEFAULT_COHORT, catalogManager.getCohort(all, null, sessionId).first());
        cohorts.put("coh3", catalogManager.getCohort(coh[3], null, sessionId).first());
        cohorts.put("coh4", catalogManager.getCohort(coh[4], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStats() throws Exception {
        before();
        

        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh[0], null, sessionId).first().getStatus().getName());

        calculateStats(coh[0]);
        // TODO: Check status "CALCULATING"
//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh0), sessionId, new QueryOptions()).first();
//        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getCohort(coh0, null, sessionId).first().getStatus().getName());
//        runStorageJob(job, sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh[0], null, sessionId).first().getStatus().getName());

        Map<String, Cohort> cohorts = new HashMap<>();
        cohorts.put("coh0", catalogManager.getCohort(coh[0], null, sessionId).first());
        checkCalculatedStats(cohorts);

        catalogManager.modifyCohort(coh[0], new ObjectMap("description", "NewDescription"), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh[0], null, sessionId).first().getStatus().getName());

        catalogManager.modifyCohort(coh[0], new ObjectMap("samples", catalogManager.getCohort(coh[0], null, sessionId).first()
                .getSamples().subList(0, 100)), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getCohort(coh[0], null, sessionId).first().getStatus().getName());

        calculateStats(coh[0]);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh[0], null, sessionId).first().getStatus().getName());
        cohorts.put("coh0", catalogManager.getCohort(coh[0], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateAggregatedStats() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", VariantSource.Aggregation.BASIC);

        calculateAggregatedStats(new QueryOptions());
    }

    @Test
    public void testCalculateAggregatedStatsNonAggregatedStudy() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", null);

        calculateAggregatedStats(new QueryOptions(VariantStorageManager.Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.BASIC));

        Study study = catalogManager.getStudy(studyId, sessionId).first();

        String agg = study.getAttributes().get(VariantStorageManager.Options.AGGREGATED_TYPE.key()).toString();
        assertNotNull(agg);
        assertEquals(VariantSource.Aggregation.BASIC.toString(), agg);
    }

    public void calculateAggregatedStats(QueryOptions options) throws Exception {
//        coh0 = catalogManager.createCohort(studyId, "ALL", Cohort.Type.COLLECTION, "", file.getSampleIds(), null, sessionId).first().getId();

        long cohId = catalogManager.getAllCohorts(studyId, null, null, sessionId).first().getId();

        calculateStats(cohId, options);

        checkCalculatedAggregatedStats(Collections.singleton(DEFAULT_COHORT), dbName);
    }

    @Test
    public void testCalculateAggregatedExacStats() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();
        List<Long> cohorIds = createCohorts(sessionId, studyId, tagMap, catalogManager, logger)
                .stream().map(Cohort::getId).collect(Collectors.toList());

        QueryOptions options = new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(cohorIds, options);

        List<Cohort> cohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getName)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
//        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsExplicitCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(options, Arrays.asList("AFR", "ALL", "AMR", "EAS", "FIN", "NFE", "OTH", "SAS"));

        List<Cohort> cohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getName)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
//        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsWrongExplicitCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);

        thrown.expectMessage("Given cohorts (if any) must match with cohorts in the aggregation mapping file.");
        calculateStats(options, Arrays.asList("AFR", "ALL"));
    }

    @Test
    public void testCalculateAggregatedExacStatsWithoutCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        QueryOptions options = new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(Collections.emptyList(), options);

        List<Cohort> cohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getName)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
//            checkCalculatedAggregatedStats(cohorts, dbName);

    }


    public void checkCalculatedStats(Map<String, Cohort> cohorts) throws Exception {
        checkCalculatedStats(cohorts, catalogManager, dbName, sessionId);
    }

    public static void checkCalculatedStats(Map<String, Cohort> cohorts, CatalogManager catalogManager, String dbName, String sessionId) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager().getDBAdaptor(dbName);

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals("In variant " + variant.toString(), cohorts.size(), sourceEntry.getStats().size());
                for (Map.Entry<String, VariantStats> entry : sourceEntry.getStats().entrySet()) {
                    assertTrue("In variant " + variant.toString(), cohorts.containsKey(entry.getKey()));
                    if (cohorts.get(entry.getKey()) != null) {
                        assertEquals("Variant: " + variant.toString() + " does not have the correct number of samples in cohort '" + entry.getKey() + "'.",
                                cohorts.get(entry.getKey()).getSamples().size(),
                                entry.getValue().getGenotypesCount().values().stream().reduce((integer, integer2) -> integer + integer2).orElse(0).intValue());
                    }
                }
            }
        }
        for (Cohort cohort : cohorts.values()) {
            cohort = catalogManager.getCohort(cohort.getId(), null, sessionId).first();
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
    }

    public static void checkCalculatedAggregatedStats(Set<String> cohortNames, String dbName) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager().getDBAdaptor(dbName);

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