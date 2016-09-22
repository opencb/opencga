package org.opencb.opencga.analysis.storage.variant;

import org.junit.After;
import org.junit.Rule;
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
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.VariantFileIndexer;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;
/**
 * Created by hpccoll1 on 08/07/15.
 */
public class StatsVariantStorageTest {

    private static final String STORAGE_ENGINE = "mongodb";
//    private static final String STORAGE_ENGINE = "hadoop";
    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    private CatalogManager catalogManager;
    private String sessionId;
    private long projectId;
    private long studyId;
    private long outputId;
    Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);
    private long all;
    private long coh1;
    private long coh2;
    private long coh3;
    private long coh4;
    private long coh5;

    private final String userId = "user";
    private final String dbName = "opencga_variants_test";

    public void before () throws Exception {
        catalogManager = opencga.getCatalogManager();
        clearDB(dbName);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject("p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, "Study 1", null, null, null, null,
                Collections.singletonMap(File.Bioformat.VARIANT, new DataStore(STORAGE_ENGINE, dbName)), null, null, null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), true, null, sessionId).first().getId();
        File file1 = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file2 = opencga.createFile(studyId, "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file3 = opencga.createFile(studyId, "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file4 = opencga.createFile(studyId, "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file5 = opencga.createFile(studyId, "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        coh1 = catalogManager.createCohort(studyId, "coh1", Study.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();
        coh2 = catalogManager.createCohort(studyId, "coh2", Study.Type.CONTROL_SET, "", file2.getSampleIds(), null, sessionId).first().getId();
        coh3 = catalogManager.createCohort(studyId, "coh3", Study.Type.CONTROL_SET, "", file3.getSampleIds(), null, sessionId).first().getId();
        coh4 = catalogManager.createCohort(studyId, "coh4", Study.Type.CONTROL_SET, "", file4.getSampleIds(), null, sessionId).first().getId();
        coh5 = catalogManager.createCohort(studyId, "coh5", Study.Type.CONTROL_SET, "", file5.getSampleIds(), null, sessionId).first().getId();

        VariantFileIndexer fileIndexer = new VariantFileIndexer(catalogManager.getCatalogConfiguration(), opencga.getStorageConfiguration());
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false).append(VariantFileIndexer.CATALOG_PATH, outputId);
        fileIndexer.index("" + file1.getId(), createTmpOutdir(file1), sessionId, queryOptions);
        fileIndexer.index("" + file2.getId(), createTmpOutdir(file2), sessionId, queryOptions);
        fileIndexer.index("" + file3.getId(), createTmpOutdir(file3), sessionId, queryOptions);
        fileIndexer.index("" + file4.getId(), createTmpOutdir(file4), sessionId, queryOptions);
        fileIndexer.index("" + file5.getId(), createTmpOutdir(file5), sessionId, queryOptions);


        all = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT),
                new QueryOptions(), sessionId).first().getId();

    }

    public File beforeAggregated(String fileName, VariantSource.Aggregation aggregation) throws Exception {
        catalogManager = opencga.getCatalogManager();
        clearDB(dbName);

        catalogManager = opencga.getCatalogManager();

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject("p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        Map<String, Object> attributes;
        if (aggregation != null) {
            attributes = Collections.singletonMap(VariantStorageManager.Options.AGGREGATED_TYPE.key(), aggregation);
        } else {
            attributes = Collections.emptyMap();
        }
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, "Study 1", null,
                null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", dbName)), null,
                attributes,
                null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), true, null, sessionId).first().getId();
        File file1 = opencga.createFile(studyId, fileName, sessionId);

//        coh1 = catalogManager.createCohort(studyId, "coh1", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();

        VariantFileIndexer fileIndexer = new VariantFileIndexer(catalogManager.getCatalogConfiguration(), opencga.getStorageConfiguration());
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        fileIndexer.index("" + file1.getId(), createTmpOutdir(file1), sessionId, queryOptions);
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
        
        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(variantStorage, coh1);
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
//        cohorts.put("all", null);
        checkCalculatedStats(cohorts);

//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh2), sessionId, new QueryOptions(ExecutorManager.EXECUTE, true)).first();
//        assertEquals(Status.READY, job.getStatus().getName());
        cohorts.put("coh2", catalogManager.getCohort(coh2, null, sessionId).first());
        calculateStats(variantStorage, coh2);
        checkCalculatedStats(cohorts);

        calculateStats(variantStorage, coh3);
        cohorts.put("coh3", catalogManager.getCohort(coh3, null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(variantStorage, coh4);
        cohorts.put("coh4", catalogManager.getCohort(coh4, null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(variantStorage, coh5);
        cohorts.put("coh5", catalogManager.getCohort(coh5, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    public void calculateStats(VariantStorage variantStorage, long cohortId) throws Exception {
        calculateStats(variantStorage, cohortId, new QueryOptions());
    }

    public void calculateStats(VariantStorage variantStorage, long cohortId, QueryOptions options) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + cohortId);
        List<Long> cohortIds = Collections.singletonList(cohortId);
        variantStorage.calculateStats(catalogManager.getStudyIdByCohortId(cohortId), cohortIds, String.valueOf(outputId), tmpOutdir, sessionId, options);
    }

    public void calculateStats(VariantStorage variantStorage, List<Long> cohortIds, QueryOptions options) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + cohortIds.stream().map(Object::toString).collect(Collectors.joining("_")));
        variantStorage.calculateStats(studyId, cohortIds, String.valueOf(outputId), tmpOutdir, sessionId, options);
    }

    @Test
    public void testCalculateStatsGroups() throws Exception {
        before();
        
        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(variantStorage, Arrays.asList(coh1, coh2, coh3), new QueryOptions());
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        cohorts.put("coh2", catalogManager.getCohort(coh2, null, sessionId).first());
        cohorts.put("coh3", catalogManager.getCohort(coh3, null, sessionId).first());
        checkCalculatedStats(cohorts);

        try {
            calculateStats(variantStorage, Arrays.asList(all, coh4, -coh5), new QueryOptions());
            fail();
        } catch (CatalogException e) {
            logger.info("received expected exception. this is OK, there is no cohort " + (-coh5) + "\n");
        }
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(all, null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh4, null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh5, null, sessionId).first().getStatus().getName());

        calculateStats(variantStorage, Arrays.asList(all, coh4, coh5), new QueryOptions());
        cohorts.put(DEFAULT_COHORT, catalogManager.getCohort(all, null, sessionId).first());
        cohorts.put("coh4", catalogManager.getCohort(coh4, null, sessionId).first());
        cohorts.put("coh5", catalogManager.getCohort(coh5, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStats() throws Exception {
        before();
        
        VariantStorage variantStorage = new VariantStorage(catalogManager);

        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getName());

        calculateStats(variantStorage, coh1);
        // TODO: Check status "CALCULATING"
//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions()).first();
//        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getName());
//        runStorageJob(job, sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getName());

        Map<String, Cohort> cohorts = new HashMap<>();
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        checkCalculatedStats(cohorts);

        catalogManager.modifyCohort(coh1, new ObjectMap("description", "NewDescription"), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getName());

        catalogManager.modifyCohort(coh1, new ObjectMap("samples", catalogManager.getCohort(coh1, null, sessionId).first()
                .getSamples().subList(0, 100)), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getName());

        calculateStats(variantStorage, coh1);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getName());
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
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
//        coh1 = catalogManager.createCohort(studyId, "ALL", Cohort.Type.COLLECTION, "", file.getSampleIds(), null, sessionId).first().getId();

        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        long cohId = catalogManager.getAllCohorts(studyId, null, null, sessionId).first().getId();

        calculateStats(variantStorage, cohId, options);

        cohorts.put(StudyEntry.DEFAULT_COHORT, new Cohort());
//        cohorts.put("all", null);
        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStats() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();
        List<Long> cohorIds = createCohorts(sessionId, studyId, tagMap, catalogManager, logger)
                .stream().map(Cohort::getId).collect(Collectors.toList());


        VariantStorage variantStorage = new VariantStorage(catalogManager);

        QueryOptions options = new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        calculateStats(variantStorage, cohorIds, options);

        Map<String, Cohort> cohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult()
                .stream()
                .collect(Collectors.toMap(Cohort::getName, Function.identity()));
        assertEquals(8, cohorts.size());
        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsWithoutCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();

        try {
            VariantStorage variantStorage = new VariantStorage(catalogManager);

            QueryOptions options = new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
            calculateStats(variantStorage, Collections.emptyList(), options);

            Map<String, Cohort> cohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult()
                    .stream()
                    .collect(Collectors.toMap(Cohort::getName, Function.identity()));
            assertEquals(8, cohorts.size());
            checkCalculatedAggregatedStats(cohorts, dbName);
        } catch (AssertionError e) {
            List<Cohort> result = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult();
            for (Cohort cohort : result) {
                System.out.println("cohort.getName() = " + cohort.getName());
            }
            throw e;
        }
    }

    //    @Test
//    public void testAnnotateVariants() throws Exception {
//
//    }

    public void checkCalculatedStats(Map<String, Cohort> cohorts) throws Exception {
        checkCalculatedStats(cohorts, catalogManager, dbName, sessionId);
    }

    public static void checkCalculatedStats(Map<String, Cohort> cohorts, CatalogManager catalogManager, String dbName, String sessionId) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager().getDBAdaptor(dbName);

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals("In variant " +variant.toString(), cohorts.size(), sourceEntry.getStats().size());
                for (Map.Entry<String, VariantStats> entry : sourceEntry.getStats().entrySet()) {
                    assertTrue("In variant " +variant.toString(), cohorts.containsKey(entry.getKey()));
                    if (cohorts.get(entry.getKey()) != null) {
                        assertEquals("Variant: " + variant.toString() + " does not have the correct number of samples.", cohorts.get(entry.getKey()).getSamples().size(), entry.getValue().getGenotypesCount().values().stream().reduce((integer, integer2) -> integer + integer2).orElse(0).intValue());
                    }
                }
            }
        }
        for (Cohort cohort : cohorts.values()) {
            cohort = catalogManager.getCohort(cohort.getId(), null, sessionId).first();
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getName());
        }
    }

    public static void checkCalculatedAggregatedStats(Map<String, Cohort> cohorts, String dbName) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager().getDBAdaptor(dbName);

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(cohorts.size(), sourceEntry.getStats().size());
                for (Map.Entry<String, VariantStats> entry : sourceEntry.getStats().entrySet()) {
                    assertTrue(cohorts.containsKey(entry.getKey()));
                }
            }
        }
    }
    /**
     * Do not execute Job using its command line, won't find the opencga-storage.sh
     * Call directly to the OpenCGAStorageMain
     */
    private Job runStorageJob(Job storageJob, String sessionId) throws AnalysisExecutionException, IOException, CatalogException {
//        storageJob.setCommandLine(storageJob.getCommandLine() + " --job-id " + storageJob.getId());
        Job job = opencga.runStorageJob(storageJob, sessionId);
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
        return job;
    }

}