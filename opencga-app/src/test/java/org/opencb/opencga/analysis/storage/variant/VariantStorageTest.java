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
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;
/**
 * Created by hpccoll1 on 08/07/15.
 */
public class VariantStorageTest {

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    private CatalogManager catalogManager;
    private String sessionId;
    private long projectId;
    private long studyId;
    private FileMetadataReader fileMetadataReader;
    private CatalogFileUtils catalogFileUtils;
    private long outputId;
    Logger logger = LoggerFactory.getLogger(VariantStorageTest.class);
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

        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new CatalogFileUtils(catalogManager);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject(userId, "p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, "Study 1", null, null, null, null,
                Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", dbName)), null, null, null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();
        File file1 = create("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file2 = create("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file3 = create("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file4 = create("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file5 = create("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        coh1 = catalogManager.createCohort(studyId, "coh1", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();
        coh2 = catalogManager.createCohort(studyId, "coh2", Cohort.Type.CONTROL_SET, "", file2.getSampleIds(), null, sessionId).first().getId();
        coh3 = catalogManager.createCohort(studyId, "coh3", Cohort.Type.CONTROL_SET, "", file3.getSampleIds(), null, sessionId).first().getId();
        coh4 = catalogManager.createCohort(studyId, "coh4", Cohort.Type.CONTROL_SET, "", file4.getSampleIds(), null, sessionId).first().getId();
        coh5 = catalogManager.createCohort(studyId, "coh5", Cohort.Type.CONTROL_SET, "", file5.getSampleIds(), null, sessionId).first().getId();

        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        runStorageJob(analysisFileIndexer.index((int) file1.getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index((int) file2.getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index((int) file3.getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index((int) file4.getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index((int) file5.getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);

        all = catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT),
                new QueryOptions(), sessionId).first().getId();

    }

    public File beforeAggregated(String fileName, VariantSource.Aggregation aggregation) throws Exception {
        catalogManager = opencga.getCatalogManager();
        clearDB(dbName);

        catalogManager = opencga.getCatalogManager();
        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new CatalogFileUtils(catalogManager);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject(userId, "p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, "Study 1", null,
                null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", dbName)), null,
                Collections.singletonMap(VariantStorageManager.Options.AGGREGATED_TYPE.key(), aggregation),
                null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();
        File file1 = create(fileName);

//        coh1 = catalogManager.createCohort(studyId, "coh1", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();

        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        runStorageJob(analysisFileIndexer.index((int) file1.getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);
        return file1;
    }


    public static List<QueryResult<Cohort>> createCohorts(String sessionId, long studyId, String tagmapPath, CatalogManager catalogManager, Logger logger) throws IOException, CatalogException {
        List<QueryResult<Cohort>> queryResults = new ArrayList<>();
        Properties tagmap = new Properties();
        tagmap.load(new FileInputStream(tagmapPath));
        Set<String> catalogCohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult().stream().map(Cohort::getName).collect(Collectors.toSet());
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
            if (!catalogCohorts.contains(cohortName)) {
                QueryResult<Cohort> cohort = catalogManager.createCohort(studyId, cohortName, Cohort.Type.COLLECTION, "", Collections.emptyList(), null, sessionId);
                queryResults.add(cohort);
            } else {
                logger.warn("cohort {} was already created", cohortName);
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

    public File create(String resourceName) throws IOException, CatalogException {
        File file;
        URI uri = getResourceUri(resourceName);
        file = fileMetadataReader.create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFile(file.getId(), sessionId).first();
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

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions()).first(), sessionId);
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
//        cohorts.put("all", null);
        checkCalculatedStats(cohorts);

        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh2), sessionId, new QueryOptions(AnalysisJobExecutor.EXECUTE, true)).first();
        assertEquals(Status.READY, job.getStatus().getStatus());
        cohorts.put("coh2", catalogManager.getCohort(coh2, null, sessionId).first());
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh3), sessionId, new QueryOptions()).first(), sessionId);
        cohorts.put("coh3", catalogManager.getCohort(coh3, null, sessionId).first());
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh4), sessionId, new QueryOptions()).first(), sessionId);
        cohorts.put("coh4", catalogManager.getCohort(coh4, null, sessionId).first());
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh5), sessionId, new QueryOptions()).first(), sessionId);
        cohorts.put("coh5", catalogManager.getCohort(coh5, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStatsGroups() throws Exception {
        before();
        
        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        runStorageJob(variantStorage.calculateStats(outputId, Arrays.asList(coh1, coh2, coh3), sessionId, new QueryOptions()).first(), sessionId);
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        cohorts.put("coh2", catalogManager.getCohort(coh2, null, sessionId).first());
        cohorts.put("coh3", catalogManager.getCohort(coh3, null, sessionId).first());
        checkCalculatedStats(cohorts);

        try {
            runStorageJob(variantStorage.calculateStats(outputId, Arrays.asList(all, coh4, -coh5), sessionId, new QueryOptions()).first(), sessionId);
            fail();
        } catch (CatalogException e) {
            logger.info("received expected exception. this is OK, there is no cohort " + (-coh5) + "\n");
        }
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(all, null, sessionId).first().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh4, null, sessionId).first().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh5, null, sessionId).first().getStatus().getStatus());

        runStorageJob(variantStorage.calculateStats(outputId, Arrays.asList(all, coh4, coh5), sessionId, new QueryOptions()).first(), sessionId);
        cohorts.put(DEFAULT_COHORT, catalogManager.getCohort(all, null, sessionId).first());
        cohorts.put("coh4", catalogManager.getCohort(coh4, null, sessionId).first());
        cohorts.put("coh5", catalogManager.getCohort(coh5, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStats() throws Exception {
        before();
        
        VariantStorage variantStorage = new VariantStorage(catalogManager);

        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getStatus());

        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions()).first();
        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getStatus());

        runStorageJob(job, sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getStatus());

        Map<String, Cohort> cohorts = new HashMap<>();
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        checkCalculatedStats(cohorts);

        catalogManager.modifyCohort(coh1, new ObjectMap("description", "NewDescription"), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getStatus());

        catalogManager.modifyCohort(coh1, new ObjectMap("samples", catalogManager.getCohort(coh1, null, sessionId).first()
                .getSamples().subList(0, 100)), new QueryOptions(), sessionId);
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getStatus());

        job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions()).first();
        runStorageJob(job, sessionId);
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus().getStatus());
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateAggregatedStats() throws Exception {
        File file = beforeAggregated("variant-test-aggregated-file.vcf.gz", VariantSource.Aggregation.BASIC);

//        coh1 = catalogManager.createCohort(studyId, "ALL", Cohort.Type.COLLECTION, "", file.getSampleIds(), null, sessionId).first().getId();

        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        runStorageJob(
                variantStorage.calculateStats(
                        outputId,
                        Arrays.asList(catalogManager.getAllCohorts(studyId, null, null, sessionId).first().getId()),
                        sessionId,
                        new QueryOptions()
                ).first(),
                sessionId
        );

        cohorts.put(StudyEntry.DEFAULT_COHORT, new Cohort());
//        cohorts.put("all", null);
        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStats() throws Exception {
        beforeAggregated("exachead.vcf.gz", VariantSource.Aggregation.EXAC);

        String tagMap = getResourceUri("exac-tag-mapping.properties").getPath();
        createCohorts(sessionId, studyId, tagMap, catalogManager, logger);

        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        runStorageJob(
                variantStorage.calculateStats(
                        outputId,
                        Arrays.asList(catalogManager.getAllCohorts(studyId, null, null, sessionId).first().getId()),
                        sessionId,
                        new QueryOptions(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap)
                ).first(),
                sessionId
        );

        Properties tagMapProperties = new Properties();
        tagMapProperties.load(new FileInputStream(tagMap));
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagMapProperties)) {
            cohorts.put(cohortName, new Cohort());
        }
        checkCalculatedAggregatedStats(cohorts, dbName);
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
            assertEquals(Cohort.CohortStatus.READY, cohort.getStatus().getStatus());
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
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());
        return job;
    }

}