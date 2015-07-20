package org.opencb.opencga.analysis.storage.variant;

import org.apache.tools.ant.types.Commandline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.CatalogStudyConfigurationManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.storage.app.StorageMain;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;

/**
 * Created by hpccoll1 on 08/07/15.
 */
public class VariantStorageTest {

    private CatalogManager catalogManager;
    private String sessionId;
    private int projectId;
    private int studyId;
    private FileMetadataReader fileMetadataReader;
    private CatalogFileUtils catalogFileUtils;
    private int outputId;
    Logger logger = LoggerFactory.getLogger(VariantStorageTest.class);
    private int all;
    private int coh1;
    private int coh2;
    private int coh3;
    private int coh4;
    private int coh5;
    private String catalogPropertiesFile;
    private final String userId = "user";
    private final String dbName = "opencga_variants_test";

    @Before
    public void before () throws Exception {
        catalogPropertiesFile = getResourceUri("catalog.properties").getPath();
        Properties properties = new Properties();
        properties.load(CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties"));

        CatalogManagerTest.clearCatalog(properties);
        clearDB(dbName);

        catalogManager = new CatalogManager(properties);
        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new CatalogFileUtils(catalogManager);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject(userId, "p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, "Study 1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", dbName)), null, null, null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();
        File file1 = create("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file2 = create("501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file3 = create("1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file4 = create("1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        File file5 = create("2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        coh1 = catalogManager.createCohort(studyId, "coh1", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();
        coh2 = catalogManager.createCohort(studyId, "coh2", Cohort.Type.CONTROL_SET, "", file2.getSampleIds(), null, sessionId).first().getId();
        coh3 = catalogManager.createCohort(studyId, "coh3", Cohort.Type.CONTROL_SET, "", file3.getSampleIds(), null, sessionId).first().getId();
        coh4 = catalogManager.createCohort(studyId, "coh4", Cohort.Type.CONTROL_SET, "", file4.getSampleIds(), null, sessionId).first().getId();
        coh5 = catalogManager.createCohort(studyId, "coh5", Cohort.Type.CONTROL_SET, "", file5.getSampleIds(), null, sessionId).first().getId();

        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false);
        runStorageJob(analysisFileIndexer.index(file1.getId(), outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index(file2.getId(), outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index(file3.getId(), outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index(file4.getId(), outputId, sessionId, queryOptions).first(), sessionId);
        runStorageJob(analysisFileIndexer.index(file5.getId(), outputId, sessionId, queryOptions).first(), sessionId);

        all = catalogManager.getAllCohorts(studyId, new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.name.toString(), "all"), sessionId).first().getId();

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
        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
//        cohorts.put("all", null);
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh2), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("coh2", catalogManager.getCohort(coh2, null, sessionId).first());
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh3), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("coh3", catalogManager.getCohort(coh3, null, sessionId).first());
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh4), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("coh4", catalogManager.getCohort(coh4, null, sessionId).first());
        checkCalculatedStats(cohorts);

        runStorageJob(variantStorage.calculateStats(outputId, Collections.singletonList(coh5), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("coh5", catalogManager.getCohort(coh5, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStatsGroups() throws Exception {
        VariantStorage variantStorage = new VariantStorage(catalogManager);
        Map<String, Cohort> cohorts = new HashMap<>();

        runStorageJob(variantStorage.calculateStats(outputId, Arrays.asList(coh1, coh2, coh3), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        cohorts.put("coh2", catalogManager.getCohort(coh2, null, sessionId).first());
        cohorts.put("coh3", catalogManager.getCohort(coh3, null, sessionId).first());
        checkCalculatedStats(cohorts);

        try {
            runStorageJob(variantStorage.calculateStats(outputId, Arrays.asList(all, coh4, -coh5), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
            fail();
        } catch (CatalogException e) {
            logger.info("received expected exception. this is OK, there is no cohort " + (-coh5) + "\n");
        }
        assertEquals(Cohort.Status.NONE, catalogManager.getCohort(all, null, sessionId).first().getStatus());
        assertEquals(Cohort.Status.NONE, catalogManager.getCohort(coh4, null, sessionId).first().getStatus());
        assertEquals(Cohort.Status.NONE, catalogManager.getCohort(coh5, null, sessionId).first().getStatus());

        runStorageJob(variantStorage.calculateStats(outputId, Arrays.asList(all, coh4, coh5), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first(), sessionId);
        cohorts.put("all", catalogManager.getCohort(all, null, sessionId).first());
        cohorts.put("coh4", catalogManager.getCohort(coh4, null, sessionId).first());
        cohorts.put("coh5", catalogManager.getCohort(coh5, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStats() throws Exception {
        VariantStorage variantStorage = new VariantStorage(catalogManager);

        assertEquals(Cohort.Status.NONE, catalogManager.getCohort(coh1, null, sessionId).first().getStatus());

        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first();
        assertEquals(Cohort.Status.CALCULATING, catalogManager.getCohort(coh1, null, sessionId).first().getStatus());

        runStorageJob(job, sessionId);
        assertEquals(Cohort.Status.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus());

        Map<String, Cohort> cohorts = new HashMap<>();
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        checkCalculatedStats(cohorts);

        catalogManager.updateCohort(coh1, new ObjectMap("description", "NewDescription"), sessionId);
        assertEquals(Cohort.Status.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus());

        catalogManager.updateCohort(coh1, new ObjectMap("samples", catalogManager.getCohort(coh1, null, sessionId).first().getSamples().subList(0, 100)), sessionId);
        assertEquals(Cohort.Status.INVALID, catalogManager.getCohort(coh1, null, sessionId).first().getStatus());

        job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)).first();
        runStorageJob(job, sessionId);
        assertEquals(Cohort.Status.READY, catalogManager.getCohort(coh1, null, sessionId).first().getStatus());
        cohorts.put("coh1", catalogManager.getCohort(coh1, null, sessionId).first());
        checkCalculatedStats(cohorts);
    }


//    @Test
//    public void testAnnotateVariants() throws Exception {
//
//    }

    public void checkCalculatedStats(Map<String, Cohort> cohorts) throws Exception {
        checkCalculatedStats(cohorts, dbName, catalogPropertiesFile, sessionId);
    }

    public static void checkCalculatedStats(Map<String, Cohort> cohorts, String dbName, String catalogPropertiesFile, String sessionId) throws Exception {
        StorageConfiguration storageConfiguration = StorageConfiguration.load();
        storageConfiguration.getStorageEngine().getVariant().getOptions()
                .append(CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE, catalogPropertiesFile)
                .append(VariantStorageManager.Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key(), CatalogStudyConfigurationManager.class.getName())
                .append("sessionId", sessionId);
        VariantDBAdaptor dbAdaptor = new StorageManagerFactory(storageConfiguration).getVariantStorageManager().getDBAdaptor(dbName);

        for (Variant variant : dbAdaptor) {
            for (VariantSourceEntry sourceEntry : variant.getSourceEntries().values()) {
                assertEquals(cohorts.size(), sourceEntry.getCohortStats().size());
                for (Map.Entry<String, VariantStats> entry : sourceEntry.getCohortStats().entrySet()) {
                    assertTrue(cohorts.containsKey(entry.getKey()));
                    if (cohorts.get(entry.getKey()) != null) {
                        assertEquals("Variant: " + variant.toString() + " does not have the correct number of samples.", cohorts.get(entry.getKey()).getSamples().size(), entry.getValue().getGenotypesCount().values().stream().reduce((integer, integer2) -> integer + integer2).orElse(0).intValue());
                    }
                }
            }
        }
    }

    /**
     * Do not execute Job using its command line, won't find the opencga-storage.sh
     * Call directly to the OpenCGAStorageMain
     */
    private Job runStorageJob(Job storageJob, String sessionId) throws AnalysisExecutionException, IOException, CatalogException {
        return runStorageJob(catalogManager, storageJob, logger, sessionId);
    }

    public static Job runStorageJob(CatalogManager catalogManager, Job storageJob, Logger logger, String sessionId) throws AnalysisExecutionException, IOException, CatalogException {
        logger.info("==========================================");
        logger.info("Executing opencga-storage");
        logger.info("==========================================");
        String[] args = Commandline.translateCommandline(storageJob.getCommandLine());
        StorageMain.Main(Arrays.copyOfRange(args, 1, args.length));
        logger.info("==========================================");
        logger.info("Finish opencga-storage");
        logger.info("==========================================");

        storageJob.setCommandLine("echo 'Executing fake CLI :' " + storageJob.getCommandLine());
        AnalysisJobExecutor.execute(catalogManager, storageJob, sessionId);
        return catalogManager.getJob(storageJob.getId(), null, sessionId).first();
    }
}