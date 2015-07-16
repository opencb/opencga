package org.opencb.opencga.analysis.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.storage.variant.VariantStorageTest;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.opencb.opencga.analysis.storage.variant.VariantStorageTest.runStorageJob;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class AnalysisFileIndexerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private CatalogManager catalogManager;
    private String sessionId;
    private int projectId;
    private int studyId;
    private FileMetadataReader fileMetadataReader;
    private CatalogFileUtils catalogFileUtils;
    private int outputId;
    Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerTest.class);
    private String catalogPropertiesFile;
    private final String userId = "user";
    private final String dbName = DB_NAME;
    private List<File> files = new ArrayList<>();

    @Before
    public void before() throws Exception {
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
        files.add(create("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));

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

    @Test
    public void testIndexWithStats() throws Exception {

        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(0).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.NONE, getDefaultCohort().getStatus());

        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(1).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(1000, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.NONE, getDefaultCohort().getStatus());

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(2).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(1500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.READY, getDefaultCohort().getStatus());

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(3).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(2000, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.INVALID, getDefaultCohort().getStatus());

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(4).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(2504, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.READY, getDefaultCohort().getStatus());
    }

    private Cohort getDefaultCohort() throws CatalogException {
        return catalogManager.getAllCohorts(studyId, new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.name.toString(), VariantSourceEntry.DEFAULT_COHORT), sessionId).first();
    }

    @Test
    public void testIndexBySteps() throws Exception {
        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(AnalysisFileIndexer.PARAMETERS, "-D" + CatalogStudyConfigurationManager.CATALOG_PROPERTIES_FILE + "=" + catalogPropertiesFile)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false).append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        queryOptions.append(AnalysisFileIndexer.TRANSFORM, true);
        queryOptions.append(AnalysisFileIndexer.LOAD, false);

        //Create transform index job
        Job job = analysisFileIndexer.index(files.get(0).getId(), outputId, sessionId, queryOptions).first();
        assertEquals(Index.Status.TRANSFORMING, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus());

        //Run transform index job
        job = runStorageJob(catalogManager, job, logger, sessionId);
        assertEquals(500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.NONE, getDefaultCohort().getStatus());
        assertEquals(Index.Status.TRANSFORMED, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus());
        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Get transformed file
        QueryOptions searchOptions = new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.id.toString(), job.getOutput())
                .append(CatalogFileDBAdaptor.FileFilterOption.name.toString(), "~variants.json");
        File transformedFile = catalogManager.getAllFiles(studyId, searchOptions, sessionId).first();
        assertEquals(job.getId(), transformedFile.getJobId());

        //Create load index job
        queryOptions.append(AnalysisFileIndexer.TRANSFORM, false);
        queryOptions.append(AnalysisFileIndexer.LOAD, true);
        job = analysisFileIndexer.index(transformedFile.getId(), outputId, sessionId, queryOptions).first();
        assertEquals(Index.Status.LOADING, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus());
        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Run load index job
        runStorageJob(catalogManager, job, logger, sessionId);
        assertEquals(500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.Status.READY, getDefaultCohort().getStatus());
        assertEquals(Index.Status.READY, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus());
    }
}