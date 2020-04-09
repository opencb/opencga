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

package org.opencb.opencga.storage.core.manager.variant;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.config.Policies;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.OpenCGATestExternalResource;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStoragePipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.manager.variant.operations.StatsVariantStorageTest.checkCalculatedStats;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractVariantStorageOperationTest extends GenericTest {
    protected CatalogManager catalogManager;

    protected String sessionId;

    protected final String userId = "user";

    protected long projectId;
    protected String projectAlias;
    protected long studyId;
    protected String studyStr;
    protected long outputId;
    protected String outputStr;
    protected String outputPath;
    protected long studyId2;
    protected long outputId2;

    private List<File> files;
    private final static String[] FILE_NAMES = {
            "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"};

    protected FileMetadataReader fileMetadataReader;
    protected FileUtils catalogFileUtils;
    protected org.opencb.opencga.storage.core.manager.variant.VariantStorageManager variantManager;

    protected final String dbName = DB_NAME;
    protected static final String STORAGE_ENGINE_DUMMY = DummyVariantStorageEngine.STORAGE_ENGINE_ID;
    protected static final String STORAGE_ENGINE_MONGODB = "mongodb";
    protected static final String STORAGE_ENGINE_HADOOP = "hadoop";
    private Logger logger = LoggerFactory.getLogger(AbstractVariantStorageOperationTest.class);



    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(getStorageEngine().equals(STORAGE_ENGINE_HADOOP));
    private File smallFile;

    @Before
    public final void setUpAbstract() throws Exception {
        catalogManager = opencga.getCatalogManager();
        StorageEngineFactory factory = opencga.getStorageEngineFactory();
        StorageConfiguration storageConfiguration = factory.getStorageConfiguration();
        storageConfiguration.setDefaultStorageEngineId(STORAGE_ENGINE_DUMMY);
        storageConfiguration.getStorageEngines().clear();
        storageConfiguration.getStorageEngines().add(new StorageEngineConfiguration(
                STORAGE_ENGINE_DUMMY,
                new StorageEtlConfiguration(),
                new StorageEtlConfiguration(DummyVariantStorageEngine.class.getName(), new ObjectMap(), new DatabaseCredentials()),
                new ObjectMap()
        ));
        factory.unregisterVariantStorageManager(DummyVariantStorageEngine.STORAGE_ENGINE_ID);

        DummyStudyConfigurationAdaptor.clear();

        variantManager = new org.opencb.opencga.storage.core.manager.variant.VariantStorageManager(catalogManager, factory);

        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new FileUtils(catalogManager);
        Policies policies = new Policies();
        policies.setUserCreation(Policies.UserCreation.ALWAYS);

        User user = catalogManager.getUserManager().create(userId, "User", "user@email.org", "user", "ACME", null, Account.Type.FULL, null).first();
        sessionId = catalogManager.getUserManager().login(userId, "user").getToken();
        projectAlias = "p1";
        projectId = catalogManager.getProjectManager().create(projectAlias, projectAlias, "Project 1", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        studyId = catalogManager.getStudyManager().create(String.valueOf(projectId), "s1", "s1", Study.Type.CASE_CONTROL, null, "Study " +
                "1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore(getStorageEngine(), dbName)),
                null, Collections.singletonMap(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), getAggregation()), null, sessionId)
                .first()
                .getId();
        studyStr = String.valueOf(studyId);
        outputId = catalogManager.getFileManager().createFolder(studyStr, Paths.get("data", "index").toString(), null,  true, null,
                QueryOptions.empty(), sessionId).first().getId();
        outputStr = String.valueOf(outputId);
        outputPath = "data/index/";
        studyId2 = catalogManager.getStudyManager().create(String.valueOf(projectId), "s2", "s2", Study.Type.CASE_CONTROL, null, "Study " +
                "2", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore(getStorageEngine(), dbName)),
                null, Collections.singletonMap(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), getAggregation()), null, sessionId).first().getId();
        outputId2 = catalogManager.getFileManager().createFolder(Long.toString(studyId2), Paths.get("data", "index").toString(), null,
                true, null, QueryOptions.empty(), sessionId).first().getId();

        files = Arrays.asList(new File[5]);
    }

    @After
    public void tearDown() throws Exception {
        DummyStudyConfigurationAdaptor.writeAndClear(opencga.getOpencgaHome());
    }

    protected String getStorageEngine() {
        return STORAGE_ENGINE_DUMMY;
    }

    protected abstract Aggregation getAggregation();

    protected File getFile(int index) throws IOException, CatalogException {
        if (files.get(index) == null) {
            files.set(index, create(FILE_NAMES[index]));
        }
        return files.get(index);
    }

    protected File getSmallFile() throws IOException, CatalogException {
        if (smallFile == null) {
            smallFile = create("variant-test-file.vcf.gz");
        }
        return smallFile;
    }

    protected File create(String resourceName) throws IOException, CatalogException {
        return create(studyId, getResourceUri(resourceName));
    }

    protected File create(long studyId, URI uri) throws IOException, CatalogException {
        return create(studyId, uri, "data/vcfs/");
    }

    protected File create(long studyId, URI uri, String path) throws IOException, CatalogException {
        File file;
        file = fileMetadataReader.create(studyId, uri, path, "", true, null, sessionId).first();
//        File.Format format = FormatDetector.detect(uri);
//        File.Bioformat bioformat = BioformatDetector.detect(uri);
//        file = catalogManager.createFile(studyId, format, bioformat, "data/vcfs/", "", true, -1, sessionId).first();
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFileManager().get(file.getId(), null, sessionId).first();
    }

    protected Cohort getDefaultCohort(long studyId) throws CatalogException {
        return catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new
                QueryOptions(), sessionId).first();
    }


    protected File transformFile(File inputFile, QueryOptions queryOptions) throws CatalogException, IOException, StorageEngineException, URISyntaxException {
        return transformFile(inputFile, queryOptions, "data/index/");
    }

    protected File transformFile(File inputFile, QueryOptions queryOptions, String path) throws CatalogException, IOException, StorageEngineException, URISyntaxException {

        try {
            catalogManager.getFileManager().createFolder(studyStr, path, null, true, null, null, sessionId);
        } catch (CatalogException ignore) {}

        queryOptions.append(VariantFileIndexerStorageOperation.TRANSFORM, true);
        queryOptions.append(VariantFileIndexerStorageOperation.LOAD, false);
        queryOptions.append(StorageOperation.CATALOG_PATH, path);
        boolean calculateStats = queryOptions.getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key());

        long studyId = catalogManager.getFileManager().getStudyId(inputFile.getId());

        //Default cohort should not be modified
        Cohort defaultCohort = getDefaultCohort(studyId);
        String outdir = opencga.createTmpOutdir(studyId, "_TRANSFORM_", sessionId);
        variantManager.index(null, String.valueOf(inputFile.getId()), outdir, queryOptions, sessionId);
        inputFile = catalogManager.getFileManager().get(inputFile.getId(), null, sessionId).first();
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, inputFile.getIndex().getStatus().getName());
        assertNotNull(inputFile.getStats().get(FileMetadataReader.VARIANT_FILE_STATS));

        // Default cohort should not be modified
        assertEquals(defaultCohort, getDefaultCohort(studyId));

        //Get transformed file
        Query searchQuery = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), path)
                .append(FileDBAdaptor.QueryParams.NAME.key(), "~" + inputFile.getName() + ".variants.(json|avro)");

        File transformedFile = catalogManager.getFileManager().get(studyId, searchQuery, new QueryOptions(), sessionId).first();

        List<File.RelatedFile> relatedFiles = transformedFile.getRelatedFiles().stream()
                .filter(relatedFile -> relatedFile.getRelation().equals(File.RelatedFile.Relation.PRODUCED_FROM))
                .collect(Collectors.toList());
        assertEquals(1, relatedFiles.size());
        assertEquals(inputFile.getId(), relatedFiles.get(0).getFile().getId());

        assertEquals(transformedFile.getId(), inputFile.getIndex().getTransformedFile().getId());

        return transformedFile;
    }

    protected List<StoragePipelineResult> loadFile(File file, QueryOptions queryOptions, long outputId) throws Exception {
        return loadFiles(Collections.singletonList(file), queryOptions, outputId);
    }

    protected List<StoragePipelineResult> loadFiles(List<File> files, QueryOptions queryOptions, long outputId) throws Exception {
        return loadFiles(files, files, queryOptions, outputId);
    }

    protected List<StoragePipelineResult> loadFiles(List<File> files, List<File> expectedLoadedFiles, QueryOptions queryOptions, long outputId) throws Exception {
        queryOptions.append(VariantFileIndexerStorageOperation.TRANSFORM, false);
        queryOptions.append(VariantFileIndexerStorageOperation.LOAD, true);
        queryOptions.append(StorageOperation.CATALOG_PATH, String.valueOf(outputId));
        boolean calculateStats = queryOptions.getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key());

        Long studyId = catalogManager.getFileManager().getStudyId(files.get(0).getId());

        List<String> fileIds = files.stream().map(File::getId).map(Object::toString).collect(Collectors.toList());
        String outdir = opencga.createTmpOutdir(studyId, "_LOAD_", sessionId);
        List<StoragePipelineResult> etlResults = variantManager.index(studyId.toString(), fileIds, outdir, queryOptions, sessionId);

        assertEquals(expectedLoadedFiles.size(), etlResults.size());
        checkEtlResults(studyId, etlResults, FileIndex.IndexStatus.READY);

        Cohort defaultCohort = getDefaultCohort(studyId);
        for (File file : expectedLoadedFiles) {
            List<Long> samplesInFile = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
            List<Long> samplesInCohort = defaultCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
            assertTrue(samplesInCohort.containsAll(samplesInFile));
        }
        if (calculateStats) {
            assertEquals(Cohort.CohortStatus.READY, defaultCohort.getStatus().getName());
            checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, defaultCohort), catalogManager, dbName, sessionId);
        }
        return etlResults;
    }

    protected List<StoragePipelineResult> indexFile(File file, QueryOptions queryOptions, long outputId) throws Exception {
        return indexFiles(Collections.singletonList(file), queryOptions, outputId);
    }

    protected List<StoragePipelineResult> indexFiles(List<File> files, QueryOptions queryOptions, long outputId) throws Exception {
        return indexFiles(files, files, queryOptions, outputId);
    }

    protected List<StoragePipelineResult> indexFiles(List<File> files, List<File> expectedLoadedFiles, QueryOptions queryOptions, long outputId) throws Exception {
        queryOptions.append(VariantFileIndexerStorageOperation.TRANSFORM, true);
        queryOptions.append(VariantFileIndexerStorageOperation.LOAD, true);
        queryOptions.append(StorageOperation.CATALOG_PATH, String.valueOf(outputId));
        boolean calculateStats = queryOptions.getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key());

        Long studyId = catalogManager.getFileManager().getStudyId(files.get(0).getId());

        String outdir = opencga.createTmpOutdir(studyId, "_INDEX_", sessionId);
        List<String> fileIds = files.stream().map(File::getId).map(Object::toString).collect(Collectors.toList());

        List<StoragePipelineResult> etlResults = variantManager.index(studyId.toString(), fileIds, outdir, queryOptions, sessionId);

        assertEquals(expectedLoadedFiles.size(), etlResults.size());
        checkEtlResults(studyId, etlResults, FileIndex.IndexStatus.READY);

        Cohort defaultCohort = getDefaultCohort(studyId);
        assertNotNull(defaultCohort);
        for (File file : expectedLoadedFiles) {
            List<Long> samplesInCohort = defaultCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
            List<Long> samplesInFiles = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
            assertTrue(samplesInCohort.containsAll(samplesInFiles));
        }
        if (calculateStats) {
            assertEquals(Cohort.CohortStatus.READY, defaultCohort.getStatus().getName());
            checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, defaultCohort), catalogManager, dbName, sessionId);
        }

        // Check transformed file relations
        for (File inputFile : expectedLoadedFiles) {
            inputFile = catalogManager.getFileManager().get(inputFile.getId(), null, sessionId).first();
            assertNotNull(inputFile.getIndex().getTransformedFile());
            File transformedFile = catalogManager.getFileManager().get(inputFile.getIndex().getTransformedFile().getId(), new QueryOptions(), sessionId).first();

            List<File.RelatedFile> relatedFiles = transformedFile.getRelatedFiles().stream()
                    .filter(relatedFile -> relatedFile.getRelation().equals(File.RelatedFile.Relation.PRODUCED_FROM))
                    .collect(Collectors.toList());
            assertEquals(1, relatedFiles.size());
            assertEquals(inputFile.getId(), relatedFiles.get(0).getFile().getId());
        }


        return etlResults;
    }

    protected void checkEtlResults(long studyId, List<StoragePipelineResult> etlResults, String expectedStatus) throws CatalogException {
        for (StoragePipelineResult etlResult : etlResults) {
            File input = catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.URI.key(), etlResult.getInput()
            ), null, sessionId).first();
            long indexedFileId;
            if (input.getRelatedFiles().isEmpty()) {
                indexedFileId = input.getId();
            } else {
                indexedFileId = input.getRelatedFiles().get(0).getFile().getId();
            }
            assertEquals(expectedStatus, catalogManager.getFileManager().get(indexedFileId, null, sessionId).first().getIndex().getStatus().getName());
            System.out.println("etlResult = " + etlResult);
        }
    }

    protected DummyVariantStorageEngine mockVariantStorageManager() {
        DummyVariantStorageEngine vsm = spy(new DummyVariantStorageEngine());
        vsm.setConfiguration(opencga.getStorageConfiguration(), DummyVariantStorageEngine.STORAGE_ENGINE_ID,
                StorageOperation.buildDatabaseName(catalogManager.getConfiguration().getDatabasePrefix(), userId, projectAlias));
        StorageEngineFactory.get(opencga.getStorageConfiguration()).registerStorageManager(vsm);
        vsm.setConfiguration(opencga.getStorageConfiguration(), DummyVariantStorageEngine.STORAGE_ENGINE_ID, DB_NAME);
        StorageEngineFactory.get(opencga.getStorageConfiguration()).registerStorageManager(vsm);
        return vsm;
    }

    protected DummyVariantDBAdaptor mockVariantDBAdaptor() throws StorageEngineException {
        DummyVariantStorageEngine vsm = mockVariantStorageManager();
        return mockVariantDBAdaptor(vsm);
    }

    protected DummyVariantDBAdaptor mockVariantDBAdaptor(DummyVariantStorageEngine vsm) throws StorageEngineException {
        DummyVariantDBAdaptor dbAdaptor = spy(new DummyVariantDBAdaptor(""));
        doReturn(dbAdaptor).when(vsm).getDBAdaptor();
        return dbAdaptor;
    }

    protected DummyVariantStoragePipeline mockVariantStorageETL() throws StorageEngineException {
        DummyVariantStorageEngine vsm = mockVariantStorageManager();
        return mockVariantStorageETL(vsm);
    }

    protected DummyVariantStoragePipeline mockVariantStorageETL(DummyVariantStorageEngine vsm) throws StorageEngineException {
        DummyVariantStoragePipeline storageETL = spy(vsm.newStoragePipeline(true));
//        doReturn(storageETL).when(vsm).newStoragePipeline(anyBoolean());
        Mockito.doAnswer(invocation -> {
            DummyVariantStoragePipeline etl = (DummyVariantStoragePipeline) invocation.callRealMethod();
            storageETL.init(etl.getOptions());
            return storageETL;
        }).when(vsm).newStoragePipeline(anyBoolean());
        return storageETL;
    }

}
