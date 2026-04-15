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

package org.opencb.opencga.storage.mongodb.variant;

import com.google.common.collect.Iterators;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.exceptions.MongoVariantStorageEngineException;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;


/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@Category(LongTests.class)
public class MongoVariantStorageEngineTest extends VariantStorageEngineTest implements MongoDBVariantStorageTest {

    @Before
    public void setUp() throws Exception {
        System.out.println("VariantMongoDBAdaptor.NUMBER_INSTANCES on setUp() " + VariantMongoDBAdaptor.NUMBER_INSTANCES.get());
    }

    @After
    public void tearDown() throws Exception {
        closeConnections();
        System.out.println("VariantMongoDBAdaptor.NUMBER_INSTANCES on tearDown() " + VariantMongoDBAdaptor.NUMBER_INSTANCES.get());
    }

    @Test
    public void stageResumeFromErrorTest() throws Exception {
        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantStorageMetadataManager metadataManager = variantStorageManager.getDBAdaptor().getMetadataManager();

        StudyMetadata studyMetadata = newStudyMetadata();
        int fileId = metadataManager.registerFile(studyMetadata.getId(), smallInputUri.getPath());

        TaskMetadata task = metadataManager.addRunningTask(studyMetadata.getId(), MongoDBVariantStorageOptions.STAGE.key(), Collections.singletonList(fileId));
        metadataManager.updateTask(studyMetadata.getId(), task.getId(), t -> {
            t.getStatus().clear();
            t.addStatus(new Date(System.currentTimeMillis() - 100), TaskMetadata.Status.RUNNING);
            t.addStatus(new Date(System.currentTimeMillis() - 50), TaskMetadata.Status.ERROR);
            // Last status is ERROR
        });

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE_RESUME.key(), false)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
        );

    }

    @Test
    public void stageForceResumeTest() throws Exception {
        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantStorageMetadataManager metadataManager = variantStorageManager.getDBAdaptor().getMetadataManager();

        StudyMetadata studyMetadata = newStudyMetadata();
        int fileId = metadataManager.registerFile(studyMetadata.getId(), smallInputUri.getPath());
        TaskMetadata task = metadataManager.addRunningTask(studyMetadata.getId(), MongoDBVariantStorageOptions.STAGE.key(), Collections.singletonList(fileId));
        metadataManager.updateTask(studyMetadata.getId(), task.getId(), operation -> {
            operation.getStatus().clear();
            operation.addStatus(new Date(System.currentTimeMillis() - 100), TaskMetadata.Status.RUNNING);
            operation.addStatus(new Date(System.currentTimeMillis() - 50), TaskMetadata.Status.ERROR);
            operation.addStatus(new Date(System.currentTimeMillis()), TaskMetadata.Status.RUNNING);
            // Last status is RUNNING
        });

        try {
            runDefaultETL(smallInputUri, variantStorageManager, studyMetadata, new ObjectMap()
                    .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false));
            fail();
        } catch (StorageEngineException e) {
            e.printStackTrace();
//            MongoVariantStorageEngineException expected = MongoVariantStorageEngineException.fileBeingStagedException(FILE_ID, "variant-test-file.vcf.gz");
            StorageEngineException expected = StorageEngineException.currentOperationInProgressException(task, metadataManager);
            assertThat(e, instanceOf(StoragePipelineException.class));
            assertThat(e, hasCause(instanceOf(expected.getClass())));
            assertThat(e, hasCause(hasMessage(is(expected.getMessage()))));
        }

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE_RESUME.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
        );
    }

    @Test
    public void stageResumeFromError2Test() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));

        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantMongoDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        long stageCount = simulateStageError(studyMetadata, dbAdaptor);

        // Resume stage and merge
        runDefaultETL(storagePipelineResult.getTransformResult(), variantStorageManager, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true)
                .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false), false, true);

        long count = dbAdaptor.count().first();
        assertEquals(stageCount, count);
    }

    private long simulateStageError(StudyMetadata studyMetadata, VariantMongoDBAdaptor dbAdaptor) throws Exception {
        // Simulate stage error
        // 1) Set ERROR status on the StudyMetadata

        TaskMetadata[] tasks = Iterators.toArray(metadataManager.taskIterator(studyMetadata.getId()), TaskMetadata.class);
        assertEquals(1, tasks.length);

        metadataManager.updateTask(studyMetadata.getId(), tasks[0].getId(), task -> {
            assertEquals(TaskMetadata.Status.READY, task.currentStatus());
            TreeMap<Date, TaskMetadata.Status> status = task.getStatus();
            status.remove(status.lastKey(), TaskMetadata.Status.READY);
            task.addStatus(TaskMetadata.Status.ERROR);
        });

        // 2) Remove from files collection
        MongoDataStore dataStore = getMongoDataStoreManager(DB_NAME).get(DB_NAME);
        MongoDBCollection files = dataStore.getCollection(MongoDBVariantStorageOptions.COLLECTION_FILES.defaultValue());
        System.out.println("Files delete count " + files.remove(new Document(), new QueryOptions()).getNumDeleted());

        // 3) Clean some variants from the Stage collection.
        MongoDBCollection stage = dbAdaptor.getStageCollection(studyMetadata.getId());

        long stageCount = stage.count().getNumMatches();
        System.out.println("stage count : " + stageCount);
        int i = 0;
        for (Document document : stage.find(new Document(), Projections.include("_id"), null).getResults()) {
            stage.remove(document, null).getNumDeleted();
            i++;
            if (i >= stageCount / 2) {
                break;
            }
        }
        System.out.println("stage count : " + stage.count().getNumMatches());
        return stageCount;
    }

    @Test
    public void mergeAlreadyStagedFileTest() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));

        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), false)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true), false, false, true);

        Long count = variantStorageEngine.getDBAdaptor().count().first();
        assertTrue(count > 0);
    }

    @Test
    public void loadStageConcurrent() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();
        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false), true, false);

        StoragePipelineException exception = loadConcurrentAndCheck(studyMetadata, storagePipelineResult);
        exception.printStackTrace();
    }

    @Test
    public void loadStageConcurrentDifferentFiles() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();

        URI file1 = getResourceUri("1000g_batches/1-50.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file2 = getResourceUri("1000g_batches/51-100.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        URI file1Transformed = runDefaultETL(file1, variantStorageEngine, studyMetadata,
                new ObjectMap(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false), true, false).getTransformResult();
        URI file2Transformed = runDefaultETL(file2, variantStorageEngine, studyMetadata,
                new ObjectMap(), true, false).getTransformResult();

        ExecutorService executor = Executors.newFixedThreadPool(3);
        Future<Integer> loadOne = executor.submit(() -> {
            runDefaultETL(file1Transformed, getVariantStorageEngine(), studyMetadata, new ObjectMap()
                    .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                    .append(MongoDBVariantStorageOptions.MERGE.key(), false), false, true);
            return 0;
        });
        Future<Integer> loadTwo = executor.submit(() -> {
            runDefaultETL(file2Transformed, getVariantStorageEngine(), studyMetadata, new ObjectMap()
                    .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                    .append(MongoDBVariantStorageOptions.MERGE.key(), false), false, true);
            return 0;
        });

        executor.shutdown();

        assertEquals(0, loadOne.get().intValue());
        assertEquals(0, loadTwo.get().intValue());

    }

    @Test
    public void loadMergeSameConcurrent() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();
        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata,
                new ObjectMap()
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                        .append(MongoDBVariantStorageOptions.MERGE.key(), false), true, true);

        StoragePipelineException exception = loadConcurrentAndCheck(studyMetadata, storagePipelineResult);
        exception.printStackTrace();
        assertEquals(1, exception.getResults().size());
        assertTrue(exception.getResults().get(0).isLoadExecuted());
        assertNotNull(exception.getResults().get(0).getLoadError());
        int fileId = metadataManager.getFileId(studyMetadata.getId(), UriUtils.fileName(smallInputUri));
        TaskMetadata opInProgress = new TaskMetadata(studyMetadata.getId(), RandomUtils.nextInt(1000, 2000), MongoDBVariantStorageOptions.MERGE.key(), Collections.singletonList(fileId), 0, TaskMetadata.Type.LOAD);
        opInProgress.addStatus(TaskMetadata.Status.RUNNING);
        StorageEngineException expected = StorageEngineException.currentOperationInProgressException(opInProgress, metadataManager);
        assertEquals(expected.getClass(), exception.getResults().get(0).getLoadError().getClass());
        assertEquals(expected.getMessage(), exception.getResults().get(0).getLoadError().getMessage());
    }

    public StoragePipelineException loadConcurrentAndCheck(StudyMetadata studyMetadata, StoragePipelineResult storagePipelineResult) throws InterruptedException, StorageEngineException, ExecutionException {

        AtomicReference<StoragePipelineException> exception = new AtomicReference<>(null);
        Callable<Integer> load = () -> {
            try {
                runDefaultETL(storagePipelineResult.getTransformResult(), getVariantStorageEngine(), studyMetadata, new ObjectMap()
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false), false, true);
            } catch (StoragePipelineException e) {
                assertEquals(null, exception.getAndSet(e));
                return 1;
            }
            return 0;
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<Integer> loadOne = executor.submit(load);
        Future<Integer> loadTwo = executor.submit(load);
        executor.shutdown();

        executor.awaitTermination(1, TimeUnit.MINUTES);

        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        assertTrue(dbAdaptor.count(new Query()).first() > 0);
        assertEquals(1, metadataManager.getIndexedFiles(studyMetadata.getId()).size());
        TaskMetadata[] tasks = Iterators.toArray(metadataManager.taskIterator(studyMetadata.getId()), TaskMetadata.class);
        assertEquals(TaskMetadata.Status.READY, tasks[0].currentStatus());
        assertEquals(MongoDBVariantStorageOptions.STAGE.key(), tasks[0].getName());
        assertEquals(TaskMetadata.Status.READY, tasks[1].currentStatus());
        assertEquals(MongoDBVariantStorageOptions.MERGE.key(), tasks[1].getName());

        assertEquals(1, loadOne.get() + loadTwo.get());
        return exception.get();
    }

    /**
     * 1. Stage file "inputUri"
     * 2. Register a RUNNING merge task for "inputUri" in metadata
     * 3. Try to stage smallInputUri
     * 4. Assert stage is rejected because a merge is in progress
     */
    @Test
    public void stageWhileMerging() throws Exception {
        StudyMetadata studyMetadata = newStudyMetadata();
        runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false)
                .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false));

        int firstFileId = metadataManager.getFileId(studyMetadata.getId(), UriUtils.fileName(inputUri));

        // Deterministically simulate a concurrent merge by registering a RUNNING merge task for the first file.
        // The thread-based approach this replaces was racy: it relied on Thread.sleep(200) to win the
        // addRunningTask lock, but ordering depended on file-registration I/O cost and often went the other way.
        TaskMetadata opInProgress = metadataManager.addRunningTask(studyMetadata.getId(),
                MongoDBVariantStorageOptions.MERGE.key(), Collections.singletonList(firstFileId),
                false, TaskMetadata.Type.LOAD);

        int secondFileId = metadataManager.registerFile(studyMetadata.getId(), smallInputUri.getPath());
        StorageEngineException expected = MongoVariantStorageEngineException.otherOperationInProgressException(
                opInProgress, MongoDBVariantStorageOptions.STAGE.key(), Collections.singletonList(secondFileId),
                metadataManager);
        thrown.expect(StoragePipelineException.class);
        thrown.expectCause(instanceOf(expected.getClass()));
        thrown.expectCause(hasMessage(is(expected.getMessage())));

        runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false));
    }

    /**
     * Try to merge a file while a merge for a different file is already in progress.
     */
    @Test
    public void mergeWhileMerging() throws Exception {
        StudyMetadata studyMetadata = newStudyMetadata();
        runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true));

        StoragePipelineResult storagePipelineResult2 = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap()
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true));

        int firstFileId = metadataManager.getFileId(studyMetadata.getId(), UriUtils.fileName(inputUri));
        int secondFileId = metadataManager.getFileId(studyMetadata.getId(), UriUtils.fileName(smallInputUri));

        // Deterministically simulate a concurrent merge by registering a RUNNING merge task for the first file.
        // The thread-based approach this replaces was racy: it relied on Thread.sleep(200) to win the
        // addRunningTask lock, but ordering depended on file-registration I/O cost and often went the other way.
        TaskMetadata opInProgress = metadataManager.addRunningTask(studyMetadata.getId(),
                MongoDBVariantStorageOptions.MERGE.key(), Collections.singletonList(firstFileId),
                false, TaskMetadata.Type.LOAD);

        StorageEngineException expected = MongoVariantStorageEngineException.otherOperationInProgressException(
                opInProgress, MongoDBVariantStorageOptions.MERGE.key(), Collections.singletonList(secondFileId),
                metadataManager);
        thrown.expect(StoragePipelineException.class);
        thrown.expectCause(instanceOf(expected.getClass()));
        thrown.expectCause(hasMessage(is(expected.getMessage())));

        runDefaultETL(storagePipelineResult2.getTransformResult(), getVariantStorageEngine(), studyMetadata,
                new ObjectMap()
                        .append(MongoDBVariantStorageOptions.MERGE.key(), true)
                        .append(MongoDBVariantStorageOptions.STAGE.key(), false)
                        .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false), false, true);
    }

    @Test
    public void mergeResumeFirstFileTest() throws Exception {
        mergeResume(VariantStorageBaseTest.inputUri, createStudyMetadata(), o -> {});
    }

    @Test
    public void mergeResumeOtherFilesTest2() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();
        // Load in study 1
        URI f1 = getResourceUri("1000g_batches/1-50.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI f2 = getResourceUri("1000g_batches/51-100.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        // Load in study 2
        URI f3 = getResourceUri("1000g_batches/101-150.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI f4 = getResourceUri("1000g_batches/151-200.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        // Load in study 1 (with interruptions)
        URI f5 = getResourceUri("1000g_batches/201-250.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        mergeResume(f5, studyMetadata, variantStorageManager -> {
            try {
                ObjectMap objectMap = new ObjectMap()
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                        .append(VariantStorageOptions.ANNOTATE.key(), false);

                runETL(variantStorageManager, f1, outputUri, objectMap
                                .append(VariantStorageOptions.STUDY.key(), studyMetadata.getStudyName())
                        , true, true, true);
                runETL(variantStorageManager, f2, outputUri, objectMap
                                .append(VariantStorageOptions.STUDY.key(), studyMetadata.getStudyName())
                        , true, true, true);
                runETL(variantStorageManager, f3, outputUri, objectMap
                                .append(VariantStorageOptions.STUDY.key(), studyMetadata.getStudyName() + "_2")
                        , true, true, true);
                runETL(variantStorageManager, f4, outputUri, objectMap
                                .append(VariantStorageOptions.STUDY.key(), studyMetadata.getStudyName() + "_2")
                        , true, true, true);
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    public void mergeResume(URI file, StudyMetadata studyMetadata, Consumer<VariantStorageEngine> setUp) throws Exception {

        setUp.accept(variantStorageEngine);

        StoragePipelineResult storagePipelineResult = runDefaultETL(file, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));

        int sleep = 0;
        int i = 0;
        Logger logger = LoggerFactory.getLogger("Test");
        AtomicBoolean success = new AtomicBoolean(false);
        final int MAX_EXECUTIONS = 15;
        while (true) {
            final int execution = ++i;
            Thread thread = new Thread(() -> {
                try {
                    runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                            .append(VariantStorageOptions.ANNOTATE.key(), false)
                            .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                            .append(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), false)
                            .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                            .append(VariantStorageOptions.RESUME.key(), execution > 1)
                            .append(MongoDBVariantStorageOptions.MERGE.key(), true), false, false, true);
                    success.set(true);
                } catch (IOException | FileFormatException | StorageEngineException e) {
                    logger.error("Error loading in execution " + execution, e);
                }
            });
            logger.warn("+-----------------------+");
            logger.warn("+   Execution : " + execution);
            if (execution == MAX_EXECUTIONS) {
                logger.warn("+   Last Execution!");
                sleep += TimeUnit.MINUTES.toMillis(5);
            }
            logger.warn("+-----------------------+");
            thread.start();
            sleep += 1000;
            logger.warn("join sleep = " + sleep);
            thread.join(sleep);
            if (thread.isAlive()) {
                thread.interrupt();
                thread.join();
            } else {
                logger.info("Exit. Success = " + success.get());
                break;
            }
            // Finish in less than MAX_EXECUTIONS executions
            assertTrue(execution < MAX_EXECUTIONS);
        }
        // Do at least one interruption
        assertTrue(i > 1);
        assertTrue(success.get());

        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) variantStorageEngine.getDBAdaptor();
        long count = dbAdaptor.count().first();
        System.out.println("count = " + count);
        assertTrue(count > 0);

        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyMetadata.getId());
        long cleanedDocuments = MongoDBVariantStageLoader.cleanStageCollection(stageCollection, studyMetadata.getId(), Collections.singletonList(FILE_ID), null, null);
        assertEquals(0, cleanedDocuments);

        System.out.println(studyMetadata.toString());
        Integer fileId = metadataManager.getFileId(studyMetadata.getId(), file);
        Assert.assertThat(metadataManager.getIndexedFiles(studyMetadata.getId()), hasItem(fileId));
        TaskMetadata[] tasks = Iterators.toArray(metadataManager.taskIterator(studyMetadata.getId()), TaskMetadata.class);
        assertEquals(TaskMetadata.Status.READY, tasks[1].currentStatus());

        // Insert in a different set of collections the same file
        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine("2");
        setUp.accept(variantStorageManager);

        runDefaultETL(file, variantStorageManager, studyMetadata, new ObjectMap()
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.ANNOTATE.key(), false));


        // Iterate over both collections to check that contain the same variants
        MongoDataStore mongoDataStore = getMongoDataStoreManager(DB_NAME).get(DB_NAME);
        MongoDBCollection variantsCollection = mongoDataStore.getCollection(MongoDBVariantStorageOptions.COLLECTION_VARIANTS.defaultValue());
        MongoDBCollection variants2Collection = mongoDataStore.getCollection(MongoDBVariantStorageOptions.COLLECTION_VARIANTS.defaultValue() + "2");
        // Compare variant documents, normalizing engine-specific fields.
        // The two engines have different metadata states (createStudyMetadata pre-registers files),
        // so file IDs and sample IDs differ. Normalize by removing those fields.
        assertEquals(count, compareCollections(variants2Collection, variantsCollection, doc -> {
            List<Document> filesDocs = doc.getList(DocumentToVariantConverter.FILES_FIELD, Document.class);
            if (filesDocs != null) {
                for (Document fileDoc : filesDocs) {
                    fileDoc.remove("fid");        // file ID differs between engines
                    fileDoc.remove("mgt");        // genotype-to-sample map uses engine-specific sample IDs
                    fileDoc.remove("sampleData"); // sample data keyed by engine-specific sample IDs
                }
            }
            return doc;
        }));
        // Stage collection comparison removed: the second engine's getStageCollection reads from global
        // StorageConfiguration (not engine-specific options), so both engines resolve to the same
        // stage_study_<id> collection. Additionally, BASIC merge mode bypasses the stage entirely
        // for direct loads, making stage comparison unreliable.
    }

    @Test
    public void stageAlreadyStagedFileTest() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));

        long count = variantStorageEngine.getDBAdaptor().count().first();
        assertEquals(0L, count);

        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false), false, false, true);
    }

    @Test
    public void stageAlreadyMergedFileTest() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));

        Long count = variantStorageEngine.getDBAdaptor().count().first();
        assertTrue(count > 0);

        String fileName = Paths.get(smallInputUri).getFileName().toString();
        List<String> sampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
        thrown.expect(StoragePipelineException.class);
        thrown.expectCause(instanceOf(StorageEngineException.class));
        thrown.expectCause(hasMessage(containsString(StorageEngineException.alreadyLoadedSamples(fileName, sampleNames).getMessage())));
        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false), false, false, true);

    }

    @Test
    public void mergeAlreadyMergedFileTest() throws Exception {
        StudyMetadata studyMetadata = createStudyMetadata();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));

        Long count = variantStorageEngine.getDBAdaptor().count().first();
        assertTrue(count > 0);

        String fileName = Paths.get(smallInputUri).getFileName().toString();
        List<String> sampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
        StorageEngineException expectCause = StorageEngineException.alreadyLoadedSamples(fileName, sampleNames);

        thrown.expect(StoragePipelineException.class);
        thrown.expectCause(instanceOf(expectCause.getClass()));
        thrown.expectCause(hasMessage(containsString(expectCause.getMessage())));
        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true), false, false, true);

    }

    /**
     * Test merge with other staged files.
     *
     * Study 1:
     *    Staged : file2
     *    Staged+Merged: file1
     * Study 2:
     *    Staged : file3, file4
     *    Staged+Merged: file5
     */
    @Test
    public void mergeWithOtherStages() throws Exception {

        StudyMetadata studyMetadata1 = new StudyMetadata(1, "s1");
        StudyMetadata studyMetadata2 = new StudyMetadata(2, "s2");
        URI file1 = getResourceUri("1000g_batches/1-50.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file2 = getResourceUri("1000g_batches/51-100.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file3 = getResourceUri("1000g_batches/101-150.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file4 = getResourceUri("1000g_batches/151-200.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file5 = getResourceUri("1000g_batches/201-250.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        // Stage and merge file1
        runDefaultETL(file1, getVariantStorageEngine(), studyMetadata1, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));
        runDefaultETL(file2, getVariantStorageEngine(), studyMetadata1, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));

        runDefaultETL(file3, getVariantStorageEngine(), studyMetadata2, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));
        runDefaultETL(file4, getVariantStorageEngine(), studyMetadata2, new ObjectMap()
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), false));
        // Stage and merge file5
        runDefaultETL(file5, getVariantStorageEngine(), studyMetadata2, new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));

        VariantStorageMetadataManager metadataManager1 = getVariantStorageEngine().getMetadataManager();
        VariantStorageMetadataManager metadataManager2 = getVariantStorageEngine("2").getMetadataManager();


        // Copy study metadata (with sampleIndexConfigurations), samples and files from the first load
        StudyMetadata newStudyMetadata1 = copyStudyMetadata(metadataManager1, metadataManager2, studyMetadata1, true);
        StudyMetadata newStudyMetadata2 = copyStudyMetadata(metadataManager1, metadataManager2, studyMetadata2, true);

        runDefaultETL(file1, getVariantStorageEngine("2"), newStudyMetadata1, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 1)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SAMPLE_INDEX.key(), YesNoAuto.NO)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));
        runDefaultETL(file5, getVariantStorageEngine("2"), newStudyMetadata2, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SAMPLE_INDEX.key(), YesNoAuto.NO)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));

        compareCollections(getVariantStorageEngine("2").getDBAdaptor().getVariantsCollection(),
                getVariantStorageEngine().getDBAdaptor().getVariantsCollection());
    }

    @Test
    public void concurrentMerge() throws Exception {
        StudyMetadata studyMetadata1 = new StudyMetadata(1, "s1");
        StudyMetadata studyMetadata2 = new StudyMetadata(2, "s2");


        URI file1 = getResourceUri("1000g_batches/1-50.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file2 = getResourceUri("1000g_batches/51-100.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file3 = getResourceUri("1000g_batches/101-150.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file4 = getResourceUri("1000g_batches/151-200.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file5 = getResourceUri("1000g_batches/201-250.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        MongoDBVariantStorageEngine variantStorageManager1 = getVariantStorageEngine();
        runDefaultETL(file1, variantStorageManager1, studyMetadata1, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 1)
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                        .append(MongoDBVariantStorageOptions.MERGE.key(), false)
                , true, true);
        runDefaultETL(file2, variantStorageManager1, studyMetadata1, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                        .append(MongoDBVariantStorageOptions.MERGE.key(), false)
                , true, true);
        StoragePipelineResult storagePipelineResult3 = runDefaultETL(file3, variantStorageManager1, studyMetadata1, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                        .append(MongoDBVariantStorageOptions.MERGE.key(), false)
                , true, true);

        runDefaultETL(file4, variantStorageManager1, studyMetadata2, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 4)
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                        .append(MongoDBVariantStorageOptions.MERGE.key(), false)
                , true, true);
        StoragePipelineResult storagePipelineResult5 = runDefaultETL(file5, variantStorageManager1, studyMetadata2, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                        .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                        .append(MongoDBVariantStorageOptions.MERGE.key(), false)
                , true, true);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future mergeFile3 = executor.submit((Callable) () -> {
            runDefaultETL(storagePipelineResult3.getTransformResult(), newVariantStorageEngine(), studyMetadata1, new ObjectMap()
//                    .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                    .append(VariantStorageOptions.ANNOTATE.key(), false)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                    .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                    .append(MongoDBVariantStorageOptions.MERGE.key(), true), false, true);
            return 0;
        });
        Future mergeFile5 = executor.submit((Callable) () -> {
            runDefaultETL(storagePipelineResult5.getTransformResult(), newVariantStorageEngine(), studyMetadata2, new ObjectMap()
//                    .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                    .append(VariantStorageOptions.ANNOTATE.key(), false)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                    .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                    .append(MongoDBVariantStorageOptions.MERGE.key(), true), false, true);
            return 0;
        });
        executor.shutdown();
        executor.awaitTermination(4, TimeUnit.MINUTES);
        assertEquals(0, mergeFile3.get());
        assertEquals(0, mergeFile5.get());

        VariantStorageMetadataManager metadataManager1 = getVariantStorageEngine().getMetadataManager();
        VariantStorageMetadataManager metadataManager2 = getVariantStorageEngine("2").getMetadataManager();

        // Copy study metadata (with sampleIndexConfigurations) and samples from the first load
        StudyMetadata newStudyMetadata1 = copyStudyMetadata(metadataManager1, metadataManager2, studyMetadata1, false);
        StudyMetadata newStudyMetadata2 = copyStudyMetadata(metadataManager1, metadataManager2, studyMetadata2, false);

        runDefaultETL(file3, getVariantStorageEngine("2"), newStudyMetadata1, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SAMPLE_INDEX.key(), YesNoAuto.NO)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));
        runDefaultETL(file5, getVariantStorageEngine("2"), newStudyMetadata2, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SAMPLE_INDEX.key(), YesNoAuto.NO)
                .append(MongoDBVariantStorageOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageOptions.MERGE.key(), true));

        compareCollections(getVariantStorageEngine("2").getDBAdaptor().getVariantsCollection(),
                getVariantStorageEngine().getDBAdaptor().getVariantsCollection(), document -> {
                    // Sort studies, because they can be inserted in a different order
                    if (document.containsKey(DocumentToVariantConverter.STUDIES_FIELD)) {
                        List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
                        studies.sort(Comparator.comparing(o -> o.getInteger(STUDYID_FIELD)));
                    }
                    // Sort and normalize root files (order and file IDs may differ between engines)
                    if (document.containsKey(DocumentToVariantConverter.FILES_FIELD)) {
                        List<Document> files = document.get(DocumentToVariantConverter.FILES_FIELD, List.class);
                        if (files != null) {
                            files.sort(Comparator.comparing(
                                    o -> ((Number) ((Document) o).get(STUDYID_FIELD)).intValue()));
                            files.forEach(file -> file.remove(DocumentToStudyEntryConverter.FILEID_FIELD));
                        }
                    }
                    return document;
                });
    }

    @Test
    public void checkCanLoadSampleBatchTest() throws StorageEngineException {
        StudyMetadata studyMetadata = createStudyMetadata();
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 1, false);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(1));
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 2, true);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(2));
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 3, false);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(3));
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 4, true);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(4));
    }

    @Test
    public void checkCanLoadSampleBatch2Test() throws StorageEngineException {
        StudyMetadata studyMetadata = createStudyMetadata();
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 4, false);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(4));
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 3, true);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(3));
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 2, false);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(2));
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(metadataManager, studyMetadata, 1, true);
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(1));
    }

    @Test
    public void checkCanLoadSampleBatchFailTest() throws StorageEngineException {
        StudyMetadata studyMetadata = createStudyMetadata();
        metadataManager.addIndexedFiles(studyMetadata.getId(), Arrays.asList(1, 3, 4));
        StorageEngineException e = MongoVariantStorageEngineException.alreadyLoadedSamples("file2.vcf", Arrays.asList("s1", "s2", "s3", "s4"));
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(variantStorageEngine.getMetadataManager(), studyMetadata, 2, false);
    }

    @Test
    public void checkCanLoadSampleBatchFail2Test() throws StorageEngineException {
        StudyMetadata studyMetadata = createStudyMetadata();
        metadataManager.addIndexedFiles(studyMetadata.getId(), Arrays.asList(1, 2));
        StorageEngineException e = MongoVariantStorageEngineException.alreadyLoadedSomeSamples("file5.vcf");
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(variantStorageEngine.getMetadataManager(), studyMetadata, 5, false);
    }

    /**
     * Copy study metadata from one metadata manager to another, resetting index status on samples and files.
     * Copies sampleIndexConfigurations from the source study so that the target study is properly initialized.
     */
    private StudyMetadata copyStudyMetadata(VariantStorageMetadataManager source, VariantStorageMetadataManager target,
                                            StudyMetadata studyMetadata, boolean copyFiles) {
        StudyMetadata sourceStudy = source.getStudyMetadata(studyMetadata.getId());
        StudyMetadata newStudy = new StudyMetadata(sourceStudy.getId(), sourceStudy.getName());
        newStudy.setSampleIndexConfigurations(sourceStudy.getSampleIndexConfigurations());
        target.unsecureUpdateStudyMetadata(newStudy);
        source.sampleMetadataIterator(studyMetadata.getId()).forEachRemaining(s -> {
            s.setIndexStatus(TaskMetadata.Status.NONE);
            target.unsecureUpdateSampleMetadata(newStudy.getId(), s);
        });
        if (copyFiles) {
            source.fileMetadataIterator(studyMetadata.getId()).forEachRemaining(f -> {
                f.setIndexStatus(TaskMetadata.Status.NONE);
                // Reset stage/merge statuses so the target engine re-stages the file
                // instead of skipping it as "already staged" (which would leave
                // VariantFileMetadata missing and no data in the stage collection).
                f.getStatus().clear();
                target.unsecureUpdateFileMetadata(newStudy.getId(), f);
            });
        }
        return newStudy;
    }

    public StudyMetadata createStudyMetadata() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        List<String> batch1 = Arrays.asList("s1", "s2", "s3", "s4");
        List<String> batch2 = Arrays.asList("s5", "s6", "s7", "s8");
        List<String> batch3 = Arrays.asList("s1", "s3", "s5", "s7"); //Mixed batch

        metadataManager.registerFile(study.getId(), "file1.vcf", batch1);
        metadataManager.registerFile(study.getId(), "file2.vcf", batch1);
        metadataManager.registerFile(study.getId(), "file3.vcf", batch2);
        metadataManager.registerFile(study.getId(), "file4.vcf", batch2);
        metadataManager.registerFile(study.getId(), "file5.vcf", batch3);

        return study;
    }

    @Test
    @Override
    public void multiIndexPlatinum() throws Exception {
        super.multiIndexPlatinum(new ObjectMap(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "DP,AD,PL"));
        checkPlatinumDatabase(Collections.singleton("0/0"));
    }

//    @Test
//    public void multiIndexPlatinumNoUnknownGenotypes() throws Exception {
//        super.multiIndexPlatinum(new ObjectMap(MongoDBVariantOptions.DEFAULT_GENOTYPE.key(), GenotypeClass.UNKNOWN_GENOTYPE));
//        checkPlatinumDatabase(Collections.singleton(GenotypeClass.UNKNOWN_GENOTYPE));
//    }

    @Test
    public void multiIndexPlatinumMergeSimple() throws Exception {
        super.multiIndexPlatinum(new ObjectMap(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC));
        checkPlatinumDatabase(Collections.singleton(GenotypeClass.UNKNOWN_GENOTYPE));
    }

    private void checkPlatinumDatabase(Set<String> defaultGenotypes) throws Exception {
        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor()) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            StudyMetadata sc1 = dbAdaptor.getMetadataManager().getStudyMetadata(1);
            StudyMetadata sc2 = dbAdaptor.getMetadataManager().getStudyMetadata(2);
            MongoDBIterator<Document> it = variantsCollection.nativeQuery().find(new Document(), new QueryOptions());
            while (it.hasNext()) {
                Document document = it.next();
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
                List<Document> allFiles = document.get(DocumentToVariantConverter.FILES_FIELD, List.class);
                assertEquals(id, 2, studies.size());

                // Group root-level files by study
                List<Document> studyFiles1 = allFiles.stream()
                        .filter(f -> f.getInteger(STUDYID_FIELD).equals(sc1.getId()))
                        .collect(Collectors.toList());
                List<Document> studyFiles2 = allFiles.stream()
                        .filter(f -> f.getInteger(STUDYID_FIELD).equals(sc2.getId()))
                        .collect(Collectors.toList());

                // Per-study genotype check: verify no default GTs stored, no duplicate samples
                for (List<Document> studyFiles : Arrays.asList(studyFiles1, studyFiles2)) {
                    Set<Integer> allSamples = new HashSet<>();
                    for (Document file : studyFiles) {
                        Document gts = file.get(FILE_GENOTYPE_FIELD, Document.class);
                        for (String defaultGenotype : defaultGenotypes) {
                            assertThat(gts.keySet(), not(hasItem(defaultGenotype)));
                        }
                        for (Map.Entry<String, Object> entry : gts.entrySet()) {
                            List<Integer> sampleIds = (List<Integer>) entry.getValue();
                            for (Integer sampleId : sampleIds) {
                                assertFalse(id, allSamples.contains(sampleId));
                                assertTrue(id, allSamples.add(sampleId));
                            }
                        }
                    }
                    // Each platinum file has 1 sample; total non-default-GT samples = number of files
                    assertEquals(id, studyFiles.size(), allSamples.size());
                }

                // Cross-study genotype comparison: merge genotypes from files per study
                Map<String, Set<Integer>> gts1 = mergeFileGenotypes(studyFiles1);
                Map<String, Set<Integer>> gts2 = mergeFileGenotypes(studyFiles2);
                assertEquals(id, gts1.keySet(), gts2.keySet());
                for (String gt : gts1.keySet()) {
                    // Normalize sample IDs (study2 samples are offset by 17)
                    Set<Integer> expected = gts1.get(gt).stream()
                            .map(i -> i > 17 ? i - 17 : i).collect(Collectors.toSet());
                    Set<Integer> actual = gts2.get(gt).stream()
                            .map(i -> i > 17 ? i - 17 : i).collect(Collectors.toSet());
                    assertEquals(id + ":" + gt, expected, actual);
                }

                // Cross-study file comparison (order is not important)
                Map<String, Document> fileMap1 = studyFiles1.stream()
                        .collect(Collectors.toMap(
                                d -> metadataManager.getFileName(sc1.getId(), d.getInteger(FILEID_FIELD)),
                                Function.identity()));
                Map<String, Document> fileMap2 = studyFiles2.stream()
                        .collect(Collectors.toMap(
                                d -> metadataManager.getFileName(sc2.getId(), d.getInteger(FILEID_FIELD)),
                                Function.identity()));
                assertEquals(id, fileMap1.size(), fileMap2.size());
                for (Map.Entry<String, Document> entry : fileMap1.entrySet()) {
                    Document file1 = entry.getValue();
                    Document file2 = fileMap2.get(entry.getKey());
                    // Compare alternates per file
                    assertEquals(id, file1.get(ALTERNATES_FIELD), file2.get(ALTERNATES_FIELD));
                    Document attrs = file1.get(ATTRIBUTES_FIELD, Document.class);
                    Document attrs2 = file2.get(ATTRIBUTES_FIELD, Document.class);
                    String ac1 = Objects.toString(attrs.remove("AC"));
                    String ac2 = Objects.toString(attrs2.remove("AC"));
                    if (!ac1.equals(ac2)) {
                        ac1 = Arrays.stream(ac1.split(",")).map(Integer::parseInt).map(String::valueOf)
                                .collect(Collectors.joining(","));
                        ac2 = Arrays.stream(ac2.split(",")).map(Integer::parseInt).map(String::valueOf)
                                .collect(Collectors.joining(","));
                        assertTrue(id + ' ' + ac1 + ' ' + ac2, ac1.startsWith(ac2) || ac2.startsWith(ac1));
                    }
                    String af1 = Objects.toString(attrs.remove("AF"));
                    String af2 = Objects.toString(attrs2.remove("AF"));
                    if (!af1.equals(af2)) {
                        af1 = Arrays.stream(af1.split(",")).map(Double::parseDouble).map(String::valueOf)
                                .collect(Collectors.joining(","));
                        af2 = Arrays.stream(af2.split(",")).map(Double::parseDouble).map(String::valueOf)
                                .collect(Collectors.joining(","));
                        assertTrue(id + ' ' + af1 + ' ' + af2, af1.startsWith(af2) || af2.startsWith(af1));
                    }
                    Document samplesData1 = (Document) file1.remove(SAMPLE_DATA_FIELD);
                    Document samplesData2 = (Document) file2.remove(SAMPLE_DATA_FIELD);
                    for (String key : samplesData1.keySet()) {
                        VariantMongoDBProto.OtherFields data1 = readSamplesData(samplesData1, key);
                        VariantMongoDBProto.OtherFields data2 = readSamplesData(samplesData2, key);
                        if (data1 == null) {
                            assertNull(data2);
                        } else {
                            assertEquals(data1.getStringValuesCount(), data2.getStringValuesCount());
                            if (1 == data2.getStringValuesCount()) {
                                String value1 = data1.getStringValues(0);
                                String value2 = data2.getStringValues(0);
                                assertTrue(id + ' ' + value1 + ' ' + value2,
                                        value1.startsWith(value2) || value2.startsWith(value1));
                            } else {
                                assertEquals(data1, data2);
                            }
                        }
                    }
                    int fileId1 = (int) file1.remove(FILEID_FIELD);
                    int fileId2 = (int) file2.remove(FILEID_FIELD);
                    assertEquals(Integer.signum(fileId1), Integer.signum(fileId2));
                    // Remove fields that differ between studies before comparing remaining
                    file1.remove(STUDYID_FIELD);
                    file2.remove(STUDYID_FIELD);
                    file1.remove(FILE_GENOTYPE_FIELD);
                    file2.remove(FILE_GENOTYPE_FIELD);
                    // sfd keys are sample IDs (study2 offset by 17); normalize before comparing
                    Document sfd1 = (Document) file1.remove(SAMPLE_FILTERABLE_DATA_FIELD);
                    Document sfd2 = (Document) file2.remove(SAMPLE_FILTERABLE_DATA_FIELD);
                    assertEquals(id, normalizeSfdSampleIds(sfd1), normalizeSfdSampleIds(sfd2));
                    assertEquals(id, file1, file2);
                }
            }
        }
    }

    private static Document normalizeSfdSampleIds(Document sfd) {
        if (sfd == null) {
            return null;
        }
        Document normalized = new Document();
        for (Map.Entry<String, Object> fieldEntry : sfd.entrySet()) {
            Document sampleValues = (Document) fieldEntry.getValue();
            Document normalizedValues = new Document();
            for (Map.Entry<String, Object> sampleEntry : sampleValues.entrySet()) {
                int sampleId = Integer.parseInt(sampleEntry.getKey());
                String normalizedKey = String.valueOf(sampleId > 17 ? sampleId - 17 : sampleId);
                normalizedValues.put(normalizedKey, sampleEntry.getValue());
            }
            normalized.put(fieldEntry.getKey(), normalizedValues);
        }
        return normalized;
    }

    private Map<String, Set<Integer>> mergeFileGenotypes(List<Document> files) {
        Map<String, Set<Integer>> merged = new LinkedHashMap<>();
        for (Document file : files) {
            Document gts = file.get(FILE_GENOTYPE_FIELD, Document.class);
            for (Map.Entry<String, Object> entry : gts.entrySet()) {
                merged.computeIfAbsent(entry.getKey(), k -> new HashSet<>())
                        .addAll((List<Integer>) entry.getValue());
            }
        }
        return merged;
    }

    private VariantMongoDBProto.OtherFields readSamplesData(Document samplesData1, String key) throws com.google.protobuf.InvalidProtocolBufferException {

        byte[] data = samplesData1.get(key, Binary.class).getData();
        try {
            data = CompressionUtils.decompress(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataFormatException ignore) {
            //It was not actually compressed, so it failed decompressing
        }
        if (data != null && data.length > 0) {
            return VariantMongoDBProto.OtherFields.parseFrom(data);
        } else {
            return null;
        }
    }


    @Test
    @Override
    public void multiRegionBatchIndex() throws Exception {
        super.multiRegionBatchIndex();
        checkLoadedVariants();

//        // Check that the first file has been loaded with DIRECT_LOAD method
//        StudyMetadata studyMetadata = variantStorageEngine.getStudyMetadataManager()
//                .getStudyMetadata(1, null).first();
//
//        for (BatchFileOperation batchFileOperation : studyMetadata.getBatches()) {
//            assertEquals(MongoDBVariantOptions.DIRECT_LOAD.key(), batchFileOperation.getOperationName());
//        }
    }

    @Test
    @Override
    public void multiRegionIndex() throws Exception {
        super.multiRegionIndex();

        checkLoadedVariants();

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata("multiRegion");

        metadataManager.taskIterator(studyMetadata.getId()).forEachRemaining(task -> {
            assertEquals(MongoDBVariantStorageOptions.DIRECT_LOAD.key(), task.getName());
        });
    }

    public void checkLoadedVariants() throws Exception {
        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor()) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            MongoDBIterator<Document> it = variantsCollection.nativeQuery().find(new Document(), new QueryOptions());
            while (it.hasNext()) {
                Document document = it.next();
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);

//                assertEquals(id, 2, studies.size());
                for (Document study : studies) {
                    List<Document> files = ((List<Document>) document.get(DocumentToVariantConverter.FILES_FIELD, List.class))
                            .stream()
                            .filter(f -> Objects.equals(f.getInteger(STUDYID_FIELD), study.getInteger(STUDYID_FIELD)))
                            .collect(Collectors.toList());

                    Map<Integer, String> samples = new HashMap<>();
                    for (Document file : files) {
                        Document gts = file.get(FILE_GENOTYPE_FIELD, Document.class);
                        if (gts == null) {
                            // AC=0 variants: no sample carries an alt allele, so mgt is absent
                            continue;
                        }

                        for (Map.Entry<String, Object> entry : gts.entrySet()) {
                            List<Integer> sampleIds = (List<Integer>) entry.getValue();
                            for (Integer sampleId : sampleIds) {
                                String message = "var: " + id + " Duplicated sampleId " + sampleId + " in gt " + entry.getKey() + " and " + samples.get(sampleId) + " : " + sampleIds;
                                assertFalse(message, samples.containsKey(sampleId));
                                assertTrue(message, samples.put(sampleId, entry.getKey()) == null);
                            }
                        }
                    }
                }
            }
        }
    }

}
