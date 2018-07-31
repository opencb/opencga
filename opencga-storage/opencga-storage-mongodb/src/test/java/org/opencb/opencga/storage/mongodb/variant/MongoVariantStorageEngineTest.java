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

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineTest;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
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
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;


/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
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
        StudyConfigurationManager scm = variantStorageManager.getDBAdaptor().getStudyConfigurationManager();

        StudyConfiguration studyConfiguration = newStudyConfiguration();
        int fileId = scm.registerFile(studyConfiguration, UriUtils.fileName(smallInputUri));
        BatchFileOperation operation = new BatchFileOperation(MongoDBVariantOptions.STAGE.key(),
                Collections.singletonList(fileId), System.currentTimeMillis(), BatchFileOperation.Type.OTHER);
        operation.addStatus(new Date(System.currentTimeMillis() - 100), BatchFileOperation.Status.RUNNING);
        operation.addStatus(new Date(System.currentTimeMillis() - 50), BatchFileOperation.Status.ERROR);
        // Last status is ERROR

        studyConfiguration.getBatches().add(operation);
        scm.updateStudyConfiguration(studyConfiguration, null);

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false)
                .append(MongoDBVariantOptions.STAGE_RESUME.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );

    }

    @Test
    public void stageForceResumeTest() throws Exception {
        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        StudyConfigurationManager scm = variantStorageManager.getDBAdaptor().getStudyConfigurationManager();

        StudyConfiguration studyConfiguration = newStudyConfiguration();
        int fileId = scm.registerFile(studyConfiguration, UriUtils.fileName(smallInputUri));
        BatchFileOperation operation = new BatchFileOperation(MongoDBVariantOptions.STAGE.key(),
                Collections.singletonList(fileId), System.currentTimeMillis(), BatchFileOperation.Type.OTHER);
        operation.addStatus(new Date(System.currentTimeMillis() - 100), BatchFileOperation.Status.RUNNING);
        operation.addStatus(new Date(System.currentTimeMillis() - 50), BatchFileOperation.Status.ERROR);
        operation.addStatus(new Date(System.currentTimeMillis()), BatchFileOperation.Status.RUNNING);
        // Last status is RUNNING
        studyConfiguration.getBatches().add(operation);
        scm.updateStudyConfiguration(studyConfiguration, null);

        try {
            runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                    .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false));
            fail();
        } catch (StorageEngineException e) {
            e.printStackTrace();
//            MongoVariantStorageEngineException expected = MongoVariantStorageEngineException.fileBeingStagedException(FILE_ID, "variant-test-file.vcf.gz");
            StorageEngineException expected = StorageEngineException.currentOperationInProgressException(operation);
            assertThat(e, instanceOf(StoragePipelineException.class));
            assertThat(e, hasCause(instanceOf(expected.getClass())));
            assertThat(e, hasCause(hasMessage(is(expected.getMessage()))));
        }

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE_RESUME.key(), true)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
    }

    @Test
    public void stageResumeFromError2Test() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantMongoDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        long stageCount = simulateStageError(studyConfiguration, dbAdaptor);

        // Resume stage and merge
        runDefaultETL(storagePipelineResult.getTransformResult(), variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true)
                .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false), false, true);

        long count = dbAdaptor.count(null).first();
        assertEquals(stageCount, count);
    }

    private long simulateStageError(StudyConfiguration studyConfiguration, VariantMongoDBAdaptor dbAdaptor) throws Exception {
        // Simulate stage error
        // 1) Set ERROR status on the StudyConfiguration
        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        studyConfiguration.copy(scm.getStudyConfiguration(studyConfiguration.getStudyId(), new QueryOptions()).first());
        assertEquals(1, studyConfiguration.getBatches().size());
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(0).currentStatus());
        TreeMap<Date, BatchFileOperation.Status> status = studyConfiguration.getBatches().get(0).getStatus();
        status.remove(status.lastKey(), BatchFileOperation.Status.READY);
        studyConfiguration.getBatches().get(0).addStatus(BatchFileOperation.Status.ERROR);
        scm.updateStudyConfiguration(studyConfiguration, null);

        // 2) Remove from files collection
        MongoDataStore dataStore = getMongoDataStoreManager(DB_NAME).get(DB_NAME);
        MongoDBCollection files = dataStore.getCollection(MongoDBVariantOptions.COLLECTION_FILES.defaultValue());
        System.out.println("Files delete count " + files.remove(new Document(), new QueryOptions()).first().getDeletedCount());

        // 3) Clean some variants from the Stage collection.
        MongoDBCollection stage = dbAdaptor.getStageCollection(studyConfiguration.getStudyId());

        long stageCount = stage.count().first();
        System.out.println("stage count : " + stageCount);
        int i = 0;
        for (Document document : stage.find(new Document(), Projections.include("_id"), null).getResult()) {
            stage.remove(document, null).first().getDeletedCount();
            i++;
            if (i >= stageCount / 2) {
                break;
            }
        }
        System.out.println("stage count : " + stage.count().first());
        return stageCount;
    }

    @Test
    public void mergeAlreadyStagedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), false)
                .append(MongoDBVariantOptions.MERGE.key(), true), false, false, true);

        Long count = variantStorageEngine.getDBAdaptor().count(null).first();
        assertTrue(count > 0);
    }

    @Test
    public void loadStageConcurrent() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false), true, false);

        StoragePipelineException exception = loadConcurrentAndCheck(studyConfiguration, storagePipelineResult);
        exception.printStackTrace();
    }

    @Test
    public void loadStageConcurrentDifferentFiles() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        URI file1 = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file2 = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        URI file1Transformed = runDefaultETL(file1, variantStorageEngine, studyConfiguration,
                new ObjectMap(MongoDBVariantOptions.DIRECT_LOAD.key(), false), true, false).getTransformResult();
        URI file2Transformed = runDefaultETL(file2, variantStorageEngine, studyConfiguration,
                new ObjectMap(), true, false).getTransformResult();

        ExecutorService executor = Executors.newFixedThreadPool(3);
        Future<Integer> loadOne = executor.submit(() -> {
            runDefaultETL(file1Transformed, getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                    .append(MongoDBVariantOptions.STAGE.key(), true)
                    .append(MongoDBVariantOptions.MERGE.key(), false), false, true);
            return 0;
        });
        Future<Integer> loadTwo = executor.submit(() -> {
            runDefaultETL(file2Transformed, getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                    .append(MongoDBVariantOptions.STAGE.key(), true)
                    .append(MongoDBVariantOptions.MERGE.key(), false), false, true);
            return 0;
        });

        executor.shutdown();

        assertEquals(0, loadOne.get().intValue());
        assertEquals(0, loadTwo.get().intValue());

    }

    @Test
    public void loadMergeSameConcurrent() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration,
                new ObjectMap()
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false), true, true);

        StoragePipelineException exception = loadConcurrentAndCheck(studyConfiguration, storagePipelineResult);
        exception.printStackTrace();
        assertEquals(1, exception.getResults().size());
        assertTrue(exception.getResults().get(0).isLoadExecuted());
        assertNotNull(exception.getResults().get(0).getLoadError());
        BatchFileOperation opInProgress = new BatchFileOperation(MongoDBVariantOptions.MERGE.key(), Collections.singletonList(FILE_ID), 0, BatchFileOperation.Type.LOAD);
        opInProgress.addStatus(BatchFileOperation.Status.RUNNING);
        StorageEngineException expected = StorageEngineException.currentOperationInProgressException(opInProgress);
        assertEquals(expected.getClass(), exception.getResults().get(0).getLoadError().getClass());
        assertEquals(expected.getMessage(), exception.getResults().get(0).getLoadError().getMessage());
    }

    public StoragePipelineException loadConcurrentAndCheck(StudyConfiguration studyConfiguration, StoragePipelineResult storagePipelineResult) throws InterruptedException, StorageEngineException, ExecutionException {

        AtomicReference<StoragePipelineException> exception = new AtomicReference<>(null);
        Callable<Integer> load = () -> {
            try {
                runDefaultETL(storagePipelineResult.getTransformResult(), getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                        .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false), false, true);
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
        assertEquals(1, studyConfiguration.getIndexedFiles().size());
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(0).currentStatus());
        assertEquals(MongoDBVariantOptions.STAGE.key(), studyConfiguration.getBatches().get(0).getOperationName());
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(1).currentStatus());
        assertEquals(MongoDBVariantOptions.MERGE.key(), studyConfiguration.getBatches().get(1).getOperationName());

        assertEquals(1, loadOne.get() + loadTwo.get());
        return exception.get();
    }

    /**
     * 1. Stage file "inputUri"
     * 2. wait
     * 3. Merge file "inputUri" (in a different thread)
     * 4. Try to stage smallInputUri (concurrently)
     * 5. Assert fail stage
     */
    @Test
    public void stageWhileMerging() throws Exception {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult storagePipelineResult = runDefaultETL(inputUri, getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false)
                .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false));
        Thread thread = new Thread(() -> {
            try {
                runDefaultETL(storagePipelineResult.getTransformResult(), getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                                .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false),
                        false, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        StudyConfigurationManager studyConfigurationManager = getVariantStorageEngine().getDBAdaptor().getStudyConfigurationManager();
        int secondFileId = 2;
        try {
            thread.start();
            Thread.sleep(200);

            BatchFileOperation opInProgress = new BatchFileOperation(MongoDBVariantOptions.MERGE.key(), Collections.singletonList(FILE_ID), 0, BatchFileOperation.Type.OTHER);
            opInProgress.addStatus(BatchFileOperation.Status.RUNNING);
            StorageEngineException expected = MongoVariantStorageEngineException.otherOperationInProgressException(opInProgress, MongoDBVariantOptions.STAGE.key(), Collections.singletonList(secondFileId));
            thrown.expect(StoragePipelineException.class);
            thrown.expectCause(instanceOf(expected.getClass()));
            thrown.expectCause(hasMessage(is(expected.getMessage())));

            runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration,
                    new ObjectMap(MongoDBVariantOptions.DIRECT_LOAD.key(), false));
        } finally {
            System.out.println("Interrupt!");
            thread.interrupt();
            System.out.println("Join!");
            thread.join();
            System.out.println("EXIT");

            StudyConfiguration sc = studyConfigurationManager.getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
            // Second file is not staged or merged
//            int secondFileId = studyConfiguration.getFileIds().get(UriUtils.fileName(smallInputUri));
            List<BatchFileOperation> ops = sc.getBatches().stream().filter(op -> op.getFileIds().contains(secondFileId)).collect(Collectors.toList());
            assertEquals(0, ops.size());
        }
    }

    /**
     * Try to merge two different files in the same study at the same time.
     */
    @Test
    public void mergeWhileMerging() throws Exception {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult storagePipelineResult = runDefaultETL(inputUri, getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true));

        StoragePipelineResult storagePipelineResult2 = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration,
                new ObjectMap()
                        .append(MongoDBVariantOptions.STAGE.key(), true));
        int secondFileId = studyConfiguration.getFileIds().get(UriUtils.fileName(smallInputUri));
        Thread thread = new Thread(() -> {
            try {
                runDefaultETL(storagePipelineResult.getTransformResult(), getVariantStorageEngine(), studyConfiguration, new ObjectMap()
                            .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false), false, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        StudyConfigurationManager studyConfigurationManager = getVariantStorageEngine().getDBAdaptor().getStudyConfigurationManager();
        try {
            thread.start();
            Thread.sleep(200);

            BatchFileOperation opInProgress = new BatchFileOperation(MongoDBVariantOptions.MERGE.key(), Collections.singletonList(FILE_ID), 0, BatchFileOperation.Type.OTHER);
            opInProgress.addStatus(BatchFileOperation.Status.RUNNING);
            StorageEngineException expected = MongoVariantStorageEngineException.otherOperationInProgressException(opInProgress, MongoDBVariantOptions.MERGE.key(), Collections.singletonList(secondFileId));
            thrown.expect(StoragePipelineException.class);
            thrown.expectCause(instanceOf(expected.getClass()));
            thrown.expectCause(hasMessage(is(expected.getMessage())));

            runDefaultETL(storagePipelineResult2.getTransformResult(), getVariantStorageEngine(), studyConfiguration,
                    new ObjectMap()
                            .append(MongoDBVariantOptions.MERGE.key(), true)
                            .append(MongoDBVariantOptions.STAGE.key(), false)
                            .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false), false, true);
        } finally {
            System.out.println("Interrupt!");
            thread.interrupt();
            System.out.println("Join!");
            thread.join();
            System.out.println("EXIT");

            StudyConfiguration sc = studyConfigurationManager.getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
            // Second file is not staged or merged
            List<BatchFileOperation> ops = sc.getBatches().stream().filter(op -> op.getFileIds().contains(secondFileId)).collect(Collectors.toList());
            assertEquals(1, ops.size());
            assertEquals(MongoDBVariantOptions.STAGE.key(), ops.get(0).getOperationName());
            System.out.println("DONE");
        }
    }

    @Test
    public void mergeResumeFirstFileTest() throws Exception {
        mergeResume(VariantStorageBaseTest.inputUri, createStudyConfiguration(), o -> {});
    }

    @Test
    public void mergeResumeOtherFilesTest2() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        // Load in study 1
        URI f1 = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI f2 = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        // Load in study 2
        URI f3 = getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI f4 = getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        // Load in study 1 (with interruptions)
        URI f5 = getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        mergeResume(f5, studyConfiguration, variantStorageManager -> {
            try {
                ObjectMap objectMap = new ObjectMap()
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false);

                runETL(variantStorageManager, f1, outputUri, objectMap
                                .append(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName())
                        , true, true, true);
                runETL(variantStorageManager, f2, outputUri, objectMap
                                .append(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName())
                        , true, true, true);
                runETL(variantStorageManager, f3, outputUri, objectMap
                                .append(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName() + "_2")
                        , true, true, true);
                runETL(variantStorageManager, f4, outputUri, objectMap
                                .append(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName() + "_2")
                        , true, true, true);
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    public void mergeResume(URI file, StudyConfiguration studyConfiguration, Consumer<VariantStorageEngine> setUp) throws Exception {

        setUp.accept(variantStorageEngine);

        StoragePipelineResult storagePipelineResult = runDefaultETL(file, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

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
                            .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                            .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                            .append(MongoDBVariantOptions.DIRECT_LOAD.key(), false)
                            .append(MongoDBVariantOptions.STAGE.key(), true)
                            .append(VariantStorageEngine.Options.RESUME.key(), execution > 1)
                            .append(MongoDBVariantOptions.MERGE.key(), true), false, false, true);
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
        long count = dbAdaptor.count(null).first();
        System.out.println("count = " + count);
        assertTrue(count > 0);

        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyConfiguration.getStudyId());
        long cleanedDocuments = MongoDBVariantStageLoader.cleanStageCollection(stageCollection, studyConfiguration.getStudyId(), Collections.singletonList(FILE_ID), null, null);
        assertEquals(0, cleanedDocuments);

        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        System.out.println(studyConfiguration.toString());
        Integer fileId = studyConfiguration.getFileIds().get(Paths.get(file.getPath()).getFileName().toString());
        assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(1).currentStatus());

        // Insert in a different set of collections the same file
        MongoDBVariantStorageEngine variantStorageManager = getVariantStorageEngine("2");
        setUp.accept(variantStorageManager);

        runDefaultETL(file, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false));


        // Iterate over both collections to check that contain the same variants
        MongoDataStore mongoDataStore = getMongoDataStoreManager(DB_NAME).get(DB_NAME);
        MongoDBCollection variantsCollection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_VARIANTS.defaultValue());
        MongoDBCollection variants2Collection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_VARIANTS.defaultValue() + "2");
//        MongoDBCollection stageCollection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_STAGE.defaultValue());
        MongoDBCollection stage2Collection = variantStorageManager.getDBAdaptor().getStageCollection(studyConfiguration.getStudyId());

        assertEquals(count, compareCollections(variants2Collection, variantsCollection));
        compareCollections(stage2Collection, stageCollection);
    }

    public MongoDBVariantStorageEngine getVariantStorageEngine(String collectionSufix) throws Exception {
        MongoDBVariantStorageEngine variantStorageEngine = newVariantStorageEngine();
        ObjectMap renameCollections = new ObjectMap()
                .append(MongoDBVariantOptions.COLLECTION_PROJECT.key(), MongoDBVariantOptions.COLLECTION_PROJECT.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_STUDIES.key(), MongoDBVariantOptions.COLLECTION_STUDIES.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_FILES.key(), MongoDBVariantOptions.COLLECTION_FILES.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_STAGE.key(), MongoDBVariantOptions.COLLECTION_STAGE.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_ANNOTATION.key(), MongoDBVariantOptions.COLLECTION_ANNOTATION.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_VARIANTS.key(), MongoDBVariantOptions.COLLECTION_VARIANTS.defaultValue() + collectionSufix);

        variantStorageEngine.getOptions().putAll(renameCollections);
        return variantStorageEngine;
    }

    public long compareCollections(MongoDBCollection expectedCollection, MongoDBCollection actualCollection) {
        return compareCollections(expectedCollection, actualCollection, d -> d);
    }

    public long compareCollections(MongoDBCollection expectedCollection, MongoDBCollection actualCollection, Function<Document, Document> map) {
        QueryOptions sort = new QueryOptions(QueryOptions.SORT, Sorts.ascending("_id"));

        System.out.println("Comparing " + expectedCollection + " vs " + actualCollection);
        assertNotEquals(expectedCollection.toString(), actualCollection.toString());
        assertEquals(expectedCollection.count().first(), actualCollection.count().first());
        assertNotEquals(0L, expectedCollection.count().first().longValue());

        Iterator<Document> actualIterator = actualCollection.nativeQuery().find(new Document(), sort).iterator();
        Iterator<Document> expectedIterator = expectedCollection.nativeQuery().find(new Document(), sort).iterator();

        long c = 0;
        while (actualIterator.hasNext() && expectedIterator.hasNext()) {
            c++;
            Document actual = map.apply(actualIterator.next());
            Document expected = map.apply(expectedIterator.next());
            assertEquals(expected, actual);
        }
        assertFalse(actualIterator.hasNext());
        assertFalse(expectedIterator.hasNext());
        return c;
    }


    @Test
    public void stageAlreadyStagedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        long count = variantStorageEngine.getDBAdaptor().count(null).first();
        assertEquals(0L, count);

        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false), false, false, true);
    }


    @Test
    public void stageAlreadyMergedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        Long count = variantStorageEngine.getDBAdaptor().count(null).first();
        assertTrue(count > 0);

        thrown.expect(StoragePipelineException.class);
        thrown.expectCause(instanceOf(StorageEngineException.class));
        thrown.expectCause(hasMessage(containsString(StorageEngineException.alreadyLoaded(FILE_ID, studyConfiguration).getMessage())));
        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false), false, false, true);

    }

    @Test
    public void mergeAlreadyMergedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StoragePipelineResult storagePipelineResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        Long count = variantStorageEngine.getDBAdaptor().count(null).first();
        assertTrue(count > 0);

        StorageEngineException expectCause = StorageEngineException.alreadyLoaded(FILE_ID, studyConfiguration);

        thrown.expect(StoragePipelineException.class);
        thrown.expectCause(instanceOf(expectCause.getClass()));
        thrown.expectCause(hasMessage(containsString(expectCause.getMessage())));
        runETL(variantStorageEngine, storagePipelineResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true), false, false, true);

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

        StudyConfiguration studyConfiguration1 = new StudyConfiguration(1, "s1");
        StudyConfiguration studyConfiguration2 = new StudyConfiguration(2, "s2");
        URI file1 = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file2 = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file3 = getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file4 = getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file5 = getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        // Stage and merge file1
        runDefaultETL(file1, getVariantStorageEngine(), studyConfiguration1, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));
        runDefaultETL(file2, getVariantStorageEngine(), studyConfiguration1, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        runDefaultETL(file3, getVariantStorageEngine(), studyConfiguration2, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));
        runDefaultETL(file4, getVariantStorageEngine(), studyConfiguration2, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));
        // Stage and merge file5
        runDefaultETL(file5, getVariantStorageEngine(), studyConfiguration2, new ObjectMap()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));


        StudyConfigurationManager scm = getVariantStorageEngine("2").getDBAdaptor().getStudyConfigurationManager();

        StudyConfiguration newStudyConfiguration1 = new StudyConfiguration(1, "s1");
        newStudyConfiguration1.setSampleIds(studyConfiguration1.getSampleIds());    // Copy the sampleIds from the first load
        newStudyConfiguration1.setFileIds(studyConfiguration1.getFileIds());
        scm.updateStudyConfiguration(newStudyConfiguration1, null);

        StudyConfiguration newStudyConfiguration2 = new StudyConfiguration(2, "s2");
        newStudyConfiguration2.setSampleIds(studyConfiguration2.getSampleIds());    // Copy the sampleIds from the first load
        newStudyConfiguration2.setFileIds(studyConfiguration2.getFileIds());
        scm.updateStudyConfiguration(newStudyConfiguration2, null);

        runDefaultETL(file1, getVariantStorageEngine("2"), newStudyConfiguration1, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 1)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));
        runDefaultETL(file5, getVariantStorageEngine("2"), newStudyConfiguration2, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        compareCollections(getVariantStorageEngine("2").getDBAdaptor().getVariantsCollection(),
                getVariantStorageEngine().getDBAdaptor().getVariantsCollection());
    }

    @Test
    public void concurrentMerge() throws Exception {
        StudyConfiguration studyConfiguration1 = new StudyConfiguration(1, "s1");
        StudyConfiguration studyConfiguration2 = new StudyConfiguration(2, "s2");


        URI file1 = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file2 = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file3 = getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file4 = getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI file5 = getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        MongoDBVariantStorageEngine variantStorageManager1 = getVariantStorageEngine();
        runDefaultETL(file1, variantStorageManager1, studyConfiguration1, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 1)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);
        runDefaultETL(file2, variantStorageManager1, studyConfiguration1, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);
        StoragePipelineResult storagePipelineResult3 = runDefaultETL(file3, variantStorageManager1, studyConfiguration1, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);

        runDefaultETL(file4, variantStorageManager1, studyConfiguration2, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 4)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);
        StoragePipelineResult storagePipelineResult5 = runDefaultETL(file5, variantStorageManager1, studyConfiguration2, new ObjectMap()
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future mergeFile3 = executor.submit((Callable) () -> {
            runDefaultETL(storagePipelineResult3.getTransformResult(), newVariantStorageEngine(), studyConfiguration1, new ObjectMap()
//                    .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                    .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                    .append(MongoDBVariantOptions.STAGE.key(), true)
                    .append(MongoDBVariantOptions.MERGE.key(), true), false, true);
            return 0;
        });
        Future mergeFile5 = executor.submit((Callable) () -> {
            runDefaultETL(storagePipelineResult5.getTransformResult(), newVariantStorageEngine(), studyConfiguration2, new ObjectMap()
//                    .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                    .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                    .append(MongoDBVariantOptions.STAGE.key(), true)
                    .append(MongoDBVariantOptions.MERGE.key(), true), false, true);
            return 0;
        });
        executor.shutdown();
        executor.awaitTermination(4, TimeUnit.MINUTES);
        assertEquals(0, mergeFile3.get());
        assertEquals(0, mergeFile5.get());

        StudyConfigurationManager scm = getVariantStorageEngine("2").getDBAdaptor().getStudyConfigurationManager();

        StudyConfiguration newStudyConfiguration1 = new StudyConfiguration(1, "s1");
        newStudyConfiguration1.setSampleIds(studyConfiguration1.getSampleIds());    // Copy the sampleIds from the first load
        scm.updateStudyConfiguration(newStudyConfiguration1, null);

        StudyConfiguration newStudyConfiguration2 = new StudyConfiguration(2, "s2");
        newStudyConfiguration2.setSampleIds(studyConfiguration2.getSampleIds());    // Copy the sampleIds from the first load
        scm.updateStudyConfiguration(newStudyConfiguration2, null);

        runDefaultETL(file3, getVariantStorageEngine("2"), newStudyConfiguration1, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));
        runDefaultETL(file5, getVariantStorageEngine("2"), newStudyConfiguration2, new ObjectMap()
//                .append(VariantStorageEngine.Options.FILE_ID.key(), 5)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        compareCollections(getVariantStorageEngine("2").getDBAdaptor().getVariantsCollection(),
                getVariantStorageEngine().getDBAdaptor().getVariantsCollection(), document -> {
                    // Sort studies, because they can be inserted in a different order
                    if (document.containsKey(DocumentToVariantConverter.STUDIES_FIELD)) {
                        List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
                        studies.sort(Comparator.comparing(o -> o.getInteger(STUDYID_FIELD)));
                        studies.forEach(study ->
                                ((List<Document>) study.get(DocumentToStudyVariantEntryConverter.FILES_FIELD, List.class))
                                .forEach(file -> file.remove(DocumentToStudyVariantEntryConverter.FILEID_FIELD)));
                    }
                    return document;
                });
    }

    @Test
    public void checkCanLoadSampleBatchTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 1, false);
        studyConfiguration.getIndexedFiles().add(1);
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 2, true);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 3, false);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 4, true);
        studyConfiguration.getIndexedFiles().add(4);
    }

    @Test
    public void checkCanLoadSampleBatch2Test() throws StorageEngineException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 4, false);
        studyConfiguration.getIndexedFiles().add(4);
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 3, true);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 2, false);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 1, true);
        studyConfiguration.getIndexedFiles().add(1);
    }

    @Test
    public void checkCanLoadSampleBatchFailTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 3, 4));
        StorageEngineException e = MongoVariantStorageEngineException.alreadyLoadedSamples(studyConfiguration, 2);
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 2, false);
    }

    @Test
    public void checkCanLoadSampleBatchFail2Test() throws StorageEngineException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 2));
        StorageEngineException e = MongoVariantStorageEngineException.alreadyLoadedSomeSamples(studyConfiguration, 5);
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        MongoDBVariantStoragePipeline.checkCanLoadSampleBatch(studyConfiguration, 5, false);
    }

    @SuppressWarnings("unchecked")
    public StudyConfiguration createStudyConfiguration() {
        StudyConfiguration studyConfiguration = new StudyConfiguration(5, "study");
        LinkedHashSet<Integer> batch1 = new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4));
        LinkedHashSet<Integer> batch2 = new LinkedHashSet<>(Arrays.asList(5, 6, 7, 8));
        LinkedHashSet<Integer> batch3 = new LinkedHashSet<>(Arrays.asList(1, 3, 5, 7)); //Mixed batch
        studyConfiguration.getSamplesInFiles().put(1, batch1);
        studyConfiguration.getSamplesInFiles().put(2, batch1);
        studyConfiguration.getSamplesInFiles().put(3, batch2);
        studyConfiguration.getSamplesInFiles().put(4, batch2);
        studyConfiguration.getSamplesInFiles().put(5, batch3);
        studyConfiguration.getSampleIds().putAll(((Map) new ObjectMap()
                .append("s1", 1)
                .append("s2", 2)
                .append("s3", 3)
                .append("s4", 4)
                .append("s5", 5)
                .append("s6", 6)
                .append("s7", 7)
                .append("s8", 8)
        ));
        studyConfiguration.getFileIds().put("file1.vcf", 1);
        studyConfiguration.getFileIds().put("file2.vcf", 2);
        studyConfiguration.getFileIds().put("file3.vcf", 3);
        studyConfiguration.getFileIds().put("file4.vcf", 4);
        studyConfiguration.getFileIds().put("file5.vcf", 5);
        return studyConfiguration;
    }

    @Test
    @Override
    public void multiIndexPlatinum() throws Exception {
        super.multiIndexPlatinum(new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "DP,AD,PL"));
        checkPlatinumDatabase(d -> 17, Collections.singleton("0/0"));

        StudyConfiguration studyConfiguration = variantStorageEngine.getStudyConfigurationManager()
                .getStudyConfiguration(1, null).first();

        Iterator<BatchFileOperation> iterator = studyConfiguration.getBatches().iterator();
        assertEquals(MongoDBVariantOptions.DIRECT_LOAD.key(), iterator.next().getOperationName());
        while (iterator.hasNext()) {
            BatchFileOperation batchFileOperation = iterator.next();
            assertNotEquals(MongoDBVariantOptions.DIRECT_LOAD.key(), batchFileOperation.getOperationName());
        }
    }

    @Test
    public void multiIndexPlatinumNoUnknownGenotypes() throws Exception {
        super.multiIndexPlatinum(new ObjectMap(MongoDBVariantOptions.DEFAULT_GENOTYPE.key(), GenotypeClass.UNKNOWN_GENOTYPE));
        checkPlatinumDatabase(d -> ((List) d.get(FILES_FIELD)).size(), Collections.singleton(GenotypeClass.UNKNOWN_GENOTYPE));
    }

    @Test
    public void multiIndexPlatinumMergeSimple() throws Exception {
        super.multiIndexPlatinum(new ObjectMap(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC));
        checkPlatinumDatabase(d -> ((List) d.get(FILES_FIELD)).size(), Collections.singleton(GenotypeClass.UNKNOWN_GENOTYPE));
    }

    private void checkPlatinumDatabase(Function<Document, Integer> getExpectedSamples, Set<String> defaultGenotypes) throws Exception {
        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor()) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            StudyConfiguration sc1 = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(1, null).first();
            StudyConfiguration sc2 = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(2, null).first();
            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
                assertEquals(id, 2, studies.size());
                Document study1 = studies.stream().filter(d -> d.getInteger(STUDYID_FIELD).equals(sc1.getStudyId())).findAny().orElse(null);
                Document study2 = studies.stream().filter(d -> d.getInteger(STUDYID_FIELD).equals(sc2.getStudyId())).findAny().orElse(null);
                for (Document study : studies) {
                    Document gts = study.get(GENOTYPES_FIELD, Document.class);
                    Set<Integer> samples = new HashSet<>();
                    for (String defaultGenotype : defaultGenotypes) {
                        assertThat(gts.keySet(), not(hasItem(defaultGenotype)));
                    }
                    for (Map.Entry<String, Object> entry : gts.entrySet()) {
                        List<Integer> sampleIds = (List<Integer>) entry.getValue();
                        for (Integer sampleId : sampleIds) {
                            assertFalse(id, samples.contains(sampleId));
                            assertTrue(id, samples.add(sampleId));
                        }
                    }
                    assertEquals("\"" + id + "\" study: " + study.get(STUDYID_FIELD), (int) getExpectedSamples.apply(study), samples.size());
                }

                Document gt1 = study1.get(GENOTYPES_FIELD, Document.class);
                Document gt2 = study2.get(GENOTYPES_FIELD, Document.class);
                assertEquals(id, gt1.keySet(), gt2.keySet());
                for (String gt : gt1.keySet()) {
                    // Order is not important. Compare using a set
                    Set<Integer> expected = ((List<Integer>) gt1.get(gt, List.class)).stream().map(i -> i > 17 ? i - 17 : i).collect(Collectors.toSet());
                    Set<Integer> actual = ((List<Integer>) gt2.get(gt, List.class)).stream().map(i -> i > 17 ? i - 17 : i).collect(Collectors.toSet());
                    assertEquals(id + ":" + gt, expected, actual);
                }

                //Order is very important!
                assertEquals(id, study1.get(ALTERNATES_FIELD), study2.get(ALTERNATES_FIELD));

                //Order is not important.
                Map<String, Document> files1 = ((List<Document>) study1.get(FILES_FIELD))
                        .stream()
                        .collect(Collectors.toMap(
                                d -> sc1.getFileIds().inverse().get(Math.abs(d.getInteger(FILEID_FIELD))),
                                Function.identity()));
                Map<String, Document> files2 = ((List<Document>) study2.get(FILES_FIELD))
                        .stream()
                        .collect(Collectors.toMap(d -> sc2.getFileIds().inverse().get(Math.abs(d.getInteger(FILEID_FIELD))), Function.identity()));
                assertEquals(id, study1.get(FILES_FIELD, List.class).size(), study2.get(FILES_FIELD, List.class).size());
                assertEquals(id, files1.size(), files2.size());
                for (Map.Entry<String, Document> entry : files1.entrySet()) {
                    Document file1 = entry.getValue();
                    Document file2 = files2.get(entry.getKey());
                    Document attrs = file1.get(ATTRIBUTES_FIELD, Document.class);
                    Document attrs2 = file2.get(ATTRIBUTES_FIELD, Document.class);
                    String ac1 = Objects.toString(attrs.remove("AC"));
                    String ac2 = Objects.toString(attrs2.remove("AC"));
                    if (!ac1.equals(ac2)) {
                        ac1 = Arrays.stream(ac1.split(",")).map(Integer::parseInt).map(String::valueOf).collect(Collectors.joining(","));
                        ac2 = Arrays.stream(ac2.split(",")).map(Integer::parseInt).map(String::valueOf).collect(Collectors.joining(","));
                        assertTrue(id + ' ' + ac1 + ' ' + ac2 , ac1.startsWith(ac2) || ac2.startsWith(ac1));
                    }
                    String af1 = Objects.toString(attrs.remove("AF"));
                    String af2 = Objects.toString(attrs2.remove("AF"));
                    if (!af1.equals(af2)) {
                        af1 = Arrays.stream(af1.split(",")).map(Double::parseDouble).map(String::valueOf).collect(Collectors.joining(","));
                        af2 = Arrays.stream(af2.split(",")).map(Double::parseDouble).map(String::valueOf).collect(Collectors.joining(","));
                        assertTrue(id + ' ' + af1 + ' ' + af2 , af1.startsWith(af2) || af2.startsWith(af1));
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
                                assertTrue(id + ' ' + value1 + ' ' + value2 , value1.startsWith(value2) || value2.startsWith(value1));
                            } else {
                                assertEquals(data1, data2);
                            }
                        }
                    }
                    int fileId1 = (int) file1.remove(FILEID_FIELD);
                    int fileId2 = (int) file2.remove(FILEID_FIELD);
                    assertEquals(Integer.signum(fileId1), Integer.signum(fileId2));
                    assertEquals(id, file1, file2);
                }

            }
//            VariantExporter variantExporter = new VariantExporter(dbAdaptor);
//            URI uri = newOutputUri();
//            variantExporter.export(uri.resolve("s1.vcf"), VariantWriterFactory.VariantOutputFormat.VCF, new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), ".").append(VariantQueryParam.STUDIES.key(), 1), new QueryOptions(QueryOptions.SORT, true));
//            variantExporter.export(uri.resolve("s2.vcf"), VariantWriterFactory.VariantOutputFormat.VCF, new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), ".").append(VariantQueryParam.STUDIES.key(), 2), new QueryOptions(QueryOptions.SORT, true));
        }

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

        StudyConfiguration studyConfiguration = variantStorageEngine.getStudyConfigurationManager()
                .getStudyConfiguration(1, null).first();

        for (BatchFileOperation batchFileOperation : studyConfiguration.getBatches()) {
            assertEquals(MongoDBVariantOptions.DIRECT_LOAD.key(), batchFileOperation.getOperationName());
        }
    }

    @Test
    @Override
    public void multiRegionIndex() throws Exception {
        super.multiRegionIndex();

        checkLoadedVariants();

        StudyConfiguration studyConfiguration = variantStorageEngine.getStudyConfigurationManager()
                .getStudyConfiguration(1, null).first();

        for (BatchFileOperation batchFileOperation : studyConfiguration.getBatches()) {
            assertEquals(MongoDBVariantOptions.DIRECT_LOAD.key(), batchFileOperation.getOperationName());
        }
    }

    public void checkLoadedVariants() throws Exception {
        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor()) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);

//                assertEquals(id, 2, studies.size());
                for (Document study : studies) {
                    Document gts = study.get(GENOTYPES_FIELD, Document.class);
                    Set<Integer> samples = new HashSet<>();

                    for (Map.Entry<String, Object> entry : gts.entrySet()) {
                        List<Integer> sampleIds = (List<Integer>) entry.getValue();
                        for (Integer sampleId : sampleIds) {
                            String message = "var: " + id + " Duplicated sampleId " + sampleId + " in gt " + entry.getKey() + " : " + sampleIds;
                            assertFalse(message, samples.contains(sampleId));
                            assertTrue(message, samples.add(sampleId));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void removeFileMergeBasicTest() throws Exception {
        removeFileTest(new QueryOptions(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC));
    }

    @Override
    public void removeFileTest(QueryOptions params) throws Exception {
        MongoDBVariantStorageEngine variantStorageEngineExpected = getVariantStorageEngine("_expected");

        StudyConfiguration studyConfiguration1 = new StudyConfiguration(1, "Study1");
        StudyConfiguration studyConfiguration2 = new StudyConfiguration(2, "Study2");

        ObjectMap options = new ObjectMap(params)
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
        //Study1
        runDefaultETL(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngineExpected, studyConfiguration1, options);

        // Register file2, so internal IDs matches with the actual database
        URI file2Uri = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        int fileId2 = variantStorageEngineExpected.getStudyConfigurationManager().registerFile(studyConfiguration1, UriUtils.fileName(file2Uri));
        variantStorageEngineExpected.getStudyConfigurationManager().registerFileSamples(studyConfiguration1, fileId2, variantStorageEngineExpected.getVariantReaderUtils().readVariantFileMetadata(file2Uri), null);
//        runDefaultETL(getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
//                variantStorageEngineExpected, studyConfiguration1, options.append(VariantStorageEngine.Options.FILE_ID.key(), 2));

        //Study2
        runDefaultETL(getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngineExpected, studyConfiguration2, options);
        runDefaultETL(getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngineExpected, studyConfiguration2, options);
        runDefaultETL(getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngineExpected, studyConfiguration2, options);

        super.removeFileTest(params);
        VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();

        int studyId = studyConfiguration1.getStudyId();
        MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();
        System.out.println("variantsCollection = " + variantsCollection);
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyId);
        System.out.println("stageCollection = " + stageCollection);

//        assertEquals(variantsCollection.count().first(), stageCollection.count().first());

        Set<String> variantIds = variantsCollection.find(new Document(DocumentToVariantConverter.STUDIES_FIELD + '.' + STUDYID_FIELD, studyId), new QueryOptions(QueryOptions.INCLUDE, "_id")).getResult().stream().map(document -> document.getString("_id")).collect(Collectors.toSet());
        Set<String> stageIds = stageCollection.find(Filters.exists(String.valueOf(studyId)), new QueryOptions(QueryOptions.INCLUDE, "_id")).getResult().stream().map(document -> document.getString("_id")).collect(Collectors.toSet());

        if (!variantIds.equals(stageIds)) {
            for (String id : variantIds) {
                assertThat("Stage does not contain " + id, stageIds, hasItem(id));
            }
            for (String id : stageIds) {
                assertThat("Variants does not contain " + id, variantIds, hasItem(id));
            }
        }

        compareCollections(
                variantStorageEngineExpected.getDBAdaptor().getVariantsCollection(),
                dbAdaptor.getVariantsCollection(),
                d -> {
                    List<Document> list = (List<Document>) d.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
                    for (Document study : list) {
                        if (study.getInteger(STUDYID_FIELD) == 1) {
                            Document gts = study.get(GENOTYPES_FIELD, Document.class);
                            // Remove empty genotype lists
                            gts.entrySet().removeIf(entry -> ((List) entry.getValue()).isEmpty());
                            int numAlleles = 1;
                            Document ori = ((Document) study.get(FILES_FIELD, List.class).get(0)).get(ORI_FIELD, Document.class);
                            if (ori != null) {
                                numAlleles = ori.getString("s").split(":")[2].split(",").length;
                            }
                            // Remove unused alternates
                            if (numAlleles > 1) {
                                List alts = study.get(ALTERNATES_FIELD, List.class);
                                if (alts.size() > numAlleles - 1) {
                                    logger.warn(d.getString("_id") + " : Unused alternates " + alts.subList(numAlleles - 1, alts.size()));
                                }
                                study.put(ALTERNATES_FIELD, alts.subList(0, numAlleles - 1));
                            } else {
                                Object remove = study.remove(ALTERNATES_FIELD);
                                if (remove != null) {
                                    logger.warn(d.getString("_id") + " : " + gts.keySet() + " Unused alternates " + remove);
                                }
                            }
                        }
                    }
                    return d;
                });

    }
}
