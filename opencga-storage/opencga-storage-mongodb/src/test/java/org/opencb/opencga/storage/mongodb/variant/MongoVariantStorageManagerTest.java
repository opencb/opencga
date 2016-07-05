/*
 * Copyright 2015 OpenCB
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

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.junit.Test;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager.MongoDBVariantOptions;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.exceptions.MongoVariantStorageManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;


/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class MongoVariantStorageManagerTest extends VariantStorageManagerTest implements MongoVariantStorageManagerTestUtils {

    @Test
    public void stageResumeFromErrorTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        BatchFileOperation operation = new BatchFileOperation(MongoDBVariantOptions.STAGE.key(),
                Collections.singletonList(FILE_ID), System.currentTimeMillis());
        operation.addStatus(new Date(System.currentTimeMillis() - 100), BatchFileOperation.Status.RUNNING);
        operation.addStatus(new Date(System.currentTimeMillis() - 50), BatchFileOperation.Status.ERROR);
        // Last status is ERROR

        studyConfiguration.getBatches().add(operation);
        MongoDBVariantStorageManager variantStorageManager = getVariantStorageManager();
        variantStorageManager.getDBAdaptor(DB_NAME).getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE_RESUME.key(), false)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
        );

    }

    @Test
    public void stageForceResumeTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        BatchFileOperation operation = new BatchFileOperation(MongoDBVariantOptions.STAGE.key(),
                Collections.singletonList(FILE_ID), System.currentTimeMillis());
        operation.addStatus(new Date(System.currentTimeMillis() - 100), BatchFileOperation.Status.RUNNING);
        operation.addStatus(new Date(System.currentTimeMillis() - 50), BatchFileOperation.Status.ERROR);
        operation.addStatus(new Date(System.currentTimeMillis()), BatchFileOperation.Status.RUNNING);
        // Last status is RUNNING
        studyConfiguration.getBatches().add(operation);
        MongoDBVariantStorageManager variantStorageManager = getVariantStorageManager();
        variantStorageManager.getDBAdaptor(DB_NAME).getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);

        try {
            runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration);
            fail();
        } catch (StorageManagerException e) {
            e.printStackTrace();
            MongoVariantStorageManagerException expected = MongoVariantStorageManagerException.fileBeingStagedException(FILE_ID, "variant-test-file.vcf.gz");
            assertThat(e, instanceOf(StorageETLException.class));
            assertThat(e, hasCause(instanceOf(expected.getClass())));
            assertThat(e, hasCause(hasMessage(is(expected.getMessage()))));
        }

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE_RESUME.key(), true)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
        );
    }


    @Test
    public void stageResumeFromError2Test() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        MongoDBVariantStorageManager variantStorageManager = getVariantStorageManager();
        VariantMongoDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);

        long stageCount = simulateStageError(studyConfiguration, dbAdaptor);

        // Resume stage and merge
        runDefaultETL(storageETLResult.getTransformResult(), variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false), false, true);

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
        MongoDBCollection stage = dataStore.getCollection(MongoDBVariantOptions.COLLECTION_STAGE.defaultValue());

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

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true), false, false, true);

        Long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertTrue(count > 0);
    }

    @Test
    public void loadStageConcurrent() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap(), true, false);

        StorageETLException exception = loadConcurrentAndCheck(studyConfiguration, storageETLResult);
        exception.printStackTrace();
    }

    @Test
    public void loadMergeSameConcurrent() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration,
                new ObjectMap()
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false), true, true);

        StorageETLException exception = loadConcurrentAndCheck(studyConfiguration, storageETLResult);
        exception.printStackTrace();
        assertEquals(1, exception.getResults().size());
        assertTrue(exception.getResults().get(0).isLoadExecuted());
        assertNotNull(exception.getResults().get(0).getLoadError());
        MongoVariantStorageManagerException expected = MongoVariantStorageManagerException.filesBeingMergedException(Collections.singletonList(FILE_ID));
        assertEquals(expected.getClass(), exception.getResults().get(0).getLoadError().getClass());
        assertEquals(expected.getMessage(), exception.getResults().get(0).getLoadError().getMessage());
    }

    public StorageETLException loadConcurrentAndCheck(StudyConfiguration studyConfiguration, StorageETLResult storageETLResult) throws InterruptedException, StorageManagerException, ExecutionException {

        AtomicReference<StorageETLException> exception = new AtomicReference<>(null);
        Callable<Integer> load = () -> {
            try {
                runDefaultETL(storageETLResult.getTransformResult(), getVariantStorageManager(), studyConfiguration, new ObjectMap()
                        .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                        .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false), false, true);
            } catch (StorageETLException e) {
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

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        assertTrue(dbAdaptor.count(new Query()).first() > 0);
        assertEquals(1, studyConfiguration.getIndexedFiles().size());
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(0).currentStatus());
        assertEquals(MongoDBVariantOptions.STAGE.key(), studyConfiguration.getBatches().get(0).getOperationName());
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(1).currentStatus());
        assertEquals(MongoDBVariantOptions.MERGE.key(), studyConfiguration.getBatches().get(1).getOperationName());

        assertEquals(1, loadOne.get() + loadTwo.get());
        return exception.get();
    }

    @Test
    public void stageWhileMerging() throws Exception {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StorageETLResult storageETLResult = runDefaultETL(inputUri, getVariantStorageManager(), studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true));
        Thread thread = new Thread(() -> {
            try {
                runDefaultETL(storageETLResult.getTransformResult(), getVariantStorageManager(), studyConfiguration, new ObjectMap(),
                        false, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        StudyConfigurationManager studyConfigurationManager = getVariantStorageManager().getDBAdaptor(DB_NAME).getStudyConfigurationManager();
        int secondFileId = 8;
        try {
            thread.start();
            Thread.sleep(200);

            BatchFileOperation opInProgress = new BatchFileOperation(MongoDBVariantOptions.MERGE.key(), Collections.singletonList(FILE_ID), 0);
            opInProgress.addStatus(BatchFileOperation.Status.RUNNING);
            MongoVariantStorageManagerException expected = MongoVariantStorageManagerException.operationInProgressException(opInProgress);
            thrown.expect(StorageETLException.class);
            thrown.expectCause(instanceOf(expected.getClass()));
            thrown.expectCause(hasMessage(is(expected.getMessage())));

            runDefaultETL(smallInputUri, getVariantStorageManager(), studyConfiguration,
                    new ObjectMap(VariantStorageManager.Options.FILE_ID.key(), secondFileId));
        } finally {
            System.out.println("Interrupt!");
            thread.interrupt();
            System.out.println("Join!");
            thread.join();
            System.out.println("EXIT");

            StudyConfiguration sc = studyConfigurationManager.getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
            // Second file is not staged or merged
            List<BatchFileOperation> ops = sc.getBatches().stream().filter(op -> op.getFileIds().contains(secondFileId)).collect(Collectors.toList());
            assertEquals(0, ops.size());
        }
    }

    @Test
    /**
     * Try to merge two different files in the same study at the same time.
     */
    public void mergeWhileMerging() throws Exception {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StorageETLResult storageETLResult = runDefaultETL(inputUri, getVariantStorageManager(), studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true));

        int secondFileId = 8;
        StorageETLResult storageETLResult2 = runDefaultETL(smallInputUri, getVariantStorageManager(), studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true).append(VariantStorageManager.Options.FILE_ID.key(), secondFileId));
        Thread thread = new Thread(() -> {
            try {
                runDefaultETL(storageETLResult.getTransformResult(), getVariantStorageManager(), studyConfiguration, new ObjectMap(),
                        false, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        StudyConfigurationManager studyConfigurationManager = getVariantStorageManager().getDBAdaptor(DB_NAME).getStudyConfigurationManager();
        try {
            thread.start();
            Thread.sleep(200);

            BatchFileOperation opInProgress = new BatchFileOperation(MongoDBVariantOptions.MERGE.key(), Collections.singletonList(FILE_ID), 0);
            opInProgress.addStatus(BatchFileOperation.Status.RUNNING);
            MongoVariantStorageManagerException expected = MongoVariantStorageManagerException.operationInProgressException(opInProgress);
            thrown.expect(StorageETLException.class);
            thrown.expectCause(instanceOf(expected.getClass()));
            thrown.expectCause(hasMessage(is(expected.getMessage())));

            runDefaultETL(storageETLResult2.getTransformResult(), getVariantStorageManager(), studyConfiguration,
                    new ObjectMap(MongoDBVariantOptions.STAGE.key(), false).append(VariantStorageManager.Options.FILE_ID.key(), secondFileId), false, true);
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
        }
    }

    @Test
    public void mergeResumeFirstFileTest() throws Exception {
        mergeResume(VariantStorageManagerTestUtils.inputUri, createStudyConfiguration(), o -> {});
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

        int studyId1 = studyConfiguration.getStudyId();
        int studyId2 = studyConfiguration.getStudyId() + 1;
        mergeResume(f5, studyConfiguration, variantStorageManager -> {
            try {
                ObjectMap objectMap = new ObjectMap()
                        .append(VariantStorageManager.Options.DB_NAME.key(), DB_NAME)
                        .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                        .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                        .append(VariantStorageManager.Options.FILE_ID.key(), null);

                runETL(variantStorageManager, f1, outputUri, objectMap
                                .append(VariantStorageManager.Options.STUDY_ID.key(), studyId1)
                                .append(VariantStorageManager.Options.STUDY_NAME.key(), studyConfiguration.getStudyName())
                        , true, true, true);
                runETL(variantStorageManager, f2, outputUri, objectMap
                                .append(VariantStorageManager.Options.STUDY_ID.key(), studyId1)
                                .append(VariantStorageManager.Options.STUDY_NAME.key(), studyConfiguration.getStudyName())
                        , true, true, true);
                runETL(variantStorageManager, f3, outputUri, objectMap
                                .append(VariantStorageManager.Options.STUDY_ID.key(), studyId2)
                                .append(VariantStorageManager.Options.STUDY_NAME.key(), studyConfiguration.getStudyName() + "_2")
                        , true, true, true);
                runETL(variantStorageManager, f4, outputUri, objectMap
                                .append(VariantStorageManager.Options.STUDY_ID.key(), studyId2)
                                .append(VariantStorageManager.Options.STUDY_NAME.key(), studyConfiguration.getStudyName() + "_2")
                        , true, true, true);
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    public void mergeResume(URI file, StudyConfiguration studyConfiguration, Consumer<VariantStorageManager> setUp) throws Exception {

        setUp.accept(variantStorageManager);

        StorageETLResult storageETLResult = runDefaultETL(file, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));


        int sleep = 0;
        int i = 0;
        Logger logger = LoggerFactory.getLogger("Test");
        AtomicBoolean success = new AtomicBoolean(false);
        while (true) {
            final int execution = ++i;
            Thread thread = new Thread(() -> {
                try {
                    runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                            .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                            .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                            .append(MongoDBVariantOptions.STAGE.key(), true)
                            .append(MongoDBVariantOptions.MERGE_RESUME.key(), execution > 1)
                            .append(MongoDBVariantOptions.MERGE.key(), true), false, false, true);
                    success.set(true);
                } catch (IOException | FileFormatException | StorageManagerException e) {
                    logger.error("Error loading in execution " + execution, e);
                }
            });
            logger.warn("+-----------------------+");
            logger.warn("+   Execution : " + execution);
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
            // Finish in less than 15 executions
            assertTrue(execution < 15);
        }
        // Do at least one interruption
        assertTrue(i > 1);
        assertTrue(success.get());

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        long count = dbAdaptor.count(null).first();
        System.out.println("count = " + count);
        assertTrue(count > 0);

        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        studyConfiguration.getHeaders().clear();
        System.out.println(studyConfiguration.toString());
        assertTrue(studyConfiguration.getIndexedFiles().contains(FILE_ID));
        assertEquals(BatchFileOperation.Status.READY, studyConfiguration.getBatches().get(1).currentStatus());

        // Insert in a different set of collections the same file
        MongoDBVariantStorageManager variantStorageManager = getVariantStorageManager("2");
        setUp.accept(variantStorageManager);

        runDefaultETL(file, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false));


        // Iterate over both collections to check that contain the same variants
        MongoDataStore mongoDataStore = getMongoDataStoreManager(DB_NAME).get(DB_NAME);
        MongoDBCollection variantsCollection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_VARIANTS.defaultValue());
        MongoDBCollection variants2Collection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_VARIANTS.defaultValue() + "2");
        MongoDBCollection stageCollection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_STAGE.defaultValue());
        MongoDBCollection stage2Collection = mongoDataStore.getCollection(MongoDBVariantOptions.COLLECTION_STAGE.defaultValue() + "2");

        assertEquals(count, compareCollections(variants2Collection, variantsCollection));
        compareCollections(stage2Collection, stageCollection);
    }

    public MongoDBVariantStorageManager getVariantStorageManager(String collectionSufix) throws Exception {
        MongoDBVariantStorageManager variantStorageManager = newVariantStorageManager();
        ObjectMap renameCollections = new ObjectMap()
                .append(MongoDBVariantOptions.COLLECTION_STUDIES.key(), MongoDBVariantOptions.COLLECTION_STUDIES.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_FILES.key(), MongoDBVariantOptions.COLLECTION_FILES.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_STAGE.key(), MongoDBVariantOptions.COLLECTION_STAGE.defaultValue() + collectionSufix)
                .append(MongoDBVariantOptions.COLLECTION_VARIANTS.key(), MongoDBVariantOptions.COLLECTION_VARIANTS.defaultValue() + collectionSufix);

        variantStorageManager.getOptions().putAll(renameCollections);
        return variantStorageManager;
    }

    public long compareCollections(MongoDBCollection expectedCollection, MongoDBCollection actualCollection) {
        return compareCollections(expectedCollection, actualCollection, d -> d);
    }

    public long compareCollections(MongoDBCollection expectedCollection, MongoDBCollection actualCollection, Function<Document, Document> map) {
        QueryOptions sort = new QueryOptions(QueryOptions.SORT, Sorts.ascending("_id"));

        System.out.println("Comparing " + expectedCollection + " vs " + actualCollection);
        assertNotEquals(expectedCollection.toString(), actualCollection.toString());
        assertEquals(expectedCollection.count().first(), actualCollection.count().first());

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

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertEquals(0L, count);

        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false), false, false, true);
    }


    @Test
    public void stageAlreadyMergedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        Long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertTrue(count > 0);

        thrown.expect(StorageETLException.class);
        thrown.expectCause(instanceOf(StorageManagerException.class));
        thrown.expectCause(hasMessage(containsString(StorageManagerException.alreadyLoaded(FILE_ID, studyConfiguration).getMessage())));
        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false), false, false, true);

    }

    @Test
    public void mergeAlreadyMergedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        Long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertTrue(count > 0);

        StorageManagerException expectCause = StorageManagerException.alreadyLoaded(FILE_ID, studyConfiguration);

        thrown.expect(StorageETLException.class);
        thrown.expectCause(instanceOf(expectCause.getClass()));
        thrown.expectCause(hasMessage(containsString(expectCause.getMessage())));
        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
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
        runDefaultETL(file1, getVariantStorageManager(), studyConfiguration1, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 1)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));
        runDefaultETL(file2, getVariantStorageManager(), studyConfiguration1, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 2)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));

        runDefaultETL(file3, getVariantStorageManager(), studyConfiguration2, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 3)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));
        runDefaultETL(file4, getVariantStorageManager(), studyConfiguration2, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 4)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), false));
        // Stage and merge file5
        runDefaultETL(file5, getVariantStorageManager(), studyConfiguration2, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 5)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));


        StudyConfigurationManager scm = getVariantStorageManager("2").getDBAdaptor(DB_NAME).getStudyConfigurationManager();

        StudyConfiguration newStudyConfiguration1 = new StudyConfiguration(1, "s1");
        newStudyConfiguration1.setSampleIds(studyConfiguration1.getSampleIds());    // Copy the sampleIds from the first load
        scm.updateStudyConfiguration(newStudyConfiguration1, null);

        StudyConfiguration newStudyConfiguration2 = new StudyConfiguration(2, "s2");
        newStudyConfiguration2.setSampleIds(studyConfiguration2.getSampleIds());    // Copy the sampleIds from the first load
        scm.updateStudyConfiguration(newStudyConfiguration2, null);

        runDefaultETL(file1, getVariantStorageManager("2"), newStudyConfiguration1, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 1)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));
        runDefaultETL(file5, getVariantStorageManager("2"), newStudyConfiguration2, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 5)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        compareCollections(getVariantStorageManager("2").getDBAdaptor(DB_NAME).getVariantsCollection(),
                getVariantStorageManager().getDBAdaptor(DB_NAME).getVariantsCollection());
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

        MongoDBVariantStorageManager variantStorageManager1 = getVariantStorageManager();
        runDefaultETL(file1, variantStorageManager1, studyConfiguration1, new ObjectMap()
                        .append(VariantStorageManager.Options.FILE_ID.key(), 1)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);
        runDefaultETL(file2, variantStorageManager1, studyConfiguration1, new ObjectMap()
                        .append(VariantStorageManager.Options.FILE_ID.key(), 2)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);
        StorageETLResult storageETLResult3 = runDefaultETL(file3, variantStorageManager1, studyConfiguration1, new ObjectMap()
                        .append(VariantStorageManager.Options.FILE_ID.key(), 3)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);

        runDefaultETL(file4, variantStorageManager1, studyConfiguration2, new ObjectMap()
                        .append(VariantStorageManager.Options.FILE_ID.key(), 4)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);
        StorageETLResult storageETLResult5 = runDefaultETL(file5, variantStorageManager1, studyConfiguration2, new ObjectMap()
                        .append(VariantStorageManager.Options.FILE_ID.key(), 5)
                        .append(MongoDBVariantOptions.STAGE.key(), true)
                        .append(MongoDBVariantOptions.MERGE.key(), false)
                , true, true);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future mergeFile3 = executor.submit((Callable) () -> {
            runDefaultETL(storageETLResult3.getTransformResult(), newVariantStorageManager(), studyConfiguration1, new ObjectMap()
                    .append(VariantStorageManager.Options.FILE_ID.key(), 3)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                    .append(MongoDBVariantOptions.STAGE.key(), true)
                    .append(MongoDBVariantOptions.MERGE.key(), true), false, true);
            return 0;
        });
        Future mergeFile5 = executor.submit((Callable) () -> {
            runDefaultETL(storageETLResult5.getTransformResult(), newVariantStorageManager(), studyConfiguration2, new ObjectMap()
                    .append(VariantStorageManager.Options.FILE_ID.key(), 5)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                    .append(MongoDBVariantOptions.STAGE.key(), true)
                    .append(MongoDBVariantOptions.MERGE.key(), true), false, true);
            return 0;
        });
        executor.shutdown();
        executor.awaitTermination(4, TimeUnit.MINUTES);
        assertEquals(0, mergeFile3.get());
        assertEquals(0, mergeFile5.get());

        StudyConfigurationManager scm = getVariantStorageManager("2").getDBAdaptor(DB_NAME).getStudyConfigurationManager();

        StudyConfiguration newStudyConfiguration1 = new StudyConfiguration(1, "s1");
        newStudyConfiguration1.setSampleIds(studyConfiguration1.getSampleIds());    // Copy the sampleIds from the first load
        scm.updateStudyConfiguration(newStudyConfiguration1, null);

        StudyConfiguration newStudyConfiguration2 = new StudyConfiguration(2, "s2");
        newStudyConfiguration2.setSampleIds(studyConfiguration2.getSampleIds());    // Copy the sampleIds from the first load
        scm.updateStudyConfiguration(newStudyConfiguration2, null);

        runDefaultETL(file3, getVariantStorageManager("2"), newStudyConfiguration1, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 3)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));
        runDefaultETL(file5, getVariantStorageManager("2"), newStudyConfiguration2, new ObjectMap()
                .append(VariantStorageManager.Options.FILE_ID.key(), 5)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantOptions.MERGE.key(), true));

        compareCollections(getVariantStorageManager("2").getDBAdaptor(DB_NAME).getVariantsCollection(),
                getVariantStorageManager().getDBAdaptor(DB_NAME).getVariantsCollection(), document -> {
                    // Sort studies, because they can be inserted in a different order
                    if (document.containsKey(DocumentToVariantConverter.STUDIES_FIELD)) {
                        ((List<Document>) document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class))
                                .sort((o1, o2) -> o1.getInteger(STUDYID_FIELD).compareTo(o2.getInteger(STUDYID_FIELD)));
                    }
                    return document;
                });
    }

    @Test
    public void checkCanLoadSampleBatchTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 1);
        studyConfiguration.getIndexedFiles().add(1);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 2);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 3);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 4);
        studyConfiguration.getIndexedFiles().add(4);
    }

    @Test
    public void checkCanLoadSampleBatch2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 4);
        studyConfiguration.getIndexedFiles().add(4);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 3);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 2);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 1);
        studyConfiguration.getIndexedFiles().add(1);
    }

    @Test
    public void checkCanLoadSampleBatchFailTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 3, 4));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("Another sample batch has been loaded");
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 2);
    }

    @Test
    public void checkCanLoadSampleBatchFail2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 2));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("There was some already indexed samples, but not all of them");
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 5);
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
        return studyConfiguration;
    }

    @Test
    @Override
    public void multiIndexPlatinum() throws Exception {
        super.multiIndexPlatinum();

        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME)) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
//                List alternates = studies.get(0).get(ALTERNATES_FIELD, List.class);
                if (id.equals("M:     16185:C:A") || id.equals("M:     16184:C:") || id.equals("M:     16184:CC:")) {
                    continue;
                }
                assertEquals(id, 2, studies.size());
                for (Document study : studies) {
                    Document gts = study.get(GENOTYPES_FIELD, Document.class);
                    Set<Integer> samples = new HashSet<>();

                    for (Map.Entry<String, Object> entry : gts.entrySet()) {
                        List<Integer> sampleIds = (List<Integer>) entry.getValue();
                        for (Integer sampleId : sampleIds) {
                            assertFalse(id, samples.contains(sampleId));
                            assertTrue(id, samples.add(sampleId));
                        }
                    }
                    assertEquals("\"" + id + "\" study: " + study.get(STUDYID_FIELD), 17, samples.size());
                }

                Document gt1 = studies.get(0).get(GENOTYPES_FIELD, Document.class);
                Document gt2 = studies.get(1).get(GENOTYPES_FIELD, Document.class);
                assertEquals(id, gt1.keySet(), gt2.keySet());
                for (String gt : gt1.keySet()) {
                    // Order is not important. Compare using a set
                    assertEquals(id + ":" + gt, new HashSet<>(gt1.get(gt, List.class)), new HashSet<>(gt2.get(gt, List.class)));
                }

                //Order is very important!
                assertEquals(id, studies.get(0).get(ALTERNATES_FIELD), studies.get(1).get(ALTERNATES_FIELD));

                //Order is not important.
                Map<String, Document> files1 = ((List<Document>) studies.get(0).get(FILES_FIELD))
                        .stream()
                        .collect(Collectors.toMap(d -> d.get(FILEID_FIELD).toString(), Function.identity()));
                Map<String, Document> files2 = ((List<Document>) studies.get(1).get(FILES_FIELD))
                        .stream()
                        .collect(Collectors.toMap(d -> d.get(FILEID_FIELD).toString(), Function.identity()));
                assertEquals(id, studies.get(0).get(FILES_FIELD, List.class).size(), studies.get(1).get(FILES_FIELD, List.class).size());
                assertEquals(id, files1.size(), files2.size());
                for (Map.Entry<String, Document> entry : files1.entrySet()) {
                    assertEquals(id, entry.getValue(), files2.get(entry.getKey()));
                }

            }
        }
    }



    @Test
    @Override
    public void multiRegionBatchIndex() throws Exception {
        super.multiRegionBatchIndex();
        checkLoadedVariants();
    }

    @Test
    @Override
    public void multiRegionIndex() throws Exception {
        super.multiRegionIndex();

        checkLoadedVariants();
    }

    public void checkLoadedVariants() throws Exception {
        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME)) {
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
    @Override
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        super.indexWithOtherFieldsExcludeGT();

        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME)) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                assertFalse(((Document) document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class).get(0))
                        .containsKey(GENOTYPES_FIELD));
                System.out.println("dbObject = " + document);
            }
        }

    }
}
