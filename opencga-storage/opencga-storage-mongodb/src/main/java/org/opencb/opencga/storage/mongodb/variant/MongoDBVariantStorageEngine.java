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

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Level;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.query.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.mongodb.annotation.MongoDBVariantAnnotationManager;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.metadata.MongoDBVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.load.MongoVariantImporter;
import org.opencb.opencga.storage.mongodb.variant.query.RegionVariantQueryExecutor;
import org.opencb.opencga.storage.mongodb.variant.stats.MongoDBVariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.RESUME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.*;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageEngine extends VariantStorageEngine {

    /*
     * This field defaultValue must be the same that the one at storage-configuration.yml
     */
    public static final String STORAGE_ENGINE_ID = "mongodb";

    // Connection to MongoDB.
    private MongoDataStoreManager mongoDataStoreManager = null;
    private final AtomicReference<VariantMongoDBAdaptor> dbAdaptor = new AtomicReference<>();
    private Logger logger = LoggerFactory.getLogger(MongoDBVariantStorageEngine.class);
    private VariantStorageMetadataManager metadataManager;

    public enum MongoDBVariantOptions {
        COLLECTION_VARIANTS("collection.variants", "variants"),
        COLLECTION_PROJECT("collection.project",  "project"),
        COLLECTION_STUDIES("collection.studies",  "studies"),
        COLLECTION_FILES("collection.files", "files"),
        COLLECTION_SAMPLES("collection.samples",  "samples"),
        COLLECTION_TASKS("collection.tasks",  "tasks"),
        COLLECTION_COHORTS("collection.cohorts",  "cohorts"),
        COLLECTION_STAGE("collection.stage",  "stage"),
        COLLECTION_ANNOTATION("collection.annotation",  "annot"),
        COLLECTION_TRASH("collection.trash", "trash"),
        BULK_SIZE("bulkSize",  100),
        DEFAULT_GENOTYPE("defaultGenotype", Arrays.asList("0/0", "0|0")),
        ALREADY_LOADED_VARIANTS("alreadyLoadedVariants", 0),

        PARALLEL_WRITE("parallel.write", false),

        STAGE("stage", false),
        STAGE_RESUME("stage.resume", false),
        STAGE_PARALLEL_WRITE("stage.parallel.write", false),
        STAGE_CLEAN_WHILE_LOAD("stage.clean.while.load", true),

        DIRECT_LOAD("direct_load", false),
        DIRECT_LOAD_PARALLEL_WRITE("direct_load.parallel.write", false),

        MERGE("merge", false),
        MERGE_SKIP("merge.skip", false), // Internal use only
        MERGE_RESUME("merge.resume", false),
        MERGE_IGNORE_OVERLAPPING_VARIANTS("merge.ignore-overlapping-variants", false),   //Do not look for overlapping variants
        MERGE_PARALLEL_WRITE("merge.parallel.write", false),
        MERGE_BATCH_SIZE("merge.batch.size", 10);          //Number of files to merge directly from first to second collection

        private final String key;
        private final Object value;

        MongoDBVariantOptions(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public static boolean isResume(ObjectMap options) {
            return options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue());
        }

        public static boolean isResumeStage(ObjectMap options) {
            return isResume(options) || options.getBoolean(STAGE_RESUME.key(), false);
        }

        public static boolean isResumeMerge(ObjectMap options) {
            return isResume(options) || options.getBoolean(MERGE_RESUME.key(), false);
        }

        public static boolean isDirectLoadParallelWrite(ObjectMap options) {
            return isParallelWrite(DIRECT_LOAD_PARALLEL_WRITE, options);
        }

        public static boolean isStageParallelWrite(ObjectMap options) {
            return isParallelWrite(STAGE_PARALLEL_WRITE, options);
        }

        public static boolean isMergeParallelWrite(ObjectMap options) {
            return isParallelWrite(MERGE_PARALLEL_WRITE, options);
        }

        private static boolean isParallelWrite(MongoDBVariantOptions option, ObjectMap options) {
            return options.getBoolean(PARALLEL_WRITE.key(), PARALLEL_WRITE.defaultValue())
                    || options.getBoolean(option.key(), option.defaultValue());
        }

        public String key() {
            return key;
        }

        @SuppressWarnings("unchecked")
        public <T> T defaultValue() {
            return (T) value;
        }
    }

    public MongoDBVariantStorageEngine() {
        //Disable MongoDB useless logging
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);
    }

    @Override
    public void testConnection() throws StorageEngineException {
        MongoCredentials credentials = getMongoCredentials();

        if (!credentials.check()) {
            logger.error("Connection to database '{}' failed", dbName);
            throw new StorageEngineException("Database connection test failed");
        }
    }

    @Override
    protected VariantImporter newVariantImporter() throws StorageEngineException {
        return new MongoVariantImporter(getDBAdaptor());
    }

    @Override
    public MongoDBVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        VariantMongoDBAdaptor dbAdaptor = connected ? getDBAdaptor() : null;
        return new MongoDBVariantStoragePipeline(configuration, STORAGE_ENGINE_ID, dbAdaptor, ioManagerProvider);
    }

    @Override
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new MongoDBVariantStatisticsManager(getDBAdaptor(), ioManagerProvider);
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        VariantMongoDBAdaptor mongoDbAdaptor = getDBAdaptor();
        return new MongoDBVariantAnnotationManager(annotator, mongoDbAdaptor, ioManagerProvider);
    }

    @Override
    public VariantSearchLoadResult searchIndex(Query inputQuery, QueryOptions inputQueryOptions, boolean overwrite)
            throws StorageEngineException, IOException, VariantSearchException {
        VariantSearchManager variantSearchManager = getVariantSearchManager();

        int deletedVariants;
        VariantSearchLoadResult searchIndex;
        long timeStamp = System.currentTimeMillis();

        if (configuration.getSearch().isActive() && variantSearchManager.isAlive(dbName)) {
            // First remove trashed variants.
            ProgressLogger progressLogger = new ProgressLogger("Variants removed from Solr");
            try (VariantDBIterator removedVariants = getDBAdaptor().trashedVariants(timeStamp)) {
                deletedVariants = variantSearchManager.delete(dbName, removedVariants, progressLogger);
                getDBAdaptor().cleanTrash(timeStamp);
            } catch (StorageEngineException | IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new StorageEngineException("Exception closing VariantDBIterator", e);
            }

            // Then, load new variants.
            searchIndex = super.searchIndex(inputQuery, inputQueryOptions, overwrite);
        } else {
            //The current dbName from the SearchEngine is not alive or does not exist. There is nothing to remove
            deletedVariants = 0;
            logger.debug("Skip removed variants!");

            // Try to index the rest of variants. This method will fail if the search engine is not alive
            searchIndex = super.searchIndex(inputQuery, inputQueryOptions, overwrite);

            // If the variants were loaded correctly, the trash can be clean up.
            getDBAdaptor().cleanTrash(timeStamp);
        }

        return new VariantSearchLoadResult(searchIndex.getNumProcessedVariants(), searchIndex.getNumLoadedVariants(), deletedVariants);
    }

    @Override
    public void removeFiles(String study, List<String> files) throws StorageEngineException {

        TaskMetadata task = preRemoveFiles(study, files);
        List<Integer> fileIds = task.getFileIds();

        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());

        VariantStorageMetadataManager scm = getMetadataManager();
        int studyId = scm.getStudyId(study);

        Thread hook = scm.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, task.getId());
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            getDBAdaptor().removeFiles(study, files, task.getTimestamp(), new QueryOptions(options));
            postRemoveFiles(study, fileIds, task.getId(), false);
        } catch (Exception e) {
            postRemoveFiles(study, fileIds, task.getId(), true);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void removeStudy(String studyName) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        AtomicReference<TaskMetadata> batchFileOperation = new AtomicReference<>();
        AtomicReference<TaskMetadata> taskMetadata = new AtomicReference<>();
        StudyMetadata studyMetadata = metadataManager.updateStudyMetadata(studyName, sm -> {
            boolean resume = getOptions().getBoolean(RESUME.key(), RESUME.defaultValue());
            taskMetadata.set(metadataManager.addRunningTask(sm.getId(),
                    REMOVE_OPERATION_NAME,
                    Collections.emptyList(),
                    resume,
                    TaskMetadata.Type.REMOVE));
            return sm;
        });
        int studyId = studyMetadata.getId();

        int taskId = taskMetadata.get().getId();
        Thread hook = metadataManager.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, taskId);
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
            getDBAdaptor().removeStudy(studyName, batchFileOperation.get().getTimestamp(), new QueryOptions(options));

            LinkedHashSet<Integer> indexedFiles = metadataManager.getIndexedFiles(studyId);
            for (Integer fileId : indexedFiles) {
                getDBAdaptor().getMetadataManager().removeVariantFileMetadata(studyId, fileId);
            }

            metadataManager.removeIndexedFiles(studyId, indexedFiles);

            metadataManager.setStatus(studyId, taskId, TaskMetadata.Status.READY);
        } catch (Exception e) {
            metadataManager.setStatus(studyId, taskId, TaskMetadata.Status.ERROR);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageEngineException {

        Map<URI, MongoDBVariantStoragePipeline> storageResultMap = new LinkedHashMap<>();
        Map<URI, StoragePipelineResult> resultsMap = new LinkedHashMap<>();
        LinkedList<StoragePipelineResult> results = new LinkedList<>();

        MemoryUsageMonitor monitor = new MemoryUsageMonitor();
        monitor.setDelay(5000);
//        monitor.start();
        try {
            for (URI inputFile : inputFiles) {
                StoragePipelineResult storagePipelineResult = new StoragePipelineResult(inputFile);
                MongoDBVariantStoragePipeline storagePipeline = newStoragePipeline(doLoad);
                storagePipeline.getOptions().append(VariantStorageEngine.Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);
                storageResultMap.put(inputFile, storagePipeline);
                resultsMap.put(inputFile, storagePipelineResult);
                results.add(storagePipelineResult);
            }


            if (doExtract) {
                for (Map.Entry<URI, MongoDBVariantStoragePipeline> entry : storageResultMap.entrySet()) {
                    URI uri = entry.getValue().extract(entry.getKey(), outdirUri);
                    resultsMap.get(entry.getKey()).setExtractResult(uri);
                }
            }

            if (doTransform) {
                for (Map.Entry<URI, MongoDBVariantStoragePipeline> entry : storageResultMap.entrySet()) {
                    StoragePipelineResult result = resultsMap.get(entry.getKey());
                    URI input = result.getExtractResult() == null ? entry.getKey() : result.getExtractResult();
                    transformFile(entry.getValue(), result, results, input, outdirUri);
                }
            }

            boolean doStage = doLoad && getOptions().getBoolean(STAGE.key());
            boolean doMerge = doLoad && getOptions().getBoolean(MERGE.key());
            if (!doStage && !doMerge) {
                doStage = doLoad;
                doMerge = doLoad;
            }

            if (doLoad) {
                int batchLoad = getOptions().getInt(MERGE_BATCH_SIZE.key(), MERGE_BATCH_SIZE.defaultValue());
                // Files to merge
                List<Integer> filesToMerge = new ArrayList<>(batchLoad);
                List<StoragePipelineResult> resultsToMerge = new ArrayList<>(batchLoad);
                List<Integer> mergedFiles = new ArrayList<>();

                Iterator<Map.Entry<URI, MongoDBVariantStoragePipeline>> iterator = storageResultMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<URI, MongoDBVariantStoragePipeline> entry = iterator.next();
                    StoragePipelineResult result = resultsMap.get(entry.getKey());
                    URI input = result.getPostTransformResult() == null ? entry.getKey() : result.getPostTransformResult();
                    MongoDBVariantStoragePipeline storagePipeline = entry.getValue();

                    StopWatch loadWatch = StopWatch.createStarted();
                    try {
                        boolean doDirectLoad;
                        // Decide if use direct load or not.
                        if (doStage && doMerge) {
                            doDirectLoad = storagePipeline.checkCanLoadDirectly(inputFiles);
                        } else {
                            doDirectLoad = false;
                        }

                        storagePipeline.getOptions().put(STAGE.key(), doStage);
                        storagePipeline.getOptions().put(MERGE.key(), doMerge);
                        storagePipeline.getOptions().put(DIRECT_LOAD.key(), doDirectLoad);

                        logger.info("PreLoad '{}'", input);
                        input = storagePipeline.preLoad(input, outdirUri);
                        result.setPreLoadResult(input);

                        if (doDirectLoad) {
                            storagePipeline.getOptions().put(STAGE.key(), false);
                            storagePipeline.getOptions().put(MERGE.key(), false);
                            storagePipeline.directLoad(input);
                            result.setLoadExecuted(true);
                            result.setLoadStats(storagePipeline.getLoadStats());
                            result.setLoadTimeMillis(loadWatch.getTime(TimeUnit.MILLISECONDS));
                        } else {
                            if (doStage) {
                                logger.info("Load - Stage '{}'", input);
                                storagePipeline.stage(input);
                                result.setLoadResult(input);
                                result.setLoadStats(storagePipeline.getLoadStats());
                                result.getLoadStats().put(STAGE.key(), true);
                                result.setLoadTimeMillis(loadWatch.getTime(TimeUnit.MILLISECONDS));
                            }

                            if (doMerge) {
                                logger.info("Load - Merge '{}'", input);
                                filesToMerge.add(storagePipeline.getFileId());
                                resultsToMerge.add(result);

                                if (filesToMerge.size() == batchLoad || !iterator.hasNext()) {
                                    StopWatch mergeWatch = StopWatch.createStarted();
                                    try {
                                        storagePipeline.merge(new ArrayList<>(filesToMerge));
                                    } catch (Exception e) {
                                        for (StoragePipelineResult storagePipelineResult : resultsToMerge) {
                                            storagePipelineResult.setLoadError(e);
                                        }
                                        throw new StoragePipelineException("Exception executing merge.", e, results);
                                    } finally {
                                        long mergeTime = mergeWatch.getTime(TimeUnit.MILLISECONDS);
                                        for (StoragePipelineResult storagePipelineResult : resultsToMerge) {
                                            storagePipelineResult.setLoadTimeMillis(storagePipelineResult.getLoadTimeMillis() + mergeTime);
                                            for (Map.Entry<String, Object> statsEntry : storagePipeline.getLoadStats().entrySet()) {
                                                storagePipelineResult.getLoadStats()
                                                        .putIfAbsent(statsEntry.getKey(), statsEntry.getValue());
                                            }
                                            storagePipelineResult.setLoadExecuted(true);
                                        }
                                        mergedFiles.addAll(filesToMerge);
                                        filesToMerge.clear();
                                        resultsToMerge.clear();
                                    }
                                } else {
                                    // We don't execute merge for this file
                                    storagePipeline.getOptions().put(MERGE.key(), false);
                                }
                            }
                        }


                        logger.info("PostLoad '{}'", input);
                        input = storagePipeline.postLoad(input, outdirUri);
                        result.setPostLoadResult(input);
                    } catch (Exception e) {
                        if (result.getLoadError() == null) {
                            result.setLoadError(e);
                        }
                        if (!(e instanceof StoragePipelineException)) {
                            throw new StoragePipelineException("Exception executing load: " + e.getMessage(), e, results);
                        } else {
                            throw e;
                        }
                    } finally {
                        if (result.getLoadTimeMillis() == 0) {
                            result.setLoadTimeMillis(loadWatch.getTime(TimeUnit.MILLISECONDS));
                        }
                        if (result.getLoadStats() == null) {
                            result.setLoadStats(storagePipeline.getLoadStats());
                        }
                    }

                }
                if (doMerge) {
                    StudyMetadata metadata = storageResultMap.get(inputFiles.get(0)).getStudyMetadata();
                    ObjectMap options = getOptions();
                    options.put(Options.STUDY.key(), metadata.getName());

                    annotateLoadedFiles(outdirUri, inputFiles, results, options);
                    calculateStatsForLoadedFiles(outdirUri, inputFiles, results, options);
                    searchIndexLoadedFiles(inputFiles, options);
                }
            }

        } finally {
//            monitor.interrupt();
            for (StoragePipeline storagePipeline : storageResultMap.values()) {
                storagePipeline.close();
            }
        }

        return results;
    }

    @Override
    public VariantMongoDBAdaptor getDBAdaptor() {
        // Lazy initialization of dbAdaptor
        if (dbAdaptor.get() == null) {
            synchronized (dbAdaptor) {
                if (dbAdaptor.get() == null) {
                    VariantMongoDBAdaptor variantMongoDBAdaptor = newDBAdaptor();
                    this.dbAdaptor.set(variantMongoDBAdaptor);
                }
            }
        }
        return dbAdaptor.get();
    }

    @Override
    protected List<VariantQueryExecutor> initVariantQueryExecutors() throws StorageEngineException {
        List<VariantQueryExecutor> executors = new ArrayList<>();

        // First, detect if it's a region only query.
        executors.add(new RegionVariantQueryExecutor(getDBAdaptor(), getStorageEngineId(), getOptions()));
        // Then, add the default executors
        executors.addAll(super.initVariantQueryExecutors());

        return executors;
    }

    private VariantMongoDBAdaptor newDBAdaptor() {
        MongoCredentials credentials = getMongoCredentials();
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        ObjectMap options = getOptions();

        String variantsCollection = options.getString(COLLECTION_VARIANTS.key(), COLLECTION_VARIANTS.defaultValue());
        String filesCollection = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
        try {
            VariantStorageMetadataManager variantStorageMetadataManager = getMetadataManager();
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(mongoDataStoreManager, credentials, variantsCollection,
                    variantStorageMetadataManager, configuration);

        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
        logger.debug("getting DBAdaptor to db: {}", credentials.getMongoDbName());
        return variantMongoDBAdaptor;
    }

    MongoCredentials getMongoCredentials() {

        // If no database name is provided, read from the configuration file
        if (StringUtils.isEmpty(dbName)) {
            dbName = getOptions().getString(DB_NAME.key(), DB_NAME.defaultValue());
        }

        DatabaseCredentials database = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getDatabase();

        try {
            return new MongoCredentials(database, dbName);
        } catch (IllegalOpenCGACredentialsException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() {
        ObjectMap options = getOptions();
        if (metadataManager != null) {
            return metadataManager;
        } else {
            MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
            MongoDataStore db = mongoDataStoreManager.get(
                    getMongoCredentials().getMongoDbName(),
                    getMongoCredentials().getMongoDBConfiguration());
            metadataManager = new VariantStorageMetadataManager(new MongoDBVariantStorageMetadataDBAdaptorFactory(db, options));
            return metadataManager;
        }
    }

    @Override
    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        if (isValidParam(originalQuery, SAMPLE_MENDELIAN_ERROR)) {
            throw VariantQueryException.unsupportedVariantQueryFilter(SAMPLE_MENDELIAN_ERROR, getStorageEngineId());
        }
        if (isValidParam(originalQuery, SAMPLE_DE_NOVO)) {
            throw VariantQueryException.unsupportedVariantQueryFilter(SAMPLE_DE_NOVO, getStorageEngineId());
        }

        Query query = super.preProcessQuery(originalQuery, options);
        List<String> studyNames = metadataManager.getStudyNames();

        if (isValidParam(query, VariantQueryParam.STUDY)
                && studyNames.size() == 1
                && !isNegated(query.getString(VariantQueryParam.STUDY.key()))
                && !isValidParam(query, FILE)
                && !isValidParam(query, FILTER)
                && !isValidParam(query, QUAL)
                && !isValidParam(query, INFO)
                && !isValidParam(query, SAMPLE)
                && !isValidParam(query, FORMAT)
                && !isValidParam(query, GENOTYPE)) {
            query.remove(VariantQueryParam.STUDY.key());
        }

        return query;
    }

    private synchronized MongoDataStoreManager getMongoDataStoreManager() {
        if (mongoDataStoreManager == null) {
            mongoDataStoreManager = new MongoDataStoreManager(getMongoCredentials().getDataStoreServerAddresses());
        }
        return mongoDataStoreManager;
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        if (dbAdaptor.get() != null) {
            dbAdaptor.get().close();
            dbAdaptor.set(null);
        }
        if (metadataManager != null) {
            metadataManager.close();
            metadataManager = null;
        }
        if (mongoDataStoreManager != null) {
            mongoDataStoreManager.close();
            mongoDataStoreManager = null;
        }
    }

}
