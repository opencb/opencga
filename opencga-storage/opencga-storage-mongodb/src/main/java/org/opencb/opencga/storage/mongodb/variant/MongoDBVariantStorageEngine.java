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
import org.opencb.biodata.models.variant.StudyEntry;
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
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.local.FileStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.mongodb.annotation.MongoDBVariantAnnotationManager;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.metadata.MongoDBVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.load.MongoVariantImporter;
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
    private StudyConfigurationManager studyConfigurationManager;

    public enum MongoDBVariantOptions {
        COLLECTION_VARIANTS("collection.variants", "variants"),
        COLLECTION_FILES("collection.files", "files"),
        COLLECTION_STUDIES("collection.studies",  "studies"),
        COLLECTION_PROJECT("collection.project",  "project"),
        COLLECTION_STAGE("collection.stage",  "stage"),
        COLLECTION_ANNOTATION("collection.annotation",  "annot"),
        BULK_SIZE("bulkSize",  100),
        DEFAULT_GENOTYPE("defaultGenotype", Arrays.asList("0/0", "0|0")),
        ALREADY_LOADED_VARIANTS("alreadyLoadedVariants", 0),
        LOADED_GENOTYPES("loadedGenotypes", null),

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
        return new MongoDBVariantStoragePipeline(configuration, STORAGE_ENGINE_ID, dbAdaptor);
    }

    @Override
    public VariantStatisticsManager newVariantStatisticsManager() throws StorageEngineException {
        return new MongoDBVariantStatisticsManager(getDBAdaptor());
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        VariantMongoDBAdaptor mongoDbAdaptor = getDBAdaptor();
        return new MongoDBVariantAnnotationManager(annotator, mongoDbAdaptor);
    }

    @Override
    public void removeFiles(String study, List<String> files) throws StorageEngineException {

        List<Integer> fileIds = preRemoveFiles(study, files);

        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());

        StudyConfigurationManager scm = getStudyConfigurationManager();
        Integer studyId = scm.getStudyId(study, null);

        Thread hook = scm.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, fileIds);
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            getDBAdaptor().removeFiles(study, files, new QueryOptions(options));
            postRemoveFiles(study, fileIds, false);
        } catch (Exception e) {
            postRemoveFiles(study, fileIds, true);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void removeStudy(String studyName) throws StorageEngineException {
        StudyConfigurationManager scm = getStudyConfigurationManager();
        int studyId = scm.lockAndUpdate(studyName, studyConfiguration -> {
            boolean resume = getOptions().getBoolean(RESUME.key(), RESUME.defaultValue());
            StudyConfigurationManager.addBatchOperation(studyConfiguration, REMOVE_OPERATION_NAME, Collections.emptyList(), resume,
                    BatchFileOperation.Type.REMOVE);
            return studyConfiguration;
        }).getStudyId();

        Thread hook = scm.buildShutdownHook(REMOVE_OPERATION_NAME, studyId, Collections.emptyList());
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
            getDBAdaptor().removeStudy(studyName, new QueryOptions(options));

            scm.lockAndUpdate(studyName, studyConfiguration -> {
                for (Integer fileId : studyConfiguration.getIndexedFiles()) {
                    getDBAdaptor().getStudyConfigurationManager().deleteVariantFileMetadata(studyId, fileId);
                }
                StudyConfigurationManager
                        .setStatus(studyConfiguration, BatchFileOperation.Status.READY, REMOVE_OPERATION_NAME, Collections.emptyList());
                studyConfiguration.getIndexedFiles().clear();
                studyConfiguration.getCalculatedStats().clear();
                studyConfiguration.getInvalidStats().clear();
                Integer defaultCohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
                studyConfiguration.getCohorts().put(defaultCohortId, Collections.emptySet());
                return studyConfiguration;
            });
        } catch (Exception e) {
            scm.lockAndUpdate(studyName, studyConfiguration -> {
                StudyConfigurationManager
                        .setStatus(studyConfiguration, BatchFileOperation.Status.ERROR, REMOVE_OPERATION_NAME, Collections.emptyList());
                return studyConfiguration;
            });
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
                            doDirectLoad = storagePipeline.checkCanLoadDirectly(input);
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
                    StudyConfiguration studyConfiguration = storageResultMap.get(inputFiles.get(0)).getStudyConfiguration();
                    ObjectMap options = getOptions();
                    options.put(Options.STUDY.key(), studyConfiguration.getStudyName());

                    annotateLoadedFiles(outdirUri, inputFiles, results, options);
                    calculateStatsForLoadedFiles(outdirUri, inputFiles, results, options);
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
    public VariantMongoDBAdaptor getDBAdaptor() throws StorageEngineException {
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

    /**
     * Decide if a query should be resolved using SearchManager or not.
     *
     * Applies some exceptions over the default method:
     *  - If true, and the query contains only filters for REGION and (optionally) STUDY, do not use SearchIndex
     *
     * @param query   Query
     * @param options QueryOptions
     * @return true if should resolve only with SearchManager
     * @throws StorageEngineException StorageEngineException
     */
    @Override
    protected boolean doQuerySearchManager(Query query, QueryOptions options) throws StorageEngineException {
        if (super.doQuerySearchManager(query, options)) {
            if (VariantSearchManager.UseSearchIndex.from(options).equals(VariantSearchManager.UseSearchIndex.YES)) {
                // Query search manager is mandatory
                return true;
            }
            // Get set of valid params. Remove Modifier params
            Set<VariantQueryParam> queryParams = VariantQueryUtils.validParams(query);
            queryParams.removeAll(MODIFIER_QUERY_PARAMS);

            // REGION + [ STUDY ]
            if (queryParams.contains(REGION) // Has region
                    // Optionally, has study
                    && (queryParams.size() == 1 || queryParams.size() == 2 && queryParams.contains(VariantQueryParam.STUDY))
                    && options.getBoolean(QueryOptions.SKIP_COUNT, DEFAULT_SKIP_COUNT)) {   // Do not require total count
                // Do not use SearchIndex either for intersect.
                options.put(VariantSearchManager.USE_SEARCH_INDEX, VariantSearchManager.UseSearchIndex.NO);
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    private VariantMongoDBAdaptor newDBAdaptor() throws StorageEngineException {
        MongoCredentials credentials = getMongoCredentials();
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        ObjectMap options = getOptions();

        String variantsCollection = options.getString(COLLECTION_VARIANTS.key(), COLLECTION_VARIANTS.defaultValue());
        String filesCollection = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
        try {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager();
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(mongoDataStoreManager, credentials, variantsCollection,
                    studyConfigurationManager, configuration);

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
    public StudyConfigurationManager getStudyConfigurationManager() throws StorageEngineException {
        ObjectMap options = getOptions();
        if (studyConfigurationManager != null) {
            return studyConfigurationManager;
        } else if (!options.getString(FileStudyConfigurationAdaptor.STUDY_CONFIGURATION_PATH, "").isEmpty()) {
            return super.getStudyConfigurationManager();
        } else {
            MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
            MongoDataStore db = mongoDataStoreManager.get(
                    getMongoCredentials().getMongoDbName(),
                    getMongoCredentials().getMongoDBConfiguration());
            studyConfigurationManager = new StudyConfigurationManager(new MongoDBVariantStorageMetadataDBAdaptorFactory(db, options));
            return studyConfigurationManager;
        }
    }

    @Override
    public Query preProcessQuery(Query originalQuery) throws StorageEngineException {
        Query query = super.preProcessQuery(originalQuery);
        List<String> studyNames = studyConfigurationManager.getStudyNames(QueryOptions.empty());
        CellBaseUtils cellBaseUtils = getCellBaseUtils();

        if (isValidParam(query, VariantQueryParam.STUDY)
                && studyNames.size() == 1
                && !isValidParam(query, FILE)
                && !isValidParam(query, SAMPLE)
                && !isValidParam(query, GENOTYPE)) {
            query.remove(VariantQueryParam.STUDY.key());
        }

        convertGoToGeneQuery(query, cellBaseUtils);
        convertExpressionToGeneQuery(query, cellBaseUtils);

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
        if (studyConfigurationManager != null) {
            studyConfigurationManager.close();
            studyConfigurationManager = null;
        }
        if (mongoDataStoreManager != null) {
            mongoDataStoreManager.close();
            mongoDataStoreManager = null;
        }
    }

}
