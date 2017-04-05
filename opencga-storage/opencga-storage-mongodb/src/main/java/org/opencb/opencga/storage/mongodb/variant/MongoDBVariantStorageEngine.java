/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.FileStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.search.VariantSearchManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.metadata.MongoDBStudyConfigurationDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.io.db.VariantMongoDBAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoVariantImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.DB_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.FILES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLES;
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
        COLLECTION_STAGE("collection.stage",  "stage"),
        BULK_SIZE("bulkSize",  100),
        DEFAULT_GENOTYPE("defaultGenotype", Arrays.asList("0/0", "0|0")),
        ALREADY_LOADED_VARIANTS("alreadyLoadedVariants", 0),
        STAGE("stage", false),
        STAGE_RESUME("stage.resume", false),
        STAGE_PARALLEL_WRITE("stage.parallel.write", false),
        STAGE_CLEAN_WHILE_LOAD("stage.clean.while.load", true),
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

        public static boolean isResumeStage(ObjectMap options) {
            return options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue())
                    || options.getBoolean(STAGE_RESUME.key(), false);
        }

        public static boolean isResumeMerge(ObjectMap options) {
            return options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue())
                    || options.getBoolean(MERGE_RESUME.key(), false);
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
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        VariantMongoDBAdaptor mongoDbAdaptor = getDBAdaptor();
        return new DefaultVariantAnnotationManager(annotator, mongoDbAdaptor) {
            @Override
            protected VariantAnnotationDBWriter newVariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options) {
                return new VariantMongoDBAnnotationDBWriter(options, mongoDbAdaptor);
            }
        };
    }

    @Override
    public void dropFile(String study, int fileId) throws StorageEngineException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        getDBAdaptor().deleteFile(study, Integer.toString(fileId), new QueryOptions(options));
    }

    @Override
    public void dropStudy(String studyName) throws StorageEngineException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        getDBAdaptor().deleteStudy(studyName, new QueryOptions(options));
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

            boolean doStage = getOptions().getBoolean(STAGE.key());
            boolean doMerge = getOptions().getBoolean(MERGE.key());
            if (!doStage && !doMerge) {
                doStage = true;
                doMerge = true;
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
                        storagePipeline.getOptions().put(STAGE.key(), doStage);
                        storagePipeline.getOptions().put(MERGE.key(), doMerge);

                        logger.info("PreLoad '{}'", input);
                        input = storagePipeline.preLoad(input, outdirUri);
                        result.setPreLoadResult(input);

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
                            filesToMerge.add(storagePipeline.getOptions().getInt(Options.FILE_ID.key()));
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
                                            storagePipelineResult.getLoadStats().putIfAbsent(statsEntry.getKey(), statsEntry.getValue());
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
                    annotateLoadedFiles(outdirUri, inputFiles, results, getOptions());
                    calculateStatsForLoadedFiles(outdirUri, inputFiles, results, getOptions());
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

    private VariantMongoDBAdaptor newDBAdaptor() throws StorageEngineException {
        MongoCredentials credentials = getMongoCredentials();
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        ObjectMap options = getOptions();

        String variantsCollection = options.getString(COLLECTION_VARIANTS.key(), COLLECTION_VARIANTS.defaultValue());
        String filesCollection = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
        try {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager();
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(mongoDataStoreManager, credentials, variantsCollection, filesCollection,
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
            String collectionName = options.getString(COLLECTION_STUDIES.key(), COLLECTION_STUDIES.defaultValue());
            try {
                studyConfigurationManager = new StudyConfigurationManager(new MongoDBStudyConfigurationDBAdaptor(getMongoDataStoreManager(),
                        getMongoCredentials(), collectionName));
                return studyConfigurationManager;
//                return getDBAdaptor(dbName).getStudyConfigurationManager();
            } catch (UnknownHostException e) {
                throw new StorageEngineException("Unable to build MongoStorageConfigurationManager", e);
            }
        }
    }

    @Override
    public VariantQueryResult<Variant> get(Query query, QueryOptions options) throws StorageEngineException {
        if (options == null) {
            options = QueryOptions.empty();
        }
        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        // TODO: Use CacheManager ?
        if (options.getBoolean(VariantSearchManager.SUMMARY)
                || !query.containsKey(VariantQueryParam.FILES.key())
                && !query.containsKey(VariantQueryParam.FILTER.key())
                && !query.containsKey(VariantQueryParam.GENOTYPE.key())
                && !query.containsKey(VariantQueryParam.SAMPLES.key())
                && !returnedFields.contains(VariantField.STUDIES_FILES)
                && !returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)
                && searchActiveAndAlive()) {
            try {
                return getVariantSearchManager().query(dbName, query, options);
            } catch (IOException | VariantSearchException e) {
                throw Throwables.propagate(e);
            }
        } else {
            VariantMongoDBAdaptor dbAdaptor = getDBAdaptor();
            StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
            query = parseQuery(query, studyConfigurationManager);
            setDefaultTimeout(options);
            return dbAdaptor.get(query, options);
        }
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) throws StorageEngineException {
        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        if (options.getBoolean(VariantSearchManager.SUMMARY)
                || !query.containsKey(VariantQueryParam.FILES.key())
                && !query.containsKey(VariantQueryParam.FILTER.key())
                && !query.containsKey(VariantQueryParam.GENOTYPE.key())
                && !query.containsKey(VariantQueryParam.SAMPLES.key())
                && !returnedFields.contains(VariantField.STUDIES_FILES)
                && !returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)
                && searchActiveAndAlive()) {
            try {
                return getVariantSearchManager().iterator(dbName, query, options);
            } catch (IOException | VariantSearchException e) {
                throw Throwables.propagate(e);
            }
        } else {
            VariantMongoDBAdaptor dbAdaptor = getDBAdaptor();
            StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
            query = parseQuery(query, studyConfigurationManager);
//            setDefaultTimeout(options);
            return dbAdaptor.iterator(query, options);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws StorageEngineException {
        if (query.containsKey(VariantQueryParam.FILES.key())
                || query.containsKey(VariantQueryParam.FILTER.key())
                || query.containsKey(VariantQueryParam.GENOTYPE.key())
                || query.containsKey(VariantQueryParam.SAMPLES.key())
                || !searchActiveAndAlive()) {
            VariantMongoDBAdaptor dbAdaptor = getDBAdaptor();
            StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
            query = parseQuery(query, studyConfigurationManager);
            return dbAdaptor.count(query);
        } else {
            try {
                StopWatch watch = StopWatch.createStarted();
                long count = getVariantSearchManager().query(dbName, query, new QueryOptions(QueryOptions.LIMIT, 1)).getNumTotalResults();
                int time = (int) watch.getTime(TimeUnit.MILLISECONDS);
                return new QueryResult<>("count", time, 1, 1, "", "", Collections.singletonList(count));
            } catch (IOException | VariantSearchException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public Query parseQuery(Query originalQuery, StudyConfigurationManager studyConfigurationManager) throws StorageEngineException {
        // Copy input query! Do not modify original query!
        Query query = originalQuery == null ? new Query() : new Query(originalQuery);
        List<String> studyNames = studyConfigurationManager.getStudyNames(QueryOptions.empty());
        CellBaseUtils cellBaseUtils = getCellBaseUtils();

        if (isValidParam(query, VariantQueryParam.STUDIES)
                && studyNames.size() == 1
                && !isValidParam(query, FILES)
                && !isValidParam(query, SAMPLES)) {
            query.remove(VariantQueryParam.STUDIES.key());
        }

        if (isValidParam(query, VariantQueryParam.ANNOT_GO)) {
            String value = query.getString(VariantQueryParam.ANNOT_GO.key());
            // Check if comma separated of semi colon separated (AND or OR)
            VariantQueryUtils.QueryOperation queryOperation = checkOperator(value);
            // Split by comma or semi colon
            List<String> goValues = splitValue(value, queryOperation);

            if (queryOperation == VariantQueryUtils.QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_GO, value, "Unimplemented AND operator");
            }
            query.remove(VariantQueryParam.ANNOT_GO.key());
            List<String> genes = new ArrayList<>(query.getAsStringList(VariantQueryParam.GENE.key()));
            Set<String> genesByGo = cellBaseUtils.getGenesByGo(goValues);
            if (genesByGo.isEmpty()) {
                genes.add("none");
            } else {
                genes.addAll(genesByGo);
            }
            query.put(VariantQueryParam.GENE.key(), genes);
        }
        if (isValidParam(query, VariantQueryParam.ANNOT_EXPRESSION)) {
            String value = query.getString(VariantQueryParam.ANNOT_EXPRESSION.key());
            // Check if comma separated of semi colon separated (AND or OR)
            VariantQueryUtils.QueryOperation queryOperation = checkOperator(value);
            // Split by comma or semi colon
            List<String> expressionValues = splitValue(value, queryOperation);

            if (queryOperation == VariantQueryUtils.QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_EXPRESSION, value, "Unimplemented AND operator");
            }
            query.remove(VariantQueryParam.ANNOT_EXPRESSION.key());
            List<String> genes = new ArrayList<>(query.getAsStringList(VariantQueryParam.GENE.key()));
            Set<String> genesByExpression = cellBaseUtils.getGenesByExpression(expressionValues);
            if (genesByExpression.isEmpty()) {
                genes.add("none");
            } else {
                genes.addAll(genesByExpression);
            }
            query.put(VariantQueryParam.GENE.key(), genes);
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
