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

package org.opencb.opencga.storage.mongodb.variant.adaptors;

import com.mongodb.BasicDBList;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.*;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.config.storage.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.converters.*;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.trash.DocumentToTrashVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static org.opencb.commons.datastore.mongodb.MongoDBCollection.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.search.MongoDBVariantSearchIndexUtils.getSetIndexNotSynchronized;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Jacobo Coll <jacobo167@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptor implements VariantDBAdaptor {

    private boolean closeConnection;
    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final String collectionName;
    private final MongoDBCollection variantsCollection;
    private final StorageConfiguration storageConfiguration;
    private final MongoCredentials credentials;
    private final VariantMongoDBQueryParser queryParser;

    private VariantStorageMetadataManager metadataManager;
    private final ObjectMap configuration;
//    private CacheManager cacheManager;

    private static Logger logger = LoggerFactory.getLogger(VariantMongoDBAdaptor.class);

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
    // Number of opened dbAdaptors
    public static final AtomicInteger NUMBER_INSTANCES = new AtomicInteger(0);

    public VariantMongoDBAdaptor(MongoCredentials credentials, String variantsCollectionName,
                                 VariantStorageMetadataManager variantStorageMetadataManager, StorageConfiguration storageConfiguration)
            throws UnknownHostException {
        this(new MongoDataStoreManager(credentials.getDataStoreServerAddresses()), credentials, variantsCollectionName,
                variantStorageMetadataManager, storageConfiguration);
        this.closeConnection = true;
    }

    public VariantMongoDBAdaptor(MongoDataStoreManager mongoManager, MongoCredentials credentials, String variantsCollectionName,
                                 VariantStorageMetadataManager variantStorageMetadataManager,
                                 StorageConfiguration storageConfiguration) throws UnknownHostException {
        // MongoDB configuration
        this.closeConnection = false;
        this.credentials = credentials;
        this.mongoManager = mongoManager;
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        collectionName = variantsCollectionName;
        variantsCollection = db.getCollection(collectionName);
        this.metadataManager = variantStorageMetadataManager;
        this.storageConfiguration = storageConfiguration;
        StorageEngineConfiguration storageEngineConfiguration =
                storageConfiguration.getVariantEngine(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID);
        this.configuration = storageEngineConfiguration == null || storageEngineConfiguration.getOptions() == null
                ? new ObjectMap()
                : storageEngineConfiguration.getOptions();

        queryParser = new VariantMongoDBQueryParser(variantStorageMetadataManager);
        NUMBER_INSTANCES.incrementAndGet();
    }

    public MongoDBCollection getVariantsCollection() {
        return variantsCollection;
    }

    public MongoDBCollection getStageCollection(int studyId) {
        String stageCollectionName = configuration.getString(COLLECTION_STAGE.key(), COLLECTION_STAGE.defaultValue());
        // Ensure retro-compatibility.
        // If a "stage" collection exists, continue using one single stage collection for all the studies.
        // Otherwise, build the stage collection name as: 'stage_study_<study-id>'
        if (db.getCollectionNames().contains(stageCollectionName)) {
            return db.getCollection(stageCollectionName);
        } else {
            return db.getCollection(stageCollectionName + "_study_" + studyId);
        }
    }

    public MongoDBCollection getStudiesCollection() {
        return db.getCollection(configuration.getString(COLLECTION_STUDIES.key(), COLLECTION_STUDIES.defaultValue()));
    }

    public MongoDBCollection getAnnotationCollection(String name) {
        return db.getCollection(getAnnotationCollectionName(name));
    }

    public String getAnnotationCollectionName(String name) {
        ProjectMetadata.VariantAnnotationMetadata saved = getMetadataManager().getProjectMetadata()
                .getAnnotation().getSaved(name);

        return configuration.getString(COLLECTION_ANNOTATION.key(), COLLECTION_ANNOTATION.defaultValue()) + "_" + saved.getId();
    }

    public void dropAnnotationCollection(String name) {
        String annotationCollectionName = getAnnotationCollectionName(name);
        db.dropCollection(annotationCollectionName);
    }

    private MongoDBCollection getTrashCollection() {
        return db.getCollection(configuration.getString(COLLECTION_TRASH.key(), COLLECTION_TRASH.defaultValue()));
    }

    protected MongoDataStore getDB() {
        return db;
    }

    protected MongoCredentials getCredentials() {
        return credentials;
    }

    /**
     * Remove all the variants from the database resulting of executing the query.
     *
     * @param query   Query to be executed in the database
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A DataResult with the number of deleted variants
     */
    public DataResult remove(Query query, QueryOptions options) {
        Bson mongoQuery = queryParser.parseQuery(query);
        logger.debug("Delete to be executed: '{}'", mongoQuery.toString());
        return variantsCollection.remove(mongoQuery, options);
    }

    /**
     * Remove the given file from the database with all the samples it has.
     *
     * @param study     The study where the file belong
     * @param files     The file name to be deleted, it must belong to the study
     * @param timestamp Timestamp of the operation
     * @param options   Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A DataResult with the file deleted
     */
    public DataResult removeFiles(String study, List<String> files, long timestamp, QueryOptions options) {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        Integer studyId = studyMetadata.getId();
        List<Integer> fileIds = metadataManager.getFileIds(studyId, files);

        LinkedHashSet<Integer> otherIndexedFiles = metadataManager.getIndexedFiles(studyMetadata.getId());
        otherIndexedFiles.removeAll(fileIds);

        // First, remove the study entry that only contains the files to remove
        if (otherIndexedFiles.isEmpty()) {
            // If we are deleting all the files in the study, delete the whole study
            return removeStudy(study, timestamp, new QueryOptions("purge", true));
        }

        // Remove all the study entries that does not contain any of the other indexed files.
        // This include studies only with the files to remove and with negated fileIds (overlapped files)
        Bson studiesToRemoveQuery = elemMatch(DocumentToVariantConverter.STUDIES_FIELD,
                and(
                        eq(STUDYID_FIELD, studyId),
//                            in(FILES_FIELD + '.' + FILEID_FIELD, fileIds),
                        nin(FILES_FIELD + '.' + FILEID_FIELD, otherIndexedFiles)
                )
        );
        removeFilesFromStageCollection(studiesToRemoveQuery, studyId, fileIds);

        return removeFilesFromVariantsCollection(studiesToRemoveQuery, studyMetadata, fileIds, timestamp);
    }

    private void removeFilesFromStageCollection(Bson studiesToRemoveQuery, Integer studyId, List<Integer> fileIds) {
        int batchSize = 500;

        logger.info("Remove files from stage collection - step 1/3"); // Remove study if only contains removed files
        MongoDBCollection stageCollection = getStageCollection(studyId);
        int updatedStageDocuments = 0;
        try (MongoDBIterator<Document> cursor = getVariantsCollection()
                .nativeQuery()
                .find(studiesToRemoveQuery, Projections.include("_id"), new QueryOptions(MongoDBCollection.BATCH_SIZE, batchSize))) {
            List<String> ids = new ArrayList<>(batchSize);
            int i = 0;
            while (cursor.hasNext()) {
                ids.add(cursor.next().getString("_id"));
                Bson updateStage = combine(
                        pull(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString()),
                        unset(studyId.toString()));
                if (ids.size() == batchSize || !cursor.hasNext()) {
                    updatedStageDocuments += stageCollection.update(in("_id", ids), updateStage, new QueryOptions(MULTI, true))
                            .getNumUpdated();
                    i++;
                    logger.debug(i + " : clear stage ids = " + ids);
                    ids.clear();
                }
            }
        }

        List<Bson> studyUpdate = new ArrayList<>(fileIds.size());

        logger.info("Remove files from stage collection - step 2/3"); // Other studies
        for (Integer fileId : fileIds) {
            studyUpdate.add(unset(String.valueOf(studyId) + '.' + fileId));
        }
        updatedStageDocuments += stageCollection.update(eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString()),
                combine(studyUpdate), new QueryOptions(MULTI, true)).getNumUpdated();

        logger.info("Remove files from stage collection - step 3/3"); // purge
        long removedStageDocuments = removeEmptyVariantsFromStage(studyId);

        logger.info("Updated " + updatedStageDocuments + " documents from stage");
        logger.info("Removed " + removedStageDocuments + " documents from stage");
    }

    private DataResult removeFilesFromVariantsCollection(Bson studiesToRemoveQuery, StudyMetadata sm,
                                                                        List<Integer> fileIds, long timestamp) {
        Set<Integer> sampleIds = new HashSet<>();
        for (Integer fileId : fileIds) {
            sampleIds.addAll(metadataManager.getFileMetadata(sm.getId(), fileId).getSamples());
        }

        // Update and remove variants from variants collection
        int studyId = sm.getId();
        logger.info("Remove files from variants collection - step 1/3"); // Remove study if only contains removed files
        long updatedVariantsDocuments = removeStudyFromVariants(studyId, studiesToRemoveQuery, timestamp).getNumUpdated();

        // Remove also negated fileIds
        List<Integer> negatedFileIds = fileIds.stream().map(i -> -i).collect(Collectors.toList());
        fileIds.addAll(negatedFileIds);

        Bson query;
        // If default genotype is not the unknown genotype, we must iterate over all the documents in the study
        if (!sm.getAttributes().getString(DEFAULT_GENOTYPE.key()).equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
            query = eq(DocumentToVariantConverter.STUDIES_FIELD + '.' + STUDYID_FIELD, studyId);
        } else {
            query = elemMatch(DocumentToVariantConverter.STUDIES_FIELD,
                    and(
                            eq(STUDYID_FIELD, studyId),
                            in(FILES_FIELD + '.' + FILEID_FIELD, fileIds)
                    )
            );
        }

        List<Bson> updates = new ArrayList<>();
        updates.add(
                pull(DocumentToVariantConverter.STUDIES_FIELD + ".$." + FILES_FIELD,
                        in(FILEID_FIELD, fileIds)));
        for (String gt : sm.getAttributes().getAsStringList(LOADED_GENOTYPES.key())) {
            updates.add(
                    pullByFilter(
                            in(DocumentToVariantConverter.STUDIES_FIELD + ".$." + GENOTYPES_FIELD + '.' + gt, sampleIds)));
        }

        Bson update = combine(updates);
        logger.debug("removeFile: query = " + query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.debug("removeFile: update = " + update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        logger.info("Remove files from variants collection - step 2/3"); // Other studies
        DataResult result2 = getVariantsCollection().update(query, update, new QueryOptions(MULTI, true));
        logger.debug("removeFile: matched  = " + result2.getNumMatches());
        logger.debug("removeFile: modified = " + result2.getNumUpdated());

        logger.info("Remove files from variants collection - step 3/3"); // purge
        long removedVariantsDocuments = removeEmptyVariants();
        logger.info("Updated " + (updatedVariantsDocuments + result2.getNumUpdated()) + " documents from variants");
        logger.info("Removed " + removedVariantsDocuments + " documents from variants");

        return result2;
    }

    /**
     * Remove the given study from the database.
     *
     * @param studyName The study name to delete
     * @param timestamp Timestamp of the operation
     * @param options   Query modifiers, accepted values are: purge
     * @return A DataResult with the study deleted
     */
    public DataResult removeStudy(String studyName, long timestamp, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }

        Integer studyId = metadataManager.getStudyId(studyName);
        Bson query = queryParser.parseQuery(new Query(STUDY.key(), studyId));

        boolean purge = options.getBoolean("purge", true);

        logger.info("Remove study from variants collection - step 1/" + (purge ? '2' : '1'));
        DataResult result = removeStudyFromVariants(studyId, query, timestamp);

        if (purge) {
            logger.info("Remove study from variants collection - step 2/2");
            removeEmptyVariants();
        }

        Bson eq = eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString());
        Bson combine = combine(pull(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString()), unset(studyId.toString()));
        logger.debug("removeStudy: stage query = " + eq.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.debug("removeStudy: stage update = " + combine.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.info("Remove study from stage collection - step 1/" + (purge ? '2' : '1'));
        getStageCollection(studyId).update(eq, combine, new QueryOptions(MULTI, true));

        if (purge) {
            logger.info("Remove study from stage collection - step 2/2");
            removeEmptyVariantsFromStage(studyId);
        }
        return result;
    }

    private DataResult removeStudyFromVariants(int studyId, Bson query, long timestamp) {
        // { $pull : { files : {  sid : <studyId> } } }
        Bson update = combine(
                pull(DocumentToVariantConverter.STUDIES_FIELD, eq(STUDYID_FIELD, studyId)),
                pull(DocumentToVariantConverter.STATS_FIELD, eq(DocumentToVariantStatsConverter.STUDY_ID, studyId)),
                getSetIndexNotSynchronized(timestamp)
        );
        logger.debug("removeStudy: query = {}", query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.debug("removeStudy: update = {}", update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        DataResult result = variantsCollection.update(query, update, new QueryOptions(MULTI, true));
        logger.debug("removeStudy: matched  = {}", result.getNumMatches());
        logger.debug("removeStudy: modified = {}", result.getNumUpdated());

        return result;
    }

    /**
     * Remove empty variants from the variants collection, and move to the trash collection.
     *
     * @return number of removed variants.
     */
    private long removeEmptyVariants() {
        long ts = System.currentTimeMillis();

        Bson purgeQuery = exists(DocumentToVariantConverter.STUDIES_FIELD + '.' + STUDYID_FIELD, false);

        MongoPersistentCursor iterator = new MongoPersistentCursor(variantsCollection, purgeQuery, queryParser.createProjection(
                new Query(),
                new QueryOptions(QueryOptions.INCLUDE, DocumentToVariantConverter.REQUIRED_FIELDS_SET)), new QueryOptions());

        MongoDBCollection trashCollection = getTrashCollection();
        trashCollection.createIndex(new Document(DocumentToTrashVariantConverter.TIMESTAMP_FIELD, 1), new ObjectMap());

        long deletedDocuments = 0;
        int deleteBatchSize = 1000;
        List<String> documentsToDelete = new ArrayList<>(deleteBatchSize);
        List<Document> documentsToInsert = new ArrayList<>(deleteBatchSize);

        while (iterator.hasNext()) {
            Document next = iterator.next();
            documentsToDelete.add(next.getString("_id"));
            next.append(DocumentToTrashVariantConverter.TIMESTAMP_FIELD, ts);
            documentsToInsert.add(next);
            if (documentsToDelete.size() == deleteBatchSize || !iterator.hasNext()) {
                if (documentsToDelete.isEmpty()) {
                    // Really unlikely, but may happen if the total number of variants to remove was multiple of "deleteBatchSize"
                    break;
                }

                // First, update the deletedVariants Collection
                List<Bson> queries = documentsToDelete.stream().map(id -> Filters.eq("_id", id)).collect(Collectors.toList());
                trashCollection.update(queries, documentsToInsert, new QueryOptions(UPSERT, true).append(REPLACE, true));

                // Then, remove the documents from the variants collection
                long deletedCount = variantsCollection.remove(and(purgeQuery, in("_id", documentsToDelete)), new QueryOptions(MULTI, true))
                        .getNumDeleted();

                // Check if there were some errors
                if (deletedCount != documentsToDelete.size()) {
                    throw new IllegalStateException("Some variants were not deleted!");
                }
                deletedDocuments += deletedCount;

                documentsToDelete.clear();
                documentsToInsert.clear();
            }
        }

        return deletedDocuments;
    }

    public VariantDBIterator trashedVariants(long timeStamp) {
        MongoDBCollection collection = getTrashCollection();
        return VariantMongoDBIterator.persistentIterator(
                collection,
                lte(DocumentToTrashVariantConverter.TIMESTAMP_FIELD, timeStamp),
                new Document(),
                new QueryOptions(),
                new DocumentToTrashVariantConverter());
    }

    public long cleanTrash(long timeStamp) {
        MongoDBCollection collection = getTrashCollection();
        // Try to get one variant beyond the ts. If exists, remove by query. Otherwise, remove the whole collection.
        QueryOptions queryOptions = new QueryOptions(QueryOptions.LIMIT, 1);
        int results = collection.find(gt(DocumentToTrashVariantConverter.TIMESTAMP_FIELD, timeStamp), queryOptions).getNumResults();

        if (results > 0) {
            return collection.remove(lte(DocumentToTrashVariantConverter.TIMESTAMP_FIELD, timeStamp), null).getNumDeleted();
        } else {
            long numElements = collection.count().getNumMatches();
            db.dropCollection(configuration.getString(COLLECTION_TRASH.key(), COLLECTION_TRASH.defaultValue()));
            return numElements;
        }
    }

    private long removeEmptyVariantsFromStage(int studyId) {
        Bson purgeQuery = eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, Collections.emptyList());
        return getStageCollection(studyId).remove(purgeQuery, new QueryOptions(MULTI, true)).getNumDeleted();
    }

    @Override
    public VariantQueryResult<Variant> get(ParsedVariantQuery variantQuery, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        } else {
            options = new QueryOptions(options);
        }
        if (options.getBoolean(QueryOptions.COUNT) && options.getInt(QueryOptions.LIMIT, -1) == 0) {
            DataResult<Long> count = count(variantQuery);
            DataResult<Variant> result = new DataResult<>(count.getTime(), count.getEvents(), 0, Collections.emptyList(), count.first());
            return addSamplesMetadataIfRequested(result, variantQuery.getQuery(), options, getMetadataManager());
        } else if (!options.getBoolean(QueryOptions.COUNT) && options.getInt(QueryOptions.LIMIT, -1) == 0) {
            DataResult<Variant> result = new DataResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), -1);
            return addSamplesMetadataIfRequested(result, variantQuery.getQuery(), options, getMetadataManager());
        }

        VariantQueryProjection variantQueryProjection = variantQuery.getProjection();
        Document mongoQuery = queryParser.parseQuery(variantQuery.getQuery());
        Document projection = queryParser.createProjection(variantQuery.getQuery(), options, variantQueryProjection);

        if (options.getBoolean("explain", false)) {
            Document explain = variantsCollection.nativeQuery().explain(mongoQuery, projection, options);
            logger.debug("MongoDB Explain = {}", explain.toJson(new JsonWriterSettings(JsonMode.SHELL, true)));
        }

        DocumentToVariantConverter converter = getDocumentToVariantConverter(variantQuery.getQuery(), variantQueryProjection);
        return addSamplesMetadataIfRequested(variantsCollection.find(mongoQuery, projection, converter, options),
                variantQuery.getQuery(), options, getMetadataManager());
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String varStr, String studyName, String sampleName, QueryOptions options,
                                                 int windowsSize) {
        StopWatch watch = StopWatch.createStarted();

        Variant variant = new Variant(varStr);
        Region region = new Region(variant.getChromosome(), variant.getStart(), variant.getEnd());
        Query query = new Query(REGION.key(), region)
                .append(REFERENCE.key(), variant.getReference())
                .append(ALTERNATE.key(), variant.getAlternate())
                .append(STUDY.key(), studyName)
                .append(INCLUDE_STUDY.key(), studyName)
                .append(INCLUDE_SAMPLE.key(), sampleName);
        VariantQueryResult<Variant> queryResult = get(query, new QueryOptions());
        variant = queryResult.first();
        if (variant != null && !variant.getStudies().isEmpty()) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            Integer psIdx = studyEntry.getSampleDataKeyPosition(VCFConstants.PHASE_SET_KEY);
            if (psIdx != null) {
                String ps = studyEntry.getSamples().get(0).getData().get(psIdx);
                if (!ps.equals(DocumentToSamplesConverter.UNKNOWN_FIELD)) {
                    sampleName = studyEntry.getOrderedSamplesName().get(0);

                    region.setStart(region.getStart() > windowsSize ? region.getStart() - windowsSize : 0);
                    region.setEnd(region.getEnd() + windowsSize);
                    query.remove(REFERENCE.key());
                    query.remove(ALTERNATE.key());
                    query.remove(INCLUDE_STUDY.key());
                    query.remove(INCLUDE_SAMPLE.key());
                    queryResult = get(query, new QueryOptions(QueryOptions.SORT, true));
                    Iterator<Variant> iterator = queryResult.getResults().iterator();
                    while (iterator.hasNext()) {
                        Variant next = iterator.next();
                        if (!next.getStudies().isEmpty()) {
                            if (!ps.equals(next.getStudies().get(0).getSampleData(sampleName, VCFConstants.PHASE_SET_KEY))) {
                                iterator.remove();
                            }
                        }
                    }
                    queryResult.setNumResults(queryResult.getResults().size());
                    queryResult.setNumMatches(queryResult.getResults().size());
                    watch.stop();
                    queryResult.setTime(((int) watch.getTime()));
                    return addSamplesMetadataIfRequested(queryResult, query, options, metadataManager);
                }
            }
        }
        watch.stop();
        return new VariantQueryResult<>(((int) watch.getTime()), 0, 0, null, Collections.emptyList(), null,
                MongoDBVariantStorageEngine.STORAGE_ENGINE_ID);
    }

    @Override
    public DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) {
        query = query == null ? new Query() : query;
        validateAnnotationQuery(query);
        options = validateAnnotationQueryOptions(options);
        Document mongoQuery = queryParser.parseQuery(query);
        Document projection = queryParser.createProjection(query, options);

        MongoDBCollection annotationCollection;
        if (name.equals(VariantAnnotationManager.CURRENT)) {
            annotationCollection = getVariantsCollection();
        } else {
            annotationCollection = getAnnotationCollection(name);
        }
        VariantQueryProjection selectVariantElements = VariantQueryProjectionParser.parseVariantQueryFields(
                query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ANNOTATION), metadataManager);

        DocumentToVariantConverter converter = getDocumentToVariantConverter(new Query(), selectVariantElements);
        DataResult<Variant> result = annotationCollection.find(mongoQuery, projection, converter, options);

        List<VariantAnnotation> annotations = result.getResults()
                .stream()
                .map(Variant::getAnnotation)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new DataResult<>(result.getTime(), result.getEvents(), annotations.size(), annotations, result.getNumMatches());
    }

    @Override
    public DataResult<Long> count(ParsedVariantQuery variantQuery) {
        Document mongoQuery = queryParser.parseQuery(variantQuery.getQuery());
        DataResult<Long> count = variantsCollection.count(mongoQuery);
        count.setResults(Collections.singletonList(count.getNumMatches()));
        return count;
    }

    @Override
    public DataResult distinct(Query query, String field) {
        String documentPath;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_ENSEMBL_GENE_ID_FIELD;
                break;
            case "ensemblTranscript":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_ENSEMBL_TRANSCRIPT_ID_FIELD;
                break;
            case "ct":
            case "consequence_type":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD;
                break;
            default:
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_GENE_NAME_FIELD;
                break;
        }
        Document mongoQuery = queryParser.parseQuery(query);
        return variantsCollection.distinct(documentPath, mongoQuery);
    }

    @Override
    public VariantDBIterator iterator(ParsedVariantQuery variantQuery, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        return iteratorFinal(variantQuery, options);
    }

    private VariantDBIterator iteratorFinal(final ParsedVariantQuery variantQuery, final QueryOptions options) {
        VariantQueryProjection variantQueryProjection = variantQuery.getProjection();
        Document mongoQuery = queryParser.parseQuery(variantQuery);
        Document projection = queryParser.createProjection(variantQuery.getQuery(), options, variantQueryProjection);
        DocumentToVariantConverter converter = getDocumentToVariantConverter(variantQuery.getQuery(), variantQueryProjection);
        options.putIfAbsent(MongoDBCollection.BATCH_SIZE, 100);

        // Short unsorted queries with timeout or limit don't need the persistent cursor.
        if (options.containsKey(QueryOptions.TIMEOUT)
                || options.containsKey(QueryOptions.LIMIT)
                || !options.getBoolean(QueryOptions.SORT, false)) {
            StopWatch stopWatch = StopWatch.createStarted();
            VariantMongoDBIterator dbIterator = new VariantMongoDBIterator(
                    () -> variantsCollection.nativeQuery().find(mongoQuery, projection, options), converter);
            dbIterator.setTimeFetching(dbIterator.getTimeFetching() + stopWatch.getNanoTime());
            return dbIterator;
        } else {
            logger.debug("Using mongodb persistent iterator");
            return VariantMongoDBIterator.persistentIterator(variantsCollection, mongoQuery, projection, options, converter);
        }
    }

    public MongoDBIterator<Document> nativeIterator(Query query, QueryOptions options, boolean persistent) {
        if (query == null) {
            query = new Query();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        Document mongoQuery = queryParser.parseQuery(query);
        Document projection = queryParser.createProjection(query, options);
        options.putIfAbsent(MongoDBCollection.BATCH_SIZE, 100);


        if (persistent) {
            logger.debug("Using mongodb persistent iterator");
            return new MongoDBIterator<>(new MongoPersistentCursor(variantsCollection, mongoQuery, projection, options), -1);
        } else {
            return variantsCollection.nativeQuery().find(mongoQuery, projection, options);
        }
    }

    @Override
    public DataResult getFrequency(ParsedVariantQuery query, Region region, int regionIntervalSize) {
        // db.variants.aggregate( { $match: { $and: [ {chr: "1"}, {start: {$gt: 251391, $lt: 2701391}} ] }},
        //                        { $group: { _id: { $subtract: [ { $divide: ["$start", 20000] }, { $divide: [{$mod: ["$start", 20000]},
        // 20000] } ] },
        //                                  totalCount: {$sum: 1}}})

        QueryOptions options = new QueryOptions();

        // If interval is not provided is set to the value that returns 200 values
        if (regionIntervalSize <= 0) {
//            regionIntervalSize = options.getInt("interval", (region.getEnd() - region.getStart()) / 200);
            regionIntervalSize = (region.getEnd() - region.getStart()) / 200;
        }

        Document start = new Document("$gt", region.getStart());
        start.append("$lt", region.getEnd());

        BasicDBList andArr = new BasicDBList();
        andArr.add(new Document(DocumentToVariantConverter.CHROMOSOME_FIELD, region.getChromosome()));
        andArr.add(new Document(DocumentToVariantConverter.START_FIELD, start));

        // Parsing the rest of options
        Document mongoQuery = queryParser.parseQuery(query);
        if (!mongoQuery.isEmpty()) {
            andArr.add(mongoQuery);
        }
        Document match = new Document("$match", new Document("$and", andArr));

//        qb.and("_at.chunkIds").in(chunkIds);
//        qb.and(DBObjectToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
//        qb.and(DBObjectToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
//
//        List<String> chunkIds = getChunkIds(region);
//        DBObject regionObject = new Document("_at.chunkIds", new Document("$in", chunkIds))
//                .append(DBObjectToVariantConverter.END_FIELD, new Document("$gte", region.getStart()))
//                .append(DBObjectToVariantConverter.START_FIELD, new Document("$lte", region.getEnd()));

        BasicDBList divide1 = new BasicDBList();
        divide1.add("$start");
        divide1.add(regionIntervalSize);

        BasicDBList divide2 = new BasicDBList();
        divide2.add(new Document("$mod", divide1));
        divide2.add(regionIntervalSize);

        BasicDBList subtractList = new BasicDBList();
        subtractList.add(new Document("$divide", divide1));
        subtractList.add(new Document("$divide", divide2));

        Document subtract = new Document("$subtract", subtractList);
        Document totalCount = new Document("$sum", 1);
        Document g = new Document("_id", subtract);
        g.append("features_count", totalCount);
        Document group = new Document("$group", g);
        Document sort = new Document("$sort", new Document("_id", 1));

//        logger.info("getAllIntervalFrequencies - (>·_·)>");
//        System.out.println(options.toString());
//        System.out.println(match.toString());
//        System.out.println(group.toString());
//        System.out.println(sort.toString());

        long dbTimeStart = System.currentTimeMillis();
        DataResult output = variantsCollection.aggregate(/*"$histogram", */Arrays.asList(match, group, sort), options);
        long dbTimeEnd = System.currentTimeMillis();

        Map<Long, Document> ids = new HashMap<>();
        // Create DBObject for intervals with features inside them
        for (Document intervalObj : (List<Document>) output.getResults()) {
            Long auxId = Math.round((Double) intervalObj.get("_id")); //is double

            Document intervalVisited = ids.get(auxId);
            if (intervalVisited == null) {
                intervalObj.put("_id", auxId);
                intervalObj.put("start", queryParser.getChunkStart(auxId.intValue(), regionIntervalSize));
                intervalObj.put("end", queryParser.getChunkEnd(auxId.intValue(), regionIntervalSize));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", Math.log((int) intervalObj.get("features_count")));
                ids.put(auxId, intervalObj);
            } else {
                Double sum = (Double) intervalVisited.get("features_count") + Math.log((int) intervalObj.get("features_count"));
                intervalVisited.put("features_count", sum.intValue());
            }
        }

        // Create DBObject for intervals without features inside them
        BasicDBList resultList = new BasicDBList();
        int firstChunkId = queryParser.getChunkId(region.getStart(), regionIntervalSize);
        int lastChunkId = queryParser.getChunkId(region.getEnd(), regionIntervalSize);
        Document intervalObj;
        for (int chunkId = firstChunkId; chunkId <= lastChunkId; chunkId++) {
            intervalObj = ids.get((long) chunkId);
            if (intervalObj == null) {
                intervalObj = new Document();
                intervalObj.put("_id", chunkId);
                intervalObj.put("start", queryParser.getChunkStart(chunkId, regionIntervalSize));
                intervalObj.put("end", queryParser.getChunkEnd(chunkId, regionIntervalSize));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", 0);
            }
            resultList.add(intervalObj);
        }

        return new DataResult(((Long) (dbTimeEnd - dbTimeStart)).intValue(), Collections.emptyList(), resultList.size(), resultList,
                resultList.size());
    }

    @Override
    public DataResult rank(Query query, String field, int numResults, boolean asc) {
        QueryOptions options = new QueryOptions();
        options.put("limit", numResults);
        options.put("count", true);
        options.put("order", (asc) ? 1 : -1); // MongoDB: 1 = ascending, -1 = descending

        return groupBy(query, field, options);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        } else {
            options = new QueryOptions(options); // Copy given QueryOptions.
        }

        String documentPath;
        String unwindPath;
        int numUnwinds = 2;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_ENSEMBL_GENE_ID_FIELD;
                unwindPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;

                break;
            case "ct":
            case "consequence_type":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD;
                unwindPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;
                numUnwinds = 3;
                break;
            default:
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_GENE_NAME_FIELD;
                unwindPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;
                break;
        }

        Document mongoQuery = queryParser.parseQuery(query);

        boolean count = options.getBoolean("count", false);
        int order = options.getInt("order", -1);

        Document project;
        Document projectAndCount;
        if (count) {
            project = new Document("$project", new Document("field", "$" + documentPath));
            projectAndCount = new Document("$project", new Document()
                    .append("id", "$_id")
                    .append("_id", 0)
                    .append("count", new Document("$size", "$values")));
        } else {
            project = new Document("$project", new Document()
                    .append("field", "$" + documentPath)
                    //.append("_id._id", "$_id")
                    .append("_id.start", "$" + DocumentToVariantConverter.START_FIELD)
                    .append("_id.end", "$" + DocumentToVariantConverter.END_FIELD)
                    .append("_id.chromosome", "$" + DocumentToVariantConverter.CHROMOSOME_FIELD)
                    .append("_id.alternate", "$" + DocumentToVariantConverter.ALTERNATE_FIELD)
                    .append("_id.reference", "$" + DocumentToVariantConverter.REFERENCE_FIELD)
                    .append("_id.ids", "$" + DocumentToVariantConverter.IDS_FIELD));
            projectAndCount = new Document("$project", new Document()
                    .append("id", "$_id")
                    .append("_id", 0)
                    .append("values", "$values")
                    .append("count", new Document("$size", "$values")));
        }

        Document match = new Document("$match", mongoQuery);
        Document unwindField = new Document("$unwind", "$field");
        Document notNull = new Document("$match", new Document("field", new Document("$ne", null)));
        Document groupAndAddToSet = new Document("$group", new Document("_id", "$field")
                .append("values", new Document("$addToSet", "$_id"))); // sum, count, avg, ...?
        Document sort = new Document("$sort", new Document("count", order)); // 1 = ascending, -1 = descending

        int skip = options.getInt(QueryOptions.SKIP, -1);
        Document skipStep = skip > 0 ? new Document("$skip", skip) : null;

        int limit = options.getInt(QueryOptions.LIMIT, -1) > 0 ? options.getInt(QueryOptions.LIMIT) : 10;
        options.remove(QueryOptions.LIMIT); // Remove limit or Datastore will add a new limit step
        Document limitStep = new Document("$limit", limit);

        List<Bson> operations = new LinkedList<>();
        operations.add(match);
        operations.add(project);
        for (int i = 0; i < numUnwinds; i++) {
            operations.add(unwindField);
        }
        operations.add(notNull);
        operations.add(groupAndAddToSet);
        operations.add(projectAndCount);
        operations.add(sort);
        if (skipStep != null) {
            operations.add(skipStep);
        }
        operations.add(limitStep);
        logger.debug("db." + collectionName + ".aggregate( " + operations + " )");
        DataResult<Document> queryResult = variantsCollection.aggregate(operations, options);

//            List<Map<String, Object>> results = new ArrayList<>(queryResult.getResults().size());
//            results.addAll(queryResult.getResults().stream().map(dbObject -> new ObjectMap("id", dbObject.get("_id")).append("count",
// dbObject.get("count"))).collect(Collectors.toList()));

        return queryResult;
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) {
        String warningMsg = "Unimplemented VariantMongoDBAdaptor::groupBy list of fields. Using field[0] : '" + fields.get(0) + "'";
        logger.warn(warningMsg);
        DataResult queryResult = groupBy(query, fields.get(0), options);
        queryResult.setEvents(Collections.singletonList(new Event(Event.Type.WARNING, warningMsg)));
        return queryResult;
    }

    @Override
    public DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, long timestamp, QueryOptions options) {
        StudyMetadata sm = metadataManager.getStudyMetadata(studyName);
        return updateStats(variantStatsWrappers, sm, timestamp, options);
    }

    @Override
    public DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyMetadata studyMetadata,
                                   long timestamp, QueryOptions options) {
//        MongoCollection<Document> coll = db.getDb().getCollection(collectionName);
//        BulkWriteOperation pullBuilder = coll.initializeUnorderedBulkOperation();
//        BulkWriteOperation pushBuilder = coll.initializeUnorderedBulkOperation();

        List<Bson> pullQueriesBulkList = new LinkedList<>();
        List<Bson> pullUpdatesBulkList = new LinkedList<>();

        List<Bson> pushQueriesBulkList = new LinkedList<>();
        List<Bson> pushUpdatesBulkList = new LinkedList<>();

        long start = System.nanoTime();
        DocumentToVariantStatsConverter statsConverter = new DocumentToVariantStatsConverter(metadataManager);
//        VariantSource variantSource = queryOptions.get(VariantStorageEngine.VARIANT_SOURCE, VariantSource.class);
        DocumentToVariantConverter variantConverter = getDocumentToVariantConverter(new Query(), options);
        boolean overwrite = options.getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(), false);

        // TODO make unset of 'st' if already present?
        for (VariantStatsWrapper wrapper : variantStatsWrappers) {
            List<VariantStats> cohortStats = wrapper.getCohortStats();

            if (cohortStats.isEmpty()) {
                continue;
            }
            List<Document> cohorts = statsConverter.convertCohortsToStorageType(cohortStats, studyMetadata.getId());
            // remove when we remove fileId
//            List cohorts = statsConverter.convertCohortsToStorageType(cohortStats, variantSource.getStudyId());   // TODO use when we
// remove fileId

            // add cohorts, overwriting old values if that cid, fid and sid already exists: remove and then add
            // db.variants.update(
            //      {_id:<id>},
            //      {$pull:{st:{cid:{$in:["Cohort 1","cohort 2"]}, fid:{$in:["file 1", "file 2"]}, sid:{$in:["study 1", "study 2"]}}}}
            // )
            // db.variants.update(
            //      {_id:<id>},
            //      {$push:{st:{$each: [{cid:"Cohort 1", fid:"file 1", ... , defaultValue:3},{cid:"Cohort 2", ... , defaultValue:3}] }}}
            // )

            if (!cohorts.isEmpty()) {
                String id = variantConverter.buildStorageId(new Variant(wrapper.getChromosome(), wrapper.getStart(), wrapper.getEnd(),
                        wrapper.getReference(), wrapper.getAlternate()).setSv(wrapper.getSv()));

                Document find = new Document("_id", id);
                if (overwrite) {
                    List<Document> idsList = new ArrayList<>(cohorts.size());
                    for (Document cohort : cohorts) {
                        Document ids = new Document()
                                .append(DocumentToVariantStatsConverter.COHORT_ID, cohort.get(DocumentToVariantStatsConverter.COHORT_ID))
                                .append(DocumentToVariantStatsConverter.STUDY_ID, cohort.get(DocumentToVariantStatsConverter.STUDY_ID));
                        idsList.add(ids);
                    }
                    Document pull = new Document("$pull",
                            new Document(DocumentToVariantConverter.STATS_FIELD,
                                    new Document("$or", idsList)));
                    pullQueriesBulkList.add(find);
                    pullUpdatesBulkList.add(pull);
                }

                Bson push = combine(pushEach(DocumentToVariantConverter.STATS_FIELD, cohorts), getSetIndexNotSynchronized(timestamp));
                pushQueriesBulkList.add(find);
                pushUpdatesBulkList.add(push);
            }
        }

        // TODO handle if the variant didn't had that studyId in the files array
        // TODO check the substitution is done right if the stats are already present
        if (overwrite) {
            variantsCollection.update(pullQueriesBulkList, pullUpdatesBulkList, new QueryOptions());
        }
        DataResult writeResult = variantsCollection.update(pushQueriesBulkList, pushUpdatesBulkList, new QueryOptions());
        if (writeResult.getNumMatches() != pushQueriesBulkList.size()) {
            logger.warn("Could not update stats from some variants: {} != {}, {} non loaded stats", writeResult.getNumMatches(),
                    pushQueriesBulkList.size(), (pushQueriesBulkList.size() - writeResult.getNumMatches()));
        }

        return writeResult;
    }

    public DataResult removeStats(String studyName, String cohortName, QueryOptions options) {
        StudyMetadata sm = metadataManager.getStudyMetadata(studyName);
        int cohortId = metadataManager.getCohortId(sm.getId(), cohortName);

        // { st : { $elemMatch : {  sid : <studyId>, cid : <cohortId> } } }
        Document query = new Document(DocumentToVariantConverter.STATS_FIELD,
                new Document("$elemMatch",
                        new Document(DocumentToVariantStatsConverter.STUDY_ID, sm.getId())
                                .append(DocumentToVariantStatsConverter.COHORT_ID, cohortId)));

        // { $pull : { st : {  sid : <studyId>, cid : <cohortId> } } }
        Document update = new Document(
                "$pull",
                new Document(DocumentToVariantConverter.STATS_FIELD,
                        new Document(DocumentToVariantStatsConverter.STUDY_ID, sm.getId())
                                .append(DocumentToVariantStatsConverter.COHORT_ID, cohortId)
                )
        );
        logger.debug("deleteStats: query = {}", query);
        logger.debug("deleteStats: update = {}", update);

        return variantsCollection.update(query, update, new QueryOptions(MULTI, true));
    }

    @Override
    public DataResult updateAnnotations(List<VariantAnnotation> variantAnnotations, long timestamp, QueryOptions queryOptions) {
        List<Bson> queries = new LinkedList<>();
        List<Bson> updates = new LinkedList<>();

        StopWatch watch = StopWatch.createStarted();
        DocumentToVariantConverter variantConverter = getDocumentToVariantConverter(new Query(), queryOptions);
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            String id;
            if (variantAnnotation.getAdditionalAttributes() != null
                    && variantAnnotation.getAdditionalAttributes().containsKey(GROUP_NAME.key())) {
                String variantString = variantAnnotation.getAdditionalAttributes()
                        .get(GROUP_NAME.key())
                        .getAttribute()
                        .get(VARIANT_ID.key());
                id = variantConverter.buildStorageId(new Variant(variantString));
            } else {
                id = variantConverter.buildStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                        variantAnnotation.getReference(), variantAnnotation.getAlternate());
            }
            Document find = new Document("_id", id);
            int currentAnnotationId = getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getId();
            DocumentToVariantAnnotationConverter converter = new DocumentToVariantAnnotationConverter(currentAnnotationId);
            Document convertedVariantAnnotation = converter.convertToStorageType(variantAnnotation);
            Bson update = combine(
                    set(DocumentToVariantConverter.ANNOTATION_FIELD + ".0", convertedVariantAnnotation),
                    getSetIndexNotSynchronized(timestamp));
            queries.add(find);
            updates.add(update);
        }
        return variantsCollection.update(queries, updates, null);
    }

    @Override
    public DataResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, long timeStamp,
                                               QueryOptions options) {
        Document queryDocument = queryParser.parseQuery(query);
        Document updateDocument = DocumentToVariantAnnotationConverter.convertToStorageType(attribute);
        return variantsCollection.update(queryDocument,
                combine(set(DocumentToVariantConverter.CUSTOM_ANNOTATION_FIELD + '.' + name, updateDocument),
                        getSetIndexNotSynchronized(timeStamp)),
                new QueryOptions(MULTI, true));
    }

    public DataResult removeAnnotation(String annotationId, Query query, QueryOptions queryOptions) {
        Document mongoQuery = queryParser.parseQuery(query);
        logger.debug("deleteAnnotation: query = {}", mongoQuery);

        Document update = new Document("$set", new Document(DocumentToVariantConverter.ANNOTATION_FIELD + ".0", null));
        logger.debug("deleteAnnotation: update = {}", update);
        return variantsCollection.update(mongoQuery, update, new QueryOptions(MULTI, true));
    }


    @Override
    public void close() throws IOException {
        if (closeConnection) {
            mongoManager.close();
        }
        metadataManager.close();
        NUMBER_INSTANCES.decrementAndGet();
    }

    private DocumentToVariantConverter getDocumentToVariantConverter(Query query, QueryOptions options) {
        return getDocumentToVariantConverter(query, VariantQueryProjectionParser.parseVariantQueryFields(query, options, metadataManager));
    }

    private DocumentToVariantConverter getDocumentToVariantConverter(Query query, VariantQueryProjection selectVariantElements) {
        List<Integer> returnedStudies = selectVariantElements.getStudyIds();
        DocumentToSamplesConverter samplesConverter;
        samplesConverter = new DocumentToSamplesConverter(metadataManager, selectVariantElements);
        samplesConverter.setSampleDataKeys(getIncludeSampleData(query));
        samplesConverter.setIncludeSampleId(query.getBoolean(INCLUDE_SAMPLE_ID.key()));
        if (query.containsKey(UNKNOWN_GENOTYPE.key())) {
            samplesConverter.setUnknownGenotype(query.getString(UNKNOWN_GENOTYPE.key()));
        }

        DocumentToStudyVariantEntryConverter studyEntryConverter;

        studyEntryConverter = new DocumentToStudyVariantEntryConverter(false, selectVariantElements.getFiles(), samplesConverter);
        studyEntryConverter.setMetadataManager(metadataManager);
        ProjectMetadata projectMetadata = getMetadataManager().getProjectMetadata();
        Map<Integer, String> annotationIds;
        if (projectMetadata != null) {
            annotationIds = projectMetadata.getAnnotation().getSaved()
                    .stream()
                    .collect(Collectors.toMap(
                            ProjectMetadata.VariantAnnotationMetadata::getId,
                            ProjectMetadata.VariantAnnotationMetadata::getName));
            ProjectMetadata.VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
            if (current != null) {
                annotationIds.put(current.getId(), current.getName());
            }
        } else {
            annotationIds = Collections.emptyMap();
        }
        return new DocumentToVariantConverter(studyEntryConverter,
                new DocumentToVariantStatsConverter(metadataManager), returnedStudies, annotationIds);
    }

    public void createIndexes(QueryOptions options) {
        createIndexes(options, variantsCollection);
    }

    /**
     * Create missing indexes on the given VariantsCollection.
     * Variant indices
     * - ChunkID
     * - Chromosome + start + end
     * - IDs
     * <p>
     * Study indices
     * - StudyId
     * - FileId
     * <p>
     * Stats indices
     * - StatsMaf
     * - StatsMgf
     * <p>
     * Annotation indices
     * - XRef.id
     * - ConsequenceType.so
     * - _gn_so : SPARSE
     * - PopulationFrequency Study + Population + AlternateFrequency : SPARSE
     * - Clinical.Clinvar.clinicalSignificance  : SPARSE
     * ConservedRegionScore
     * - phastCons.score
     * - phylop.score
     * - gerp.score
     * FunctionalScore
     * - cadd_scaled
     * - cadd_raw
     * - Drugs.name  : SPARSE
     * ProteinSubstitution
     * - polyphen.score : SPARSE
     * - polyphen.description : SPARSE
     * - sift.score : SPARSE
     * - sift.description : SPARSE
     * - ProteinVariantAnnotation.keywords : SPARSE
     * - TranscriptAnnotationFlags : SPARSE
     * SearchIndex
     * - _index.ts
     *
     * @param options            Unused Options.
     * @param variantsCollection MongoDBCollection
     */
    public static void createIndexes(QueryOptions options, MongoDBCollection variantsCollection) {
        logger.info("Start creating indexes");
        ObjectMap onBackground = new ObjectMap(MongoDBCollection.BACKGROUND, true);
        ObjectMap onBackgroundSparse = new ObjectMap(MongoDBCollection.BACKGROUND, true).append(MongoDBCollection.SPARSE, true);

        // Variant indices
        ////////////////
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.AT_FIELD + '.'
                + DocumentToVariantConverter.CHUNK_IDS_FIELD, 1), onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.CHROMOSOME_FIELD, 1)
                .append(DocumentToVariantConverter.START_FIELD, 1)
                .append(DocumentToVariantConverter.END_FIELD, 1), onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.IDS_FIELD, 1), onBackground);

        // Study indices
        ////////////////
        variantsCollection.createIndex(
                new Document(DocumentToVariantConverter.STUDIES_FIELD + '.' + STUDYID_FIELD, 1), onBackground);
        variantsCollection.createIndex(
                new Document(DocumentToVariantConverter.STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1), onBackground);

        // Stats indices
        ////////////////
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.STATS_FIELD + '.' + DocumentToVariantStatsConverter
                .MAF_FIELD, 1), onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.STATS_FIELD + '.' + DocumentToVariantStatsConverter
                .MGF_FIELD, 1), onBackground);

        // Annotation indices
        ////////////////

        // XRefs.id
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD, 1),
                onBackground);
        // ConsequenceType.so
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD, 1),
                onBackground);
        // _gn_so : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.GENE_SO_FIELD, 1),
                onBackgroundSparse);
        // Population frequency : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD, 1)
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD, 1)
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, 1),
                new ObjectMap(onBackgroundSparse).append(NAME, "pop_freq"));
        // Clinical clinvar : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CLINICAL_DATA_FIELD
                                + ".variantClassification.clinicalSignificance", 1),
                new ObjectMap(onBackgroundSparse).append(NAME, "clinical"));

        // Conserved region score (phastCons, phylop, gerp)
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSERVED_REGION_GERP_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSERVED_REGION_PHYLOP_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSERVED_REGION_PHASTCONS_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);

        // Functional score (cadd_scaled, cadd_raw)
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.FUNCTIONAL_CADD_SCALED_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.FUNCTIONAL_CADD_RAW_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);

        // Drugs : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.DRUG_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.DRUG_NAME_FIELD, 1),
                onBackgroundSparse);
        // Protein substitution score (polyphen , sift) : SPARSE
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_PROTEIN_POLYPHEN_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackgroundSparse);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_PROTEIN_SIFT_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackgroundSparse);

        // Protein substitution score description (polyphen , sift) : SPARSE
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_PROTEIN_POLYPHEN_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD, 1),
                onBackgroundSparse);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CT_PROTEIN_SIFT_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD, 1),
                onBackgroundSparse);

        // Protein Keywords : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CT_PROTEIN_KEYWORDS, 1),
                onBackgroundSparse);
        // TranscriptAnnotationFlags : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CT_TRANSCRIPT_ANNOT_FLAGS, 1),
                onBackgroundSparse);

        // _index.ts
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.INDEX_FIELD + '.' + DocumentToVariantConverter.INDEX_TIMESTAMP_FIELD, 1),
                onBackground);

        logger.debug("sent order to create indices");
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
    }

    @Override
    public void setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.metadataManager = variantStorageMetadataManager;
    }

}
