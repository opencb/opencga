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
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.converters.*;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static org.opencb.commons.datastore.mongodb.MongoDBCollection.MULTI;
import static org.opencb.commons.datastore.mongodb.MongoDBCollection.NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.getIncludeFormats;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.getReturnedFiles;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.getSamplesMetadata;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;

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
    private final VariantSourceMongoDBAdaptor variantSourceMongoDBAdaptor;
    private final StorageConfiguration storageConfiguration;
    private final MongoCredentials credentials;
    private final VariantMongoDBQueryParser queryParser;

    private StudyConfigurationManager studyConfigurationManager;
    private final ObjectMap configuration;
//    private CacheManager cacheManager;

    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBAdaptor.class);

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
    // Number of opened dbAdaptors
    public static final AtomicInteger NUMBER_INSTANCES = new AtomicInteger(0);

    public VariantMongoDBAdaptor(MongoCredentials credentials, String variantsCollectionName, String filesCollectionName,
                                 StudyConfigurationManager studyConfigurationManager, StorageConfiguration storageConfiguration)
            throws UnknownHostException {
        this(new MongoDataStoreManager(credentials.getDataStoreServerAddresses()), credentials, variantsCollectionName, filesCollectionName,
                studyConfigurationManager, storageConfiguration);
        this.closeConnection = true;
    }

    public VariantMongoDBAdaptor(MongoDataStoreManager mongoManager, MongoCredentials credentials,
                                 String variantsCollectionName, String filesCollectionName,
                                 StudyConfigurationManager studyConfigurationManager, StorageConfiguration storageConfiguration)
            throws UnknownHostException {
        // MongoDB configuration
        this.closeConnection = false;
        this.credentials = credentials;
        this.mongoManager = mongoManager;
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        variantSourceMongoDBAdaptor = new VariantSourceMongoDBAdaptor(db, filesCollectionName);
        collectionName = variantsCollectionName;
        variantsCollection = db.getCollection(collectionName);
        this.studyConfigurationManager = studyConfigurationManager;
        this.storageConfiguration = storageConfiguration;
        StorageEngineConfiguration storageEngineConfiguration =
                storageConfiguration.getStorageEngine(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID);
        this.configuration = storageEngineConfiguration == null || storageEngineConfiguration.getVariant().getOptions() == null
                ? new ObjectMap()
                : storageEngineConfiguration.getVariant().getOptions();

        queryParser = new VariantMongoDBQueryParser(studyConfigurationManager);
//        this.cacheManager = new CacheManager(storageConfiguration);
        NUMBER_INSTANCES.incrementAndGet();
    }

    public MongoDBCollection getVariantsCollection() {
        return variantsCollection;
    }

    public MongoDBCollection getStageCollection() {
        return db.getCollection(configuration.getString(COLLECTION_STAGE.key(), COLLECTION_STAGE.defaultValue()));
    }

    public MongoDBCollection getStudiesCollection() {
        return db.getCollection(configuration.getString(COLLECTION_STUDIES.key(), COLLECTION_STUDIES.defaultValue()));
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
     * @return A QueryResult with the number of deleted variants
     */
    public QueryResult remove(Query query, QueryOptions options) {
        Bson mongoQuery = queryParser.parseQuery(query);
        logger.debug("Delete to be executed: '{}'", mongoQuery.toString());
        QueryResult queryResult = variantsCollection.remove(mongoQuery, options);

        return queryResult;
    }

    /**
     * Remove all the given samples belonging to the study from the database.
     *
     * @param studyName   The study name where samples belong to
     * @param sampleNames Sample names to be deleted, these must belong to the study
     * @param options     Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with a list with all the samples deleted
     */
    public QueryResult removeSamples(String studyName, List<String> sampleNames, QueryOptions options) {
        //TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Remove the given file from the database with all the samples it has.
     *
     * @param study     The study where the file belong
     * @param files     The file name to be deleted, it must belong to the study
     * @param options   Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the file deleted
     */
    public QueryResult removeFiles(String study, List<String> files, QueryOptions options) {

        Integer studyId = studyConfigurationManager.getStudyId(study, null, false);
        StudyConfiguration sc = studyConfigurationManager.getStudyConfiguration(studyId, null).first();
        List<Integer> fileIds = studyConfigurationManager.getFileIds(files, false, sc);

        ArrayList<Integer> otherIndexedFiles = new ArrayList<>(sc.getIndexedFiles());
        otherIndexedFiles.removeAll(fileIds);

        // First, remove the study entry that only contains the files to remove
        if (otherIndexedFiles.isEmpty()) {
            // If we are deleting all the files in the study, delete the whole study
            return removeStudy(study, new QueryOptions("purge", true));
        }

        // Remove all the study entries that does not contain any of the other indexed files.
        // This include studies only with the files to remove and with negated fileIds (overlapped files)
        Bson query = elemMatch(DocumentToVariantConverter.STUDIES_FIELD,
                and(
                        eq(STUDYID_FIELD, studyId),
//                            in(FILES_FIELD + '.' + FILEID_FIELD, fileIds),
                        nin(FILES_FIELD + '.' + FILEID_FIELD, otherIndexedFiles)
                )
        );
        removeFilesFromStageCollection(query, studyId, fileIds);

        return removeFilesFromVariantsCollection(query, sc, fileIds);
    }

    private Bson removeFilesFromStageCollection(Bson query, Integer studyId, List<Integer> fileIds) {

        int batchSize = 500;
        FindIterable<Document> findIterable = getVariantsCollection()
                .nativeQuery()
                .find(query, Projections.include("_id"), new QueryOptions())
                .batchSize(batchSize);

        logger.info("Remove files from stage collection - step 1/3"); // Remove study if only contains removed files
        MongoDBCollection stageCollection = getStageCollection();
        int updatedStageDocuments = 0;
        try (MongoCursor<Document> cursor = findIterable.iterator()) {
            List<String> ids = new ArrayList<>(batchSize);
            int i = 0;
            while (cursor.hasNext()) {
                ids.add(cursor.next().getString("_id"));
                Bson updateStage = combine(
                        pull(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString()),
                        unset(studyId.toString()));
                if (ids.size() == batchSize || !cursor.hasNext()) {
                    updatedStageDocuments += stageCollection.update(in("_id", ids), updateStage, new QueryOptions(MULTI, true))
                            .first().getModifiedCount();
                    i++;
                    logger.debug(i + " : clear stage ids = " + ids);
                    ids.clear();
                }
            }
        }

        List<Bson> studyUpdate = new ArrayList<>(fileIds.size());

        logger.info("Remove files from stage collection - step 2/3"); // Other studies
        for (Integer fileId : fileIds) {
            studyUpdate.add(unset(studyId + "." + fileId));
        }
        updatedStageDocuments += stageCollection.update(eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString()),
                combine(studyUpdate), new QueryOptions(MULTI, true)).first().getModifiedCount();

        logger.info("Remove files from stage collection - step 3/3"); // purge
        long removedStageDocuments = removeEmptyVariantsFromStage();

        logger.info("Updated " + updatedStageDocuments + " documents from stage");
        logger.info("Removed " + removedStageDocuments + " documents from stage");
        return query;
    }

    private QueryResult<UpdateResult> removeFilesFromVariantsCollection(Bson query, StudyConfiguration sc, List<Integer> fileIds) {

        Set<Integer> sampleIds = fileIds.stream()
                .map(sc.getSamplesInFiles()::get)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        // Update and remove variants from variants collection
        int studyId = sc.getStudyId();
        logger.info("Remove files from variants collection - step 1/3"); // Remove study if only contains removed files
        long updatedVariantsDocuments = removeStudy(studyId, query).first().getModifiedCount();


        // Remove also negated fileIds
        List<Integer> negatedFileIds = fileIds.stream().map(i -> -i).collect(Collectors.toList());
        fileIds.addAll(negatedFileIds);

        // If default genotype is not the unknown genotype, we must iterate over all the documents in the study
        if (!sc.getAttributes().getString(DEFAULT_GENOTYPE.key()).equals(DocumentToSamplesConverter.UNKNOWN_GENOTYPE)) {
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
        for (String gt : sc.getAttributes().getAsStringList(LOADED_GENOTYPES.key())) {
            updates.add(
                    pullByFilter(
                            in(DocumentToVariantConverter.STUDIES_FIELD + ".$." + GENOTYPES_FIELD + '.' + gt, sampleIds)));
        }

        Bson update = combine(updates);
        logger.debug("removeFile: query = " + query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.debug("removeFile: update = " + update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        logger.info("Remove files from variants collection - step 2/3"); // Other studies
        QueryResult<UpdateResult> result2 = getVariantsCollection().update(query, update, new QueryOptions(MULTI, true));

        logger.debug("removeFile: matched  = " + result2.first().getMatchedCount());
        logger.debug("removeFile: modified = " + result2.first().getModifiedCount());


        logger.info("Remove files from variants collection - step 3/3"); // purge
        long removedVariantsDocuments = removeEmptyVariants();

        logger.info("Updated " + (updatedVariantsDocuments + result2.first().getModifiedCount()) + " documents from variants");
        logger.info("Removed " + removedVariantsDocuments + " documents from variants");
        return result2;
    }

    /**
     * Remove the given study from the database.
     *
     * @param studyName The study name to delete
     * @param options   Query modifiers, accepted values are: purge
     * @return A QueryResult with the study deleted
     */
    public QueryResult removeStudy(String studyName, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        Integer studyId = studyConfigurationManager.getStudyId(studyName, null, false);
        Bson query = queryParser.parseQuery(new Query(STUDIES.key(), studyId));

        boolean purge = options.getBoolean("purge", true);

        QueryResult<UpdateResult> result = removeStudy(studyId, query);

        if (purge) {
            removeEmptyVariants();
        }

        Bson eq = eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString());
        Bson combine = combine(pull(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyId.toString()), unset(studyId.toString()));
        logger.debug("removeStudy: stage query = " + eq.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.debug("removeStudy: stage update = " + combine.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        getStageCollection().update(eq, combine, new QueryOptions(MULTI, true));

        if (purge) {
            removeEmptyVariantsFromStage();
        }
        return result;
    }

    private QueryResult<UpdateResult> removeStudy(int studyId, Bson query) {
        // { $pull : { files : {  sid : <studyId> } } }
        Bson update = combine(
                pull(DocumentToVariantConverter.STUDIES_FIELD, eq(STUDYID_FIELD, studyId)),
                pull(DocumentToVariantConverter.STATS_FIELD, eq(DocumentToVariantStatsConverter.STUDY_ID, studyId))
        );
        logger.debug("removeStudy: query = {}", query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        logger.debug("removeStudy: update = {}", update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        QueryResult<UpdateResult> result = variantsCollection.update(query, update, new QueryOptions(MULTI, true));

        logger.debug("removeStudy: matched  = {}", result.first().getMatchedCount());
        logger.debug("removeStudy: modified = {}", result.first().getModifiedCount());

        return result;
    }

    private long removeEmptyVariants() {
        Bson purgeQuery = exists(DocumentToVariantConverter.STUDIES_FIELD + '.' + STUDYID_FIELD, false);
        return variantsCollection.remove(purgeQuery, new QueryOptions(MULTI, true)).first().getDeletedCount();
    }

    private long removeEmptyVariantsFromStage() {
        Bson purgeQuery = eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, Collections.emptyList());
        return getStageCollection().remove(purgeQuery, new QueryOptions(MULTI, true)).first().getDeletedCount();
    }

    @Override
    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {

        if (options == null) {
            options = new QueryOptions();
        }

//        logger.info("******************** Summary => " + options.getBoolean("summary"));

//
//        VariantQueryResult<Variant> queryResult;
//
//        if (options.getBoolean("cache") && cacheManager.isTypeAllowed("var")) {
//            List<Integer> studyIds = studyConfigurationManager.getStudyIds(query.getAsList(STUDIES.key()), options);
//            // TODO : ONLY USING ONE STUDY ID ?
//            String key = cacheManager.createKey(studyIds.get(0).toString(), "var", query, options);
//            queryResult = new VariantQueryResult<>(cacheManager.get(key), null);
//            if (queryResult.getResult() == null || queryResult.getResult().size() == 0) {
//                queryResult = getVariantQueryResult(query, options);
//                cacheManager.set(key, query, queryResult);
//            }
//        } else {
//            if (options.getBoolean("summary", false) && storageConfiguration.getSearch().getActive()
//                    && variantSearchManager != null && variantSearchManager.isAlive(credentials.getMongoDbName())) {
//                try {
//                    queryResult = variantSearchManager.query(credentials.getMongoDbName(), query, options);
//                } catch (IOException | VariantSearchException e) {
//                    throw Throwables.propagate(e);
//                }
//            } else {
//                queryResult = getVariantQueryResult(query, options);
//            }
//        }
//        return queryResult;
//    }
//
//    private VariantQueryResult<Variant> getVariantQueryResult(Query query, QueryOptions options) {
        Document mongoQuery = queryParser.parseQuery(query);
        Document projection = queryParser.createProjection(query, options);
//        logger.debug("Query to be executed: '{}'", mongoQuery.toJson(new JsonWriterSettings(JsonMode.SHELL, false)));
        options.putIfAbsent(QueryOptions.SKIP_COUNT, true);

//        int defaultTimeout = configuration.getInt(DEFAULT_TIMEOUT.key(), DEFAULT_TIMEOUT.defaultValue());
//        int maxTimeout = configuration.getInt(MAX_TIMEOUT.key(), MAX_TIMEOUT.defaultValue());
//        int timeout = options.getInt(QueryOptions.TIMEOUT, defaultTimeout);
//        if (timeout > maxTimeout || timeout < 0) {
//            timeout = maxTimeout;
//        }
//        options.put(QueryOptions.TIMEOUT, timeout);

        // FIXME: MONGO_MIGRATION
//        if (options.getBoolean("mongodb.explain", false)) {
//            FindIterable<Document> dbCursor = variantsCollection.nativeQuery().find(mongoQuery, projection, options);
//            DBObject explain = dbCursor.explain();
//            try {
//                System.err.println("mongodb.explain = "
//                        + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(explain));
//            } catch (JsonProcessingException ignore) {
//                System.err.println("mongodb.explain = " + explain);
//            }
//        }
        DocumentToVariantConverter converter = getDocumentToVariantConverter(query, options);
        Map<String, List<String>> samples = getSamplesMetadata(query, options, studyConfigurationManager);
        return new VariantQueryResult<>(variantsCollection.find(mongoQuery, projection, converter, options), samples);
    }

    @Override
    public List<VariantQueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        List<VariantQueryResult<Variant>> queryResultList = new ArrayList<>(queries.size());
        for (Query query : queries) {
            VariantQueryResult<Variant> queryResult = get(query, options);
            queryResultList.add(queryResult);
        }
        return queryResultList;
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String varStr, String studyName, String sampleName, QueryOptions options,
                                                 int windowsSize) {
        StopWatch watch = new StopWatch();
        watch.start();

        Variant variant = new Variant(varStr);
        Region region = new Region(variant.getChromosome(), variant.getStart(), variant.getEnd());
        Query query = new Query(REGION.key(), region)
                .append(REFERENCE.key(), variant.getReference())
                .append(ALTERNATE.key(), variant.getAlternate())
                .append(STUDIES.key(), studyName)
                .append(RETURNED_STUDIES.key(), studyName)
                .append(RETURNED_SAMPLES.key(), sampleName);
        VariantQueryResult<Variant> queryResult = get(query, new QueryOptions());
        variant = queryResult.first();
        if (variant != null && !variant.getStudies().isEmpty()) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            Integer psIdx = studyEntry.getFormatPositions().get(VCFConstants.PHASE_SET_KEY);
            if (psIdx != null) {
                String ps = studyEntry.getSamplesData().get(0).get(psIdx);
                if (!ps.equals(DocumentToSamplesConverter.UNKNOWN_FIELD)) {
                    sampleName = studyEntry.getOrderedSamplesName().get(0);

                    region.setStart(region.getStart() > windowsSize ? region.getStart() - windowsSize : 0);
                    region.setEnd(region.getEnd() + windowsSize);
                    query.remove(REFERENCE.key());
                    query.remove(ALTERNATE.key());
                    query.remove(RETURNED_STUDIES.key());
                    query.remove(RETURNED_SAMPLES.key());
                    queryResult = get(query, new QueryOptions(QueryOptions.SORT, true));
                    Iterator<Variant> iterator = queryResult.getResult().iterator();
                    while (iterator.hasNext()) {
                        Variant next = iterator.next();
                        if (!next.getStudies().isEmpty()) {
                            if (!ps.equals(next.getStudies().get(0).getSampleData(sampleName, VCFConstants.PHASE_SET_KEY))) {
                                iterator.remove();
                            }
                        }
                    }
                    queryResult.setNumResults(queryResult.getResult().size());
                    queryResult.setNumTotalResults(queryResult.getResult().size());
                    watch.stop();
                    queryResult.setDbTime(((int) watch.getTime()));
                    queryResult.setId("getPhased");
                    queryResult.setSamples(getSamplesMetadata(query, options, studyConfigurationManager));
                    return queryResult;
                }
            }
        }
        watch.stop();
        return new VariantQueryResult<>("getPhased", ((int) watch.getTime()), 0, 0, null, null, Collections.emptyList(), null);
    }

    @Override
    public QueryResult<Long> count(Query query) {
        Document mongoQuery = queryParser.parseQuery(query);
        return variantsCollection.count(mongoQuery);
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        String documentPath;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_ENSEMBL_GENE_ID_FIELD;
                break;
            case "ensemblTranscript":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_ENSEMBL_TRANSCRIPT_ID_FIELD;
                break;
            case "ct":
            case "consequence_type":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD;
                break;
            default:
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_GENE_NAME_FIELD;
                break;
        }
        Document mongoQuery = queryParser.parseQuery(query);
        return variantsCollection.distinct(documentPath, mongoQuery);
    }

    @Override
    public VariantDBIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

//        if (options.getBoolean("summary", false) && storageConfiguration.getSearch().getActive()
//                && variantSearchManager != null && variantSearchManager.isAlive(credentials.getMongoDbName())) {
//            // Solr iterator
//            try {
//                return variantSearchManager.iterator(credentials.getMongoDbName(), query, options);
//            } catch (VariantSearchException | IOException e) {
//                e.printStackTrace();
//            }
//            //throw new UnsupportedOperationException("Summary option (i.e., Solr search) not implemented yet!!");
//        } else {
        Document mongoQuery = queryParser.parseQuery(query);
        Document projection = queryParser.createProjection(query, options);
        DocumentToVariantConverter converter = getDocumentToVariantConverter(query, options);
        options.putIfAbsent(MongoDBCollection.BATCH_SIZE, 100);

        // Short unsorted queries with timeout or limit don't need the persistent cursor.
        if (options.containsKey(QueryOptions.TIMEOUT)
                || options.containsKey(QueryOptions.LIMIT)
                || !options.getBoolean(QueryOptions.SORT, false)) {
            StopWatch stopWatch = StopWatch.createStarted();
            FindIterable<Document> dbCursor = variantsCollection.nativeQuery().find(mongoQuery, projection, options);
            VariantMongoDBIterator dbIterator = new VariantMongoDBIterator(dbCursor, converter);
            dbIterator.setTimeFetching(dbIterator.getTimeFetching() + stopWatch.getNanoTime());
            return dbIterator;
        } else {
            logger.debug("Using mongodb persistent iterator");
            return VariantMongoDBIterator.persistentIterator(variantsCollection, mongoQuery, projection, options, converter);
        }
    }

    @Override
    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) {
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
        QueryResult output = variantsCollection.aggregate(/*"$histogram", */Arrays.asList(match, group, sort), options);
        long dbTimeEnd = System.currentTimeMillis();

        Map<Long, Document> ids = new HashMap<>();
        // Create DBObject for intervals with features inside them
        for (Document intervalObj : (List<Document>) output.getResult()) {
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

        QueryResult queryResult = new QueryResult(region.toString(), ((Long) (dbTimeEnd - dbTimeStart)).intValue(),
                resultList.size(), resultList.size(), null, null, resultList);
        return queryResult;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        QueryOptions options = new QueryOptions();
        options.put("limit", numResults);
        options.put("count", true);
        options.put("order", (asc) ? 1 : -1); // MongoDB: 1 = ascending, -1 = descending

        return groupBy(query, field, options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        String documentPath;
        String unwindPath;
        int numUnwinds = 2;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_ENSEMBL_GENE_ID_FIELD;
                unwindPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;

                break;
            case "ct":
            case "consequence_type":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD;
                unwindPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;
                numUnwinds = 3;
                break;
            default:
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_GENE_NAME_FIELD;
                unwindPath = DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;
                break;
        }

        Document mongoQuery = queryParser.parseQuery(query);

        if (options == null) {
            options = new QueryOptions();
        } else {
            options = new QueryOptions(options); // Copy given QueryOptions.
        }
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
        QueryResult<Document> queryResult = variantsCollection.aggregate(operations, options);

//            List<Map<String, Object>> results = new ArrayList<>(queryResult.getResult().size());
//            results.addAll(queryResult.getResult().stream().map(dbObject -> new ObjectMap("id", dbObject.get("_id")).append("count",
// dbObject.get("count"))).collect(Collectors.toList()));

        return queryResult;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        String warningMsg = "Unimplemented VariantMongoDBAdaptor::groupBy list of fields. Using field[0] : '" + fields.get(0) + "'";
        logger.warn(warningMsg);
        QueryResult queryResult = groupBy(query, fields.get(0), options);
        queryResult.setWarningMsg(warningMsg);
        return queryResult;
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions options) {
        return updateStats(variantStatsWrappers, studyConfigurationManager.getStudyConfiguration(studyName, options).first(), options);
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration,
                                   QueryOptions options) {
//        MongoCollection<Document> coll = db.getDb().getCollection(collectionName);
//        BulkWriteOperation pullBuilder = coll.initializeUnorderedBulkOperation();
//        BulkWriteOperation pushBuilder = coll.initializeUnorderedBulkOperation();

        List<Bson> pullQueriesBulkList = new LinkedList<>();
        List<Bson> pullUpdatesBulkList = new LinkedList<>();

        List<Bson> pushQueriesBulkList = new LinkedList<>();
        List<Bson> pushUpdatesBulkList = new LinkedList<>();

        long start = System.nanoTime();
        DocumentToVariantStatsConverter statsConverter = new DocumentToVariantStatsConverter(studyConfigurationManager);
//        VariantSource variantSource = queryOptions.get(VariantStorageEngine.VARIANT_SOURCE, VariantSource.class);
        DocumentToVariantConverter variantConverter = getDocumentToVariantConverter(new Query(), options);
        boolean overwrite = options.getBoolean(VariantStorageEngine.Options.OVERWRITE_STATS.key(), false);
        //TODO: Use the StudyConfiguration to change names to ids

        // TODO make unset of 'st' if already present?
        for (VariantStatsWrapper wrapper : variantStatsWrappers) {
            Map<String, VariantStats> cohortStats = wrapper.getCohortStats();
            Iterator<VariantStats> iterator = cohortStats.values().iterator();
            VariantStats variantStats = iterator.hasNext() ? iterator.next() : null;
            List<Document> cohorts = statsConverter.convertCohortsToStorageType(cohortStats, studyConfiguration.getStudyId());   // TODO
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
                String id = variantConverter.buildStorageId(wrapper.getChromosome(), wrapper.getPosition(),
                        variantStats.getRefAllele(), variantStats.getAltAllele());


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

                Document push = new Document("$push",
                        new Document(DocumentToVariantConverter.STATS_FIELD,
                                new Document("$each", cohorts)));
                pushQueriesBulkList.add(find);
                pushUpdatesBulkList.add(push);
            }
        }

        // TODO handle if the variant didn't had that studyId in the files array
        // TODO check the substitution is done right if the stats are already present
        if (overwrite) {
            variantsCollection.update(pullQueriesBulkList, pullUpdatesBulkList, new QueryOptions());
        }
        BulkWriteResult writeResult = variantsCollection.update(pushQueriesBulkList, pushUpdatesBulkList, new QueryOptions()).first();
        int writes = writeResult.getModifiedCount();


        return new QueryResult<>("", ((int) (System.nanoTime() - start)), writes, writes, "", "", Collections.singletonList(writeResult));
    }

    public QueryResult removeStats(String studyName, String cohortName, QueryOptions options) {
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyName, options).first();
        int cohortId = studyConfiguration.getCohortIds().get(cohortName);

        // { st : { $elemMatch : {  sid : <studyId>, cid : <cohortId> } } }
        Document query = new Document(DocumentToVariantConverter.STATS_FIELD,
                new Document("$elemMatch",
                        new Document(DocumentToVariantStatsConverter.STUDY_ID, studyConfiguration.getStudyId())
                                .append(DocumentToVariantStatsConverter.COHORT_ID, cohortId)));

        // { $pull : { st : {  sid : <studyId>, cid : <cohortId> } } }
        Document update = new Document(
                "$pull",
                new Document(DocumentToVariantConverter.STATS_FIELD,
                        new Document(DocumentToVariantStatsConverter.STUDY_ID, studyConfiguration.getStudyId())
                                .append(DocumentToVariantStatsConverter.COHORT_ID, cohortId)
                )
        );
        logger.debug("deleteStats: query = {}", query);
        logger.debug("deleteStats: update = {}", update);

        return variantsCollection.update(query, update, new QueryOptions(MULTI, true));
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {

        List<Bson> queries = new LinkedList<>();
        List<Bson> updates = new LinkedList<>();

        long start = System.nanoTime();
        DocumentToVariantConverter variantConverter = getDocumentToVariantConverter(new Query(), queryOptions);
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            String id = variantConverter.buildStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                    variantAnnotation.getReference(), variantAnnotation.getAlternate());
            Document find = new Document("_id", id);
            DocumentToVariantAnnotationConverter converter = new DocumentToVariantAnnotationConverter();
            Document convertedVariantAnnotation = converter.convertToStorageType(variantAnnotation);
            Document update = new Document("$set", new Document(DocumentToVariantConverter.ANNOTATION_FIELD + ".0",
                    convertedVariantAnnotation));
            queries.add(find);
            updates.add(update);
        }
        BulkWriteResult writeResult = variantsCollection.update(queries, updates, null).first();

        return new QueryResult<>("", ((int) (System.nanoTime() - start)), 1, 1, "", "", Collections.singletonList(writeResult));
    }

    @Override
    public QueryResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, QueryOptions options) {
        Document queryDocument = queryParser.parseQuery(query);
        Document updateDocument = DocumentToVariantAnnotationConverter.convertToStorageType(attribute);
        return variantsCollection.update(queryDocument,
                set(DocumentToVariantConverter.CUSTOM_ANNOTATION_FIELD + "." + name, updateDocument),
                new QueryOptions(MULTI, true));
    }

    public QueryResult removeAnnotation(String annotationId, Query query, QueryOptions queryOptions) {
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
        studyConfigurationManager.close();
        NUMBER_INSTANCES.decrementAndGet();
    }

    private DocumentToVariantConverter getDocumentToVariantConverter(Query query, QueryOptions options) {
        List<Integer> returnedStudies = getReturnedStudies(query, options);
        DocumentToSamplesConverter samplesConverter;
        samplesConverter = new DocumentToSamplesConverter(studyConfigurationManager);
        samplesConverter.setFormat(getIncludeFormats(query));
        // Fetch some StudyConfigurations that will be needed
        if (returnedStudies != null) {
            for (Integer studyId : returnedStudies) {
                QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, options);
                if (queryResult.getResult().isEmpty()) {
                    throw VariantQueryException.studyNotFound(studyId);
//                    throw new IllegalArgumentException("Couldn't find studyConfiguration for StudyId '" + studyId + "'");
                } else {
                    samplesConverter.addStudyConfiguration(queryResult.first());
                }
            }
        }
        if (query.containsKey(UNKNOWN_GENOTYPE.key())) {
            samplesConverter.setReturnedUnknownGenotype(query.getString(UNKNOWN_GENOTYPE.key()));
        }

        Set<VariantField> fields = VariantField.getReturnedFields(options);
        samplesConverter.setReturnedSamples(getReturnedSamples(query, options));

        DocumentToStudyVariantEntryConverter studyEntryConverter;
        Collection<Integer> returnedFiles = getReturnedFiles(query, options, fields, studyConfigurationManager);

        studyEntryConverter = new DocumentToStudyVariantEntryConverter(false, returnedFiles, samplesConverter);
        studyEntryConverter.setStudyConfigurationManager(studyConfigurationManager);
        return new DocumentToVariantConverter(studyEntryConverter,
                new DocumentToVariantStatsConverter(studyConfigurationManager), returnedStudies);
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
     * - StudyId + FileId
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
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.STUDIES_FIELD
                + "." + STUDYID_FIELD, 1)
                .append(DocumentToVariantConverter.STUDIES_FIELD
                        + "." + FILES_FIELD
                        + "." + FILEID_FIELD, 1), onBackground);
        // Stats indices
        ////////////////
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter
                .MAF_FIELD, 1), onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter
                .MGF_FIELD, 1), onBackground);

        // Annotation indices
        ////////////////

        // XRefs.id
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                + "." + DocumentToVariantAnnotationConverter.XREF_ID_FIELD, 1),
                onBackground);
        // ConsequenceType.so
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD, 1),
                onBackground);
        // _gn_so : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.GENE_SO_FIELD, 1),
                onBackgroundSparse);
        // Population frequency : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD, 1)
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD, 1)
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, 1),
                new ObjectMap(onBackgroundSparse).append(NAME, "pop_freq"));
        // Clinical clinvar : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CLINICAL_DATA_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CLINICAL_CLINVAR_FIELD
                                + ".clinicalSignificance", 1),
                new ObjectMap(onBackgroundSparse).append(NAME, "clinvar"));

        // Conserved region score (phastCons, phylop, gerp)
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSERVED_REGION_GERP_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSERVED_REGION_PHYLOP_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSERVED_REGION_PHASTCONS_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);

        // Functional score (cadd_scaled, cadd_raw)
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.FUNCTIONAL_CADD_SCALED_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.FUNCTIONAL_CADD_RAW_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackground);

        // Drugs : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.DRUG_FIELD
                                + "." + DocumentToVariantAnnotationConverter.DRUG_NAME_FIELD, 1),
                onBackgroundSparse);
        // Protein substitution score (polyphen , sift) : SPARSE
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_POLYPHEN_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackgroundSparse);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_SIFT_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, 1),
                onBackgroundSparse);

        // Protein substitution score description (polyphen , sift) : SPARSE
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_POLYPHEN_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD, 1),
                onBackgroundSparse);
        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_SIFT_FIELD
                        + "." + DocumentToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD, 1),
                onBackgroundSparse);

        // Protein Keywords : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_KEYWORDS, 1),
                onBackgroundSparse);
        // TranscriptAnnotationFlags : SPARSE
        variantsCollection.createIndex(new Document()
                        .append(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + "." + DocumentToVariantAnnotationConverter.CT_TRANSCRIPT_ANNOT_FLAGS, 1),
                onBackgroundSparse);

        logger.debug("sent order to create indices");
    }

    @Override
    public StudyConfigurationManager getStudyConfigurationManager() {
        return studyConfigurationManager;
    }

    @Override
    public VariantSourceMongoDBAdaptor getVariantSourceDBAdaptor() {
        return variantSourceMongoDBAdaptor;
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public static List<Integer> getLoadedSamples(int fileId, StudyConfiguration studyConfiguration) {
        List<Integer> loadedSampleIds = new LinkedList<>();
        for (Integer indexedFile : studyConfiguration.getIndexedFiles()) {
            if (indexedFile.equals(fileId)) {
                continue;
            } else {
                loadedSampleIds.addAll(studyConfiguration.getSamplesInFiles().get(indexedFile));
            }
        }
        loadedSampleIds.removeAll(studyConfiguration.getSamplesInFiles().get(fileId));
        return loadedSampleIds;
    }

}
