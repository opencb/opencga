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

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantSourceSimpleConverter;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantSourceMongoDBAdaptor implements VariantSourceDBAdaptor {

//    private static final Map<String, List> SAMPLES_IN_SOURCES = new HashMap<>();

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final DocumentToVariantSourceSimpleConverter variantSourceConverter;
    private final String collectionName;

    public VariantSourceMongoDBAdaptor(MongoCredentials credentials, String collectionName) throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDBConfiguration mongoDBConfiguration = credentials.getMongoDBConfiguration();
        db = mongoManager.get(credentials.getMongoDbName(), mongoDBConfiguration);
        this.collectionName = collectionName;
        variantSourceConverter = new DocumentToVariantSourceSimpleConverter();
    }

    public VariantSourceMongoDBAdaptor(MongoDataStore db, String collectionName) throws UnknownHostException {
        mongoManager = null;
        this.db = db;
        this.collectionName = collectionName;
        variantSourceConverter = new DocumentToVariantSourceSimpleConverter();
    }

    @Override
    public QueryResult<Long> count() {
        MongoDBCollection coll = db.getCollection(collectionName);
        return coll.count();
    }

    @Override
    public void updateVariantSource(VariantSource variantSource) {
        MongoDBCollection coll = db.getCollection(collectionName);
        Document document = variantSourceConverter.convertToStorageType(variantSource);
        String id = document.getString("_id");
        document.append("_id", id);
        QueryOptions options = new QueryOptions(MongoDBCollection.REPLACE, true).append(MongoDBCollection.UPSERT, true);
        coll.update(Filters.eq("_id", id), document, options);
    }

    @Override
    public Iterator<VariantSource> iterator(Query query, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        Bson filter = parseQuery(query);

        return new Iterator<VariantSource>() {
            private final MongoCursor<Document> iterator = coll.nativeQuery().find(filter, options).iterator();
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public VariantSource next() {
                return variantSourceConverter.convertToDataModelType(iterator.next());
            }
        };
    }


//    @Override
//    public QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options) {
//        MongoDBCollection coll = db.getCollection(collectionName);
//        QueryBuilder qb = QueryBuilder.start();
//        options.put("studyId", studyId);
//        parseQueryOptions(options, qb);
//
//        return coll.find((BasicDBObject) qb.get(), null, variantSourceConverter, options);
//    }
//
//    @Override
//    public QueryResult getAllSourcesByStudyIds(List<String> studyIds, QueryOptions options) {
//        MongoDBCollection coll = db.getCollection(collectionName);
//        QueryBuilder qb = QueryBuilder.start();
////        getStudyIdFilter(studyIds, qb);
//        options.put("studyId", studyIds);
//        parseQueryOptions(options, qb);
//
//        return coll.find((BasicDBObject) qb.get(), null, variantSourceConverter, options);
//    }

//    @Override
//    public QueryResult getSamplesBySource(String fileId, QueryOptions options) {    // TODO jmmut: deprecate when we remove fileId, and
//        // change for getSamplesBySource(String studyId, QueryOptions options)
//        if (SAMPLES_IN_SOURCES.size() != countSources().getResult().get(0)) {
//            synchronized (VariantSourceMongoDBAdaptor.class) {
//                if (SAMPLES_IN_SOURCES.size() != countSources().getResult().get(0)) {
//                    QueryResult queryResult = populateSamplesInSources();
//                    populateSamplesQueryResult(fileId, queryResult);
//                    return queryResult;
//                }
//            }
//        }
//
//        QueryResult queryResult = new QueryResult();
//        populateSamplesQueryResult(fileId, queryResult);
//        return queryResult;
//    }

//    @Override
//    public QueryResult getSamplesBySources(List<String> fileIds, QueryOptions options) {
//        if (SAMPLES_IN_SOURCES.size() != (long) countSources().getResult().get(0)) {
//            synchronized (StudyMongoDBAdaptor.class) {
//                if (SAMPLES_IN_SOURCES.size() != (long) countSources().getResult().get(0)) {
//                    QueryResult queryResult = populateSamplesInSources();
//                    populateSamplesQueryResult(fileIds, queryResult);
//                    return queryResult;
//                }
//            }
//        }
//
//        QueryResult queryResult = new QueryResult();
//        populateSamplesQueryResult(fileIds, queryResult);
//        return queryResult;
//    }

    @Override
    public void delete(int study, int file) {
        String id = DocumentToVariantSourceSimpleConverter.buildId(study, file);
        MongoDBCollection coll = db.getCollection(collectionName);

        DeleteResult deleteResult = coll.remove(Filters.eq("_id", id), null).first();

        if (deleteResult.getDeletedCount() != 1) {
            throw new IllegalArgumentException("Unable to delete VariantSource " + id);
        }

    }

    @Override
    public void close() {
        if (mongoManager != null) {
            mongoManager.close();
        }
    }

    protected Bson parseQuery(Query query) {
        LinkedList<Bson> filters = new LinkedList<>();
        if (query.containsKey(VariantSourceQueryParam.STUDY_ID.key())) {
            List<String> studyIds = query.getAsStringList(VariantSourceQueryParam.STUDY_ID.key());
            filters.add(Filters.in(VariantSourceQueryParam.STUDY_ID.key(), studyIds));
        }
        if (query.containsKey(VariantSourceQueryParam.FILE_ID.key())) {
            List<String> studyIds = query.getAsStringList(VariantSourceQueryParam.FILE_ID.key());
            filters.add(Filters.in(VariantSourceQueryParam.FILE_ID.key(), studyIds));
        }
        if (filters.isEmpty()) {
            return new Document();
        } else {
            return Filters.and(filters);
        }
    }

//    private void parseQueryOptions(QueryOptions options, QueryBuilder builder) {
//
//        if (options.containsKey("studyId")) {
//            andIs(DocumentToVariantSourceConverter.STUDYID_FIELD, options.get("studyId"), builder);
//        }
//        if (options.containsKey("studyName")) {
//            andIs(DocumentToVariantSourceConverter.STUDYNAME_FIELD, options.get("studyId"), builder);
//        }
//        if (options.containsKey("fileId")) {
//            andIs(DocumentToVariantSourceConverter.FILEID_FIELD, options.get("fileId"), builder);
//        }
//        if (options.containsKey("fileName")) {
//            andIs(DocumentToVariantSourceConverter.FILENAME_FIELD, options.get("fileName"), builder);
//        }
//    }
//
//    private QueryBuilder andIs(String fieldName, Object object, QueryBuilder builder) {
//        if (object == null) {
//            return builder;
//        } else if (object instanceof Collection) {
//            return builder.and(fieldName).in(object);
//        } else {
//            return builder.and(fieldName).is(object);
//        }
//    }
//

//    /**
//     * Populates the dictionary relating sources and samples.
//     *
//     * @return The QueryResult with information of how long the query took
//     */
//    private QueryResult populateSamplesInSources() {
//        MongoDBCollection coll = db.getCollection(collectionName);
//        BasicDBObject projection = new BasicDBObject(DocumentToVariantSourceConverter.FILEID_FIELD, true)
//                .append(DocumentToVariantSourceConverter.SAMPLES_FIELD, true);
//        QueryResult queryResult = coll.find((Bson) null, projection, null);
//
//        List<DBObject> result = queryResult.getResult();
//        for (DBObject dbo : result) {
//            if (!dbo.containsField(DocumentToVariantSourceConverter.FILEID_FIELD)) {
//                continue;
//            }
//            String key = dbo.get(DocumentToVariantSourceConverter.FILEID_FIELD).toString();
//            DBObject value = (DBObject) dbo.get(DocumentToVariantSourceConverter.SAMPLES_FIELD);
//            SAMPLES_IN_SOURCES.put(key, new ArrayList(value.toMap().keySet()));
//        }
//
//        return queryResult;
//    }
//
//    private void populateSamplesQueryResult(String fileId, QueryResult queryResult) {
//        List<List> samples = new ArrayList<>(1);
//        List<String> samplesInSource = SAMPLES_IN_SOURCES.get(fileId);
//
//        if (samplesInSource == null || samplesInSource.isEmpty()) {
//            queryResult.setWarningMsg("Source " + fileId + " not found");
//            queryResult.setNumTotalResults(0);
//        } else {
//            samples.add(samplesInSource);
//            queryResult.setResult(samples);
//            queryResult.setNumTotalResults(1);
//        }
//    }
//
//    private void populateSamplesQueryResult(List<String> fileIds, QueryResult queryResult) {
//        List<List> samples = new ArrayList<>(fileIds.size());
//
//        for (String fileId : fileIds) {
//            List<String> samplesInSource = SAMPLES_IN_SOURCES.get(fileId);
//
//            if (samplesInSource == null || samplesInSource.isEmpty()) {
//                // Samples not found
//                samples.add(new ArrayList<>());
//                if (queryResult.getWarningMsg() == null) {
//                    queryResult.setWarningMsg("Source " + fileId + " not found");
//                } else {
//                    queryResult.setWarningMsg(queryResult.getWarningMsg().concat("\nSource " + fileId + " not found"));
//                }
////                queryResult.setNumTotalResults(0);
//            } else {
//                // Add new list of samples
//                samples.add(samplesInSource);
////                queryResult.setNumTotalResults(1);
//            }
//        }
//
//        queryResult.setResult(samples);
//        queryResult.setNumTotalResults(fileIds.size());
//    }


    @Override
    public QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions
            queryOptions) {
        MongoDBCollection coll = db.getCollection(collectionName);

        VariantSource source = new VariantSource(new VariantFileMetadata());
        source.setStats(variantSourceStats.getFileStats());
        Document globalStats = variantSourceConverter.convertToStorageType(source).get("stats", Document.class);

        Bson query = parseQuery(new Query(VariantSourceQueryParam.STUDY_ID.key(), variantSourceStats.getStudyId())
                .append(VariantSourceQueryParam.FILE_ID.key(), variantSourceStats.getFileId()));
        Bson update = Updates.set("stats", globalStats);

        return coll.update(query, update, null);
    }


}
