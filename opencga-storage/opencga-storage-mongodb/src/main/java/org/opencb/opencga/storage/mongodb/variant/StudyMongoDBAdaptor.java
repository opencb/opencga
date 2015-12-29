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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.adaptors.StudyDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class StudyMongoDBAdaptor implements StudyDBAdaptor {

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final String collectionName;

    public StudyMongoDBAdaptor(MongoCredentials credentials, String collectionName) throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        this.collectionName = collectionName;
    }

    @Override
    public QueryResult listStudies() {
//        db.files.aggregate( { $project : { _id : 0, sid : 1, sname : 1 } },
//                    { $group : { _id : { studyId : "$sid", studyName : "$sname"} }},
//                    { $project : { "studyId" : "$_id.studyId", "studyName" : "$_id.studyName", "_id" : 0 }} )
        MongoDBCollection coll = db.getCollection(collectionName);
        DBObject project1 = new BasicDBObject("$project", new BasicDBObject("_id", 0)
                .append(DBObjectToVariantSourceConverter.STUDYID_FIELD, 1)
                .append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, 1));
        DBObject group = new BasicDBObject("$group",
                new BasicDBObject("_id", new BasicDBObject("studyId", "$" + DBObjectToVariantSourceConverter.STUDYID_FIELD)
                        .append("studyName", "$" + DBObjectToVariantSourceConverter.STUDYNAME_FIELD)));
        DBObject project2 = new BasicDBObject("$project", new BasicDBObject("studyId", "$_id.studyId")
                .append("studyName", "$_id.studyName")
                .append("_id", 0));

        return coll.aggregate(/*"$studyList", */Arrays.asList(project1, group, project2), null);
    }

    @Override
    public QueryResult getAllStudies(QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public QueryResult findStudyNameOrStudyId(String study, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        QueryBuilder qb = QueryBuilder.start();
        qb.or(new BasicDBObject(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, study),
                new BasicDBObject(DBObjectToVariantSourceConverter.STUDYID_FIELD, study));
//        parseQueryOptions(options, qb);

        DBObject projection = new BasicDBObject(DBObjectToVariantSourceConverter.STUDYID_FIELD, 1).append("_id", 0);

        options.add("limit", 1);

        return coll.find(qb.get(), projection, options);
    }

    @Override
    public QueryResult getStudyById(String studyId, QueryOptions options) {
        // db.files.aggregate( { $match : { "studyId" : "abc" } },
        //                     { $project : { _id : 0, studyId : 1, studyName : 1 } },
        //                     { $group : {
        //                           _id : { studyId : "$studyId", studyName : "$studyName"},
        //                           numSources : { $sum : 1}
        //                     }} )
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        getStudyIdFilter(studyId, qb);

        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("_id", 0)
                .append(DBObjectToVariantSourceConverter.STUDYID_FIELD, 1)
                .append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, 1));
        DBObject group = new BasicDBObject("$group",
                new BasicDBObject("_id", new BasicDBObject("studyId", "$" + DBObjectToVariantSourceConverter.STUDYID_FIELD)
                        .append("studyName", "$" + DBObjectToVariantSourceConverter.STUDYNAME_FIELD))
                        .append("numFiles", new BasicDBObject("$sum", 1)));


        QueryResult aggregationResult = coll.aggregate(/*"$studyInfo", */Arrays.asList(match, project, group), options);
        Iterable<DBObject> results = aggregationResult.getResult();
        DBObject dbo = results.iterator().next();
        DBObject dboId = (DBObject) dbo.get("_id");

        DBObject outputDbo = new BasicDBObject("studyId", dboId.get("studyId")).append("studyName", dboId.get("studyName"))
                .append("numFiles", dbo.get("numFiles"));
        QueryResult transformedResult = new QueryResult(aggregationResult.getId(), aggregationResult.getDbTime(),
                aggregationResult.getNumResults(), aggregationResult.getNumTotalResults(),
                aggregationResult.getWarningMsg(), aggregationResult.getErrorMsg(), Arrays.asList(outputDbo));
        return transformedResult;
    }

    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    private QueryBuilder getStudyFilter(String name, QueryBuilder builder) {
        return builder.and(DBObjectToVariantSourceConverter.STUDYNAME_FIELD).is(name);
    }

    private QueryBuilder getStudyIdFilter(String id, QueryBuilder builder) {
        return builder.and(DBObjectToVariantSourceConverter.STUDYID_FIELD).is(id);
    }

}
