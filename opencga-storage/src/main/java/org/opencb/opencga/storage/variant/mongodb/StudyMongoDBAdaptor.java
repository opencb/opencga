package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import java.net.UnknownHostException;
import java.util.Arrays;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.StudyDBAdaptor;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class StudyMongoDBAdaptor implements StudyDBAdaptor {

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;

    public StudyMongoDBAdaptor(MongoCredentials credentials) throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().add("username", "biouser").add("password", "biopass").build();
        db = mongoManager.get(credentials.getMongoDbName(), mongoDBConfiguration);
    }

    @Override
    public QueryResult listStudies() {
//        db.files.aggregate( { $project : { _id : 0, sid : 1, sname : 1 } },
//                    { $group : { _id : { studyId : "$sid", studyName : "$sname"} }}, 
//                    { $project : { "studyId" : "$_id.studyId", "studyName" : "$_id.studyName", "_id" : 0 }} )
        MongoDBCollection coll = db.getCollection("files");
        DBObject project1 = new BasicDBObject("$project", new BasicDBObject("_id", 0)
                .append(DBObjectToVariantSourceConverter.STUDYID_FIELD, 1)
                .append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, 1));
        DBObject group = new BasicDBObject("$group", 
                new BasicDBObject("_id", new BasicDBObject("studyId", "$" + DBObjectToVariantSourceConverter.STUDYID_FIELD)
                        .append("studyName", "$" + DBObjectToVariantSourceConverter.STUDYNAME_FIELD)));
        DBObject project2 = new BasicDBObject("$project", new BasicDBObject("studyId", "$_id.studyId")
                .append("studyName", "$_id.studyName")
                .append("_id", 0));
        
        return coll.aggregate("$studyList", Arrays.asList(project1, group, project2), null);
    }

    @Override
    public QueryResult findStudyNameOrStudyId(String study, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("files");
        QueryBuilder qb = QueryBuilder.start();
        qb.or(new BasicDBObject(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, study), new BasicDBObject(DBObjectToVariantSourceConverter.STUDYID_FIELD, study));
//        parseQueryOptions(options, qb);
        
        DBObject returnFields = new BasicDBObject(DBObjectToVariantSourceConverter.STUDYID_FIELD, 1).append("_id", 0);
        
        options.add("limit", 1);
        
        return coll.find(qb.get(), options, null, returnFields);
    }

    @Override
    public QueryResult getStudyById(String studyId, QueryOptions options) {
        // db.files.aggregate( { $match : { "studyId" : "abc" } }, 
        //                     { $project : { _id : 0, studyId : 1, studyName : 1 } }, 
        //                     { $group : {
        //                           _id : { studyId : "$studyId", studyName : "$studyName"}, 
        //                           numSources : { $sum : 1} 
        //                     }} )
        MongoDBCollection coll = db.getCollection("files");
        
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
        
        
        QueryResult aggregationResult = coll.aggregate("$studyInfo", Arrays.asList(match, project, group), options);
        Iterable<DBObject> results = aggregationResult.getResult();
        DBObject dbo = results.iterator().next();
        DBObject dboId = (DBObject) dbo.get("_id");
        
        DBObject outputDbo = new BasicDBObject("studyId", dboId.get("studyId")).append("studyName", dboId.get("studyName")).append("numFiles", dbo.get("numFiles"));
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
