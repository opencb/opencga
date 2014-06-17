package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
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
        MongoDBCollection coll = db.getCollection("files");
        return coll.distinct("studyName");
    }

    @Override
    public QueryResult findStudyNameOrStudyId(String study, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("files");
        QueryBuilder qb = QueryBuilder.start();
        qb.or(new BasicDBObject("studyName", study), new BasicDBObject("studyId", study));
//        parseQueryOptions(options, qb);
        
        DBObject returnFields = new BasicDBObject("studyId", 1).append("_id", 0);
        
        options.add("limit", 1);
        
        return coll.find(qb.get(), returnFields, options);
    }

    @Override
    public QueryResult getStudyById(String studyId, QueryOptions options) {
        // db.variants.aggregate( { $match : { "studyId" : "abc" } }, 
        //                        { $project : { _id : 0, studyId : 1, studyName : 1 } }, 
        //                        { $group : {
        //                              _id : { studyId : "$studyId", studyName : "$studyName"}, 
        //                              numSources : { $sum : 1} 
        //                        }} )
        MongoDBCollection coll = db.getCollection("files");
        
        QueryBuilder qb = QueryBuilder.start();
        getStudyIdFilter(studyId, qb);
        
        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("_id", 0).append("studyId", 1).append("studyName", 1));
        DBObject group = new BasicDBObject("$group", 
                new BasicDBObject("_id", new BasicDBObject("studyId", "$studyId").append("studyName", "$studyName"))
                .append("numFiles", new BasicDBObject("$sum", 1)));
        
        
        QueryResult aggregationResult = coll.aggregate("$studyInfo", Arrays.asList(match, project, group), options);
        Iterable<DBObject> results = aggregationResult.getResult();
        DBObject dbo = results.iterator().next();
        DBObject dboId = (DBObject) dbo.get("_id");
        
        DBObject outputDbo = new BasicDBObject("studyId", dboId.get("studyId")).append("studyName", dboId.get("studyName")).append("numFiles", dbo.get("numFiles"));
        QueryResult transformedResult = new QueryResult(aggregationResult.getId(), aggregationResult.getDBTime(), 
                aggregationResult.getNumResults(), aggregationResult.getNumTotalResults(), 
                aggregationResult.getWarning(), aggregationResult.getError(), 
                DBObject.class, Arrays.asList(outputDbo));
        return transformedResult;
    }

    @Override
    public QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("files");
        QueryBuilder qb = QueryBuilder.start();
        getStudyIdFilter(studyId, qb);
//        parseQueryOptions(options, qb);
        
        return coll.find(qb.get(), options);
    }

    
    private QueryBuilder getStudyFilter(String name, QueryBuilder builder) {
        return builder.and("studyName").is(name);
    }
    
    private QueryBuilder getStudyIdFilter(String id, QueryBuilder builder) {
        return builder.and("studyId").is(id);
    }
    
    
    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

}
