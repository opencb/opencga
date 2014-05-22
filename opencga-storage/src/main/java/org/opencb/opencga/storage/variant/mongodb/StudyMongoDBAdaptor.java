package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.QueryBuilder;
import java.net.UnknownHostException;
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
    public QueryResult getAllSourcesByStudy(String studyName, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("files");
        QueryBuilder qb = QueryBuilder.start();
        getStudyFilter(studyName, qb);
//        parseQueryOptions(options, qb);
        
        return coll.find(qb.get(), options);
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
