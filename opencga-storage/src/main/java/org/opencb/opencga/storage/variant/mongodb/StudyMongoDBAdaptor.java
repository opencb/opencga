package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static Map<String, List> samplesInSources = new HashMap<>();
    
    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final DBObjectToVariantSourceConverter variantSourceConverter;

    
    public StudyMongoDBAdaptor(MongoCredentials credentials) throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().add("username", "biouser").add("password", "biopass").build();
        db = mongoManager.get(credentials.getMongoDbName(), mongoDBConfiguration);
        variantSourceConverter = new DBObjectToVariantSourceConverter();
    }

    @Override
    public QueryResult listStudies() {
        MongoDBCollection coll = db.getCollection("files");
        return coll.distinct("studyName", null);
    }

    @Override
    public QueryResult getNumberOfSources() {
        MongoDBCollection coll = db.getCollection("files");
        return coll.count();
    }
    
    @Override
    public QueryResult getStudyNameById(String studyId, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("files");
        QueryBuilder qb = QueryBuilder.start();
        qb.or(new BasicDBObject("studyName", studyId), new BasicDBObject("studyId", studyId));
//        parseQueryOptions(options, qb);
        
        BasicDBObject returnFields = new BasicDBObject("studyId", 1).append("_id", 0);
        
        options.add("limit", 1);
        
        return coll.find(qb.get(), options, null, returnFields);
    }

    @Override
    public QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("files");
        QueryBuilder qb = QueryBuilder.start();
        getStudyIdFilter(studyId, qb);
//        parseQueryOptions(options, qb);
        
        return coll.find(qb.get(), options, variantSourceConverter);
    }

    @Override
    public QueryResult getSamplesBySource(String fileId, String studyId, QueryOptions options) {
        if (samplesInSources.size() != (long) getNumberOfSources().getResult().get(0)) {
            synchronized (StudyMongoDBAdaptor.class) {
                if (samplesInSources.size() != (long) getNumberOfSources().getResult().get(0)) {
                    QueryResult queryResult = populateSamplesInSources();
                    populateSamplesInSourcesQueryResult(fileId, studyId, queryResult);
                    return queryResult;
                }
            }
        } 
        
        QueryResult queryResult = new QueryResult();
        populateSamplesInSourcesQueryResult(fileId, studyId, queryResult);
        return queryResult;
    }
    
    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    
    private QueryBuilder getStudyFilter(String name, QueryBuilder builder) {
        return builder.and("studyName").is(name);
    }
    
    private QueryBuilder getStudyIdFilter(String id, QueryBuilder builder) {
        return builder.and("studyId").is(id);
    }
    
    /**
     * Populates the dictionary relating sources and samples. 
     * 
     * @return The QueryResult with information of how long the query took
     */
    private QueryResult populateSamplesInSources() {
        MongoDBCollection coll = db.getCollection("files");
        DBObject returnFields = new BasicDBObject("fileId", true).append("studyId", true).append("samples", true);
        QueryResult queryResult = coll.find(null, null, null, returnFields);
        
        List<DBObject> result = queryResult.getResult();
        for (DBObject dbo : result) {
            String key = dbo.get("studyId").toString() + "_" + dbo.get("fileId").toString();
            List value = (List) dbo.get("samples");
            samplesInSources.put(key, value);
        }
        
        return queryResult;
    }
    
    private void populateSamplesInSourcesQueryResult(String fileId, String studyId, QueryResult queryResult) {
        List<List> samples = new ArrayList<>(1);
        samples.add(samplesInSources.get("studyId" + "_" + "fileId"));
        queryResult.setResult(samples);

        if (samples.isEmpty()) {
            queryResult.setWarning("Source " + fileId + " in study " + studyId + " not found");
            queryResult.setNumTotalResults(0);
        } else {
            queryResult.setNumTotalResults(1);
        }
    }
}
