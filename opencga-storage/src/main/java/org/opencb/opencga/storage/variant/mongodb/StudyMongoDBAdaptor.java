package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
        return coll.distinct(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, null);
    }

    @Override
    public QueryResult countSources() {
        MongoDBCollection coll = db.getCollection("files");
        return coll.count();
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
        DBObject project = new BasicDBObject("$project", new BasicDBObject("_id", 0).append(DBObjectToVariantSourceConverter.STUDYID_FIELD, 1).append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, 1));
        DBObject group = new BasicDBObject("$group", 
                new BasicDBObject("_id", new BasicDBObject(DBObjectToVariantSourceConverter.STUDYID_FIELD, "$studyId").append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, "$studyName"))
                .append("numFiles", new BasicDBObject("$sum", 1)));
        
        
        QueryResult aggregationResult = coll.aggregate("$studyInfo", Arrays.asList(match, project, group), options);
        Iterable<DBObject> results = aggregationResult.getResult();
        DBObject dbo = results.iterator().next();
        DBObject dboId = (DBObject) dbo.get("_id");
        
        DBObject outputDbo = new BasicDBObject(DBObjectToVariantSourceConverter.STUDYID_FIELD, dboId.get(DBObjectToVariantSourceConverter.STUDYID_FIELD)).append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, dboId.get(DBObjectToVariantSourceConverter.STUDYNAME_FIELD)).append("numFiles", dbo.get("numFiles"));
        QueryResult transformedResult = new QueryResult(aggregationResult.getId(), aggregationResult.getDbTime(), 
                aggregationResult.getNumResults(), aggregationResult.getNumTotalResults(), 
                aggregationResult.getWarningMsg(), aggregationResult.getErrorMsg(), Arrays.asList(outputDbo));
        return transformedResult;
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
        if (samplesInSources.size() != (long) countSources().getResult().get(0)) {
            synchronized (StudyMongoDBAdaptor.class) {
                if (samplesInSources.size() != (long) countSources().getResult().get(0)) {
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
        return builder.and(DBObjectToVariantSourceConverter.STUDYNAME_FIELD).is(name);
    }
    
    private QueryBuilder getStudyIdFilter(String id, QueryBuilder builder) {
        return builder.and(DBObjectToVariantSourceConverter.STUDYID_FIELD).is(id);
    }
    
    /**
     * Populates the dictionary relating sources and samples. 
     * 
     * @return The QueryResult with information of how long the query took
     */
    private QueryResult populateSamplesInSources() {
        MongoDBCollection coll = db.getCollection("files");
        DBObject returnFields = new BasicDBObject(DBObjectToVariantSourceConverter.FILEID_FIELD, true)
                .append(DBObjectToVariantSourceConverter.STUDYID_FIELD, true)
                .append(DBObjectToVariantSourceConverter.SAMPLES_FIELD, true);
        QueryResult queryResult = coll.find(null, null, null, returnFields);
        
        List<DBObject> result = queryResult.getResult();
        for (DBObject dbo : result) {
            String key = dbo.get(DBObjectToVariantSourceConverter.STUDYID_FIELD).toString() + "_" 
                    + dbo.get(DBObjectToVariantSourceConverter.FILEID_FIELD).toString();
            DBObject value = (DBObject) dbo.get(DBObjectToVariantSourceConverter.SAMPLES_FIELD);
            samplesInSources.put(key, new ArrayList<>(value.toMap().keySet()));
        }
        
        return queryResult;
    }
    
    private void populateSamplesInSourcesQueryResult(String fileId, String studyId, QueryResult queryResult) {
        List<List> samples = new ArrayList<>(1);
        List<String> samplesInSource = samplesInSources.get(studyId + "_" + fileId);

        if (samplesInSource == null || samplesInSource.isEmpty()) {
            queryResult.setWarningMsg("Source " + fileId + " in study " + studyId + " not found");
            queryResult.setNumTotalResults(0);
        } else {
            samples.add(samplesInSource);
            queryResult.setResult(samples);
            queryResult.setNumTotalResults(1);
        }
    }
}
