package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hpccoll1 on 19/03/15.
 */
public class StudyConfigurationMongoDBAdaptor {

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final String collectionName;

    private final Map<Integer, StudyConfiguration> studyConfigurationMap = new HashMap();
    private final DBObjectToStudyConfigurationConverter studyConfigurationConverter = new DBObjectToStudyConfigurationConverter();


    public StudyConfigurationMongoDBAdaptor(MongoCredentials credentials, String collectionName) throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", credentials.getUsername())
                .add("password", credentials.getPassword() != null ? new String(credentials.getPassword()) : null).build();
        db = mongoManager.get(credentials.getMongoDbName(), mongoDBConfiguration);
        this.collectionName = collectionName;
    }

    public QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options) {
        long start = System.currentTimeMillis();
        StudyConfiguration studyConfiguration;
        if (!studyConfigurationMap.containsKey(studyId)) {
            MongoDBCollection coll = db.getCollection(collectionName);

            BasicDBObject query = new BasicDBObject("studyId", studyId);
            if (options.containsKey("fileId")) {
                query.put(DBObjectToStudyConfigurationConverter.FIELD_FILE_IDS, options.getInt("fileId"));
            }
            QueryResult<StudyConfiguration> queryResult = coll.find(query, null, studyConfigurationConverter, options);
            if (queryResult.getResult().isEmpty()) {
                studyConfiguration = null;
            } else {
                studyConfiguration = queryResult.first();
            }
            studyConfigurationMap.put(studyId, studyConfiguration);
        } else {
            studyConfiguration = studyConfigurationMap.get(studyId);
        }

        return new QueryResult<>("getStudyConfiguration", ((int) (System.currentTimeMillis() - start)), 1, 1, "", "", Collections.singletonList(studyConfiguration));
    }

    public QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        DBObject studyMongo = new DBObjectToStudyConfigurationConverter().convertToStorageType(studyConfiguration);

        DBObject query = new BasicDBObject("studyId", studyConfiguration.getStudyId());
        QueryResult<WriteResult> queryResult = coll.update(query, studyMongo, new QueryOptions("upsert", true));
        studyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);

        return queryResult;
    }

}
