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

import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyConfigurationConverter;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.mongodb.MongoDBCollection.REPLACE;
import static org.opencb.commons.datastore.mongodb.MongoDBCollection.UPSERT;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class MongoDBStudyConfigurationManager extends StudyConfigurationManager {

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final String collectionName;
    private final boolean closeConnection;

    private final DocumentToStudyConfigurationConverter studyConfigurationConverter = new DocumentToStudyConfigurationConverter();

    public MongoDBStudyConfigurationManager(MongoCredentials credentials, String collectionName) throws UnknownHostException {
        super(null);
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        closeConnection = true;
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        this.collectionName = collectionName;
    }

    public MongoDBStudyConfigurationManager(MongoDataStoreManager mongoManager, MongoCredentials credentials, String collectionName)
            throws UnknownHostException {
        super(null);
        // Mongo configuration
        this.mongoManager = mongoManager;
        closeConnection = false;
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        this.collectionName = collectionName;
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        return internalGetStudyConfiguration(null, studyName, timeStamp, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return internalGetStudyConfiguration(studyId, null, timeStamp, options);
    }

    private QueryResult<StudyConfiguration> internalGetStudyConfiguration(Integer studyId, String studyName, Long timeStamp, QueryOptions
            options) {
        long start = System.currentTimeMillis();
        StudyConfiguration studyConfiguration;

        MongoDBCollection coll = db.getCollection(collectionName);

        Document query = new Document();
        if (studyId != null) {
            query.append("_id", studyId);
        }
        if (studyName != null) {
            query.append("studyName", studyName);
        }
        if (timeStamp != null) {
            query.append("timeStamp", new Document("$ne", timeStamp));
        }

        QueryResult<StudyConfiguration> queryResult = coll.find(query, null, studyConfigurationConverter, null);
        if (queryResult.getResult().isEmpty()) {
            studyConfiguration = null;
        } else {
            studyConfiguration = queryResult.first();
        }

        if (studyConfiguration == null) {
            return new QueryResult<>(studyName, ((int) (System.currentTimeMillis() - start)), 0, 0, "", "", Collections.emptyList());
        } else {
            return new QueryResult<>(studyName, ((int) (System.currentTimeMillis() - start)), 1, 1, "", "",
                    Collections.singletonList(studyConfiguration));
        }
    }

    @Override
    public QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        Document studyMongo = new DocumentToStudyConfigurationConverter().convertToStorageType(studyConfiguration);

        Document query = new Document("_id", studyConfiguration.getStudyId());
        QueryResult<UpdateResult> queryResult = coll.update(query, studyMongo, new QueryOptions(UPSERT, true).append(REPLACE, true));
//        studyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);

        return queryResult;
    }

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        List<String> studyNames = coll.distinct("studyName", null).getResult();

        return studyNames.stream().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public List<Integer> getStudyIds(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        return coll.distinct("_id", null, Integer.class).getResult();
    }

    @Override
    public Map<String, Integer> getStudies(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);
        QueryResult<StudyConfiguration> queryResult = coll.find(new Document(), Projections.include("studyId", "studyName"),
                studyConfigurationConverter, null);
        return queryResult.getResult()
                .stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyName, StudyConfiguration::getStudyId));
    }

    @Override
    public void close() {
        if (closeConnection) {
            mongoManager.close();
        }
    }
}
