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

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.mongodb.utils.MongoLock;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyConfigurationConverter;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Updates.set;
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
    private final MongoLock mongoLock;

    public MongoDBStudyConfigurationManager(MongoCredentials credentials, String collectionName) throws UnknownHostException {
        super(null);
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        closeConnection = true;
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        this.collectionName = collectionName;
        mongoLock = new MongoLock(db.getCollection(collectionName), "_lock");
    }

    public MongoDBStudyConfigurationManager(MongoDataStoreManager mongoManager, MongoCredentials credentials, String collectionName)
            throws UnknownHostException {
        super(null);
        // Mongo configuration
        this.mongoManager = mongoManager;
        closeConnection = false;
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        this.collectionName = collectionName;
        mongoLock = new MongoLock(db.getCollection(collectionName), "_lock");
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        return internalGetStudyConfiguration(null, studyName, timeStamp, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return internalGetStudyConfiguration(studyId, null, timeStamp, options);
    }

    @Override
    public long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        try {
            // Ensure document exists
            MongoDBCollection coll = db.getCollection(collectionName);
            coll.update(new Document("_id", studyId), set("id", studyId), new QueryOptions(MongoDBCollection.UPSERT, true));
        } catch (MongoWriteException e) {
            // Duplicated key exception
            if (e.getError().getCode() != 11000) {
                throw e;
            }
        } catch (DuplicateKeyException ignore) {
            // Ignore this exception.
            // With UPSERT=true, this command should never throw DuplicatedKeyException.
            // See https://jira.mongodb.org/browse/SERVER-14322
        }
        return mongoLock.lock(studyId, lockDuration, timeout);
    }

    @Override
    public void unLockStudy(int studyId, long lockId) {
        mongoLock.unlock(studyId, lockId);
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
            if (queryResult.first().getStudyName() == null) {
                // If the studyName is null, it may be only a lock instead of a real study configuration
                studyConfiguration = null;
            } else {
                studyConfiguration = queryResult.first();
            }
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

        // Update field by field, instead of replacing the whole object to preserve existing fields like "_lock"
        Document query = new Document("_id", studyConfiguration.getStudyId());
        List<Bson> updates = new ArrayList<>(studyMongo.size());
        studyMongo.forEach((s, o) -> updates.add(new Document("$set", new Document(s, o))));
        QueryResult<UpdateResult> queryResult = coll.update(query, Updates.combine(updates), new QueryOptions(UPSERT, true));
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
