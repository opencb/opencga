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

package org.opencb.opencga.storage.mongodb.metadata;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationAdaptor;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
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
public class MongoDBStudyConfigurationDBAdaptor extends StudyConfigurationAdaptor {

    private final MongoDataStoreManager mongoManager;
    private final boolean closeConnection;

    private final DocumentToStudyConfigurationConverter studyConfigurationConverter = new DocumentToStudyConfigurationConverter();
    private final MongoLock mongoLock;
    private final MongoDBCollection collection;

    public MongoDBStudyConfigurationDBAdaptor(MongoCredentials credentials, String collectionName)
            throws UnknownHostException {
        this(new MongoDataStoreManager(credentials.getDataStoreServerAddresses()), true, credentials, collectionName);
    }

    public MongoDBStudyConfigurationDBAdaptor(MongoDataStoreManager mongoManager, MongoCredentials credentials, String collectionName)
            throws UnknownHostException {
        this(mongoManager, false, credentials, collectionName);
    }

    private MongoDBStudyConfigurationDBAdaptor(MongoDataStoreManager mongoManager, boolean closeConnection,
                                               MongoCredentials credentials, String collectionName)
            throws UnknownHostException {
        // Mongo configuration
        this.mongoManager = mongoManager;
        this.closeConnection = closeConnection;
        MongoDataStore db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        collection = db.getCollection(collectionName)
                .withReadPreference(ReadPreference.primary())
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);
        mongoLock = new MongoLock(collection, "_lock");
    }

    @Override
    protected QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        return getStudyConfiguration(null, studyName, timeStamp, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return getStudyConfiguration(studyId, null, timeStamp, options);
    }

    @Override
    public long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws InterruptedException, TimeoutException {
        if (StringUtils.isNotEmpty(lockName)) {
            throw new UnsupportedOperationException("Unsupported lockStudy given a lockName");
        }

        try {
            // Ensure document exists
            collection.update(new Document("_id", studyId), set("id", studyId), new QueryOptions(MongoDBCollection.UPSERT, true));
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
    public void unLockStudy(int studyId, long lockId, String lockName) {
        if (StringUtils.isNotEmpty(lockName)) {
            throw new UnsupportedOperationException("Unsupported unlockStudy given a lockName");
        }
        mongoLock.unlock(studyId, lockId);
    }

    private QueryResult<StudyConfiguration> getStudyConfiguration(Integer studyId, String studyName, Long timeStamp,
                                                                  QueryOptions options) {
        long start = System.currentTimeMillis();
        StudyConfiguration studyConfiguration;

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

        QueryResult<StudyConfiguration> queryResult = collection.find(query, null, studyConfigurationConverter, null);
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
    public QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        Document studyMongo = new DocumentToStudyConfigurationConverter().convertToStorageType(studyConfiguration);

        // Update field by field, instead of replacing the whole object to preserve existing fields like "_lock"
        Document query = new Document("_id", studyConfiguration.getStudyId());
        List<Bson> updates = new ArrayList<>(studyMongo.size());
        studyMongo.forEach((s, o) -> updates.add(new Document("$set", new Document(s, o))));
        QueryResult<UpdateResult> queryResult = collection.update(query, Updates.combine(updates), new QueryOptions(UPSERT, true));
//        studyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);

        return queryResult;
    }

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        List<String> studyNames = collection.distinct("studyName", null).getResult();
        return studyNames.stream().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public List<Integer> getStudyIds(QueryOptions options) {
        return collection.distinct("_id", null, Integer.class).getResult();
    }

    @Override
    public Map<String, Integer> getStudies(QueryOptions options) {
        QueryResult<StudyConfiguration> queryResult = collection.find(new Document(), Projections.include("studyId", "studyName"),
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
