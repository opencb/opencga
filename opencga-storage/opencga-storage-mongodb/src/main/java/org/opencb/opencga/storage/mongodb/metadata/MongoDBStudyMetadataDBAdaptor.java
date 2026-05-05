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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class MongoDBStudyMetadataDBAdaptor extends AbstractMongoDBAdaptor<StudyMetadata> implements StudyMetadataDBAdaptor {

    private final Logger logger = LoggerFactory.getLogger(MongoDBStudyMetadataDBAdaptor.class);

    public MongoDBStudyMetadataDBAdaptor(MongoDataStore db, String collectionName) {
        super(db, collectionName, StudyMetadata.class);
        try {
            collection.createIndex(new Document("name", 1), new ObjectMap(MongoDBCollection.UNIQUE, true));
        } catch (DuplicateKeyException e) {
            logger.warn("Ignore duplicateKeyException creating index. Migration required " + e.getMessage());
            logger.debug("DuplicateKeyException creating index.", e);
        }
    }

    @Override
    public Lock lock(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
        if (StringUtils.isNotEmpty(lockName)) {
            throw new UnsupportedOperationException("Unsupported lockStudy given a lockName");
        }
        return lock(studyId, lockDuration, timeout);
    }

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        List<String> studyNames = collection.distinct("name", new Document("name", new Document("$exists", 1)), String.class).getResults();
        return studyNames.stream().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public List<Integer> getStudyIds(QueryOptions options) {
        return collection.distinct("_id", null, Integer.class).getResults();
    }

    @Override
    public Map<String, Integer> getStudies(QueryOptions options) {
        Map<String, Integer> map = new HashMap<>();
        iterator(new Document(), new QueryOptions(QueryOptions.INCLUDE, "id,name")).forEachRemaining(s -> map.put(s.getName(), s.getId()));
        return map;
    }

    @Override
    public StudyMetadata getStudyMetadata(int id, Long timeStamp) {
        return get(id, null);
    }

    @Override
    public void updateStudyMetadata(StudyMetadata studyMetadata) {
        update(studyMetadata.getId(), studyMetadata);
    }

    @Override
    public void close() {
    }
}
