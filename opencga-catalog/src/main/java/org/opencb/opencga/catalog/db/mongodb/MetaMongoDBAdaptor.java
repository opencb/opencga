/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.JwtUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Metadata;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getMongoDBDocument;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by imedina on 13/01/16.
 */
public class MetaMongoDBAdaptor extends MongoDBAdaptor implements MetaDBAdaptor {

    private static final String OLD_ID = "_id";
    public static final Bson METADATA_QUERY = Filters.or(
            Filters.eq(ID, MongoDBAdaptorFactory.METADATA_OBJECT_ID),
            Filters.eq(OLD_ID, MongoDBAdaptorFactory.METADATA_OBJECT_ID));
    private final MongoDBCollection metaCollection;
    private static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public MetaMongoDBAdaptor(MongoDBCollection metaMongoDBCollection, Configuration configuration,
                              MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ProjectMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaMongoDBCollection;
    }

    public long getNewAutoIncrementId() {
        return getNewAutoIncrementId(null, "idCounter"); //, metaCollection
    }

    public long getNewAutoIncrementId(ClientSession clientSession) {
        return getNewAutoIncrementId(clientSession, "idCounter"); //, metaCollection
    }

    public long getNewAutoIncrementId(ClientSession clientSession, String field) { //, MongoDBCollection metaCollection
        Bson query = METADATA_QUERY;
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1L);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        DataResult<Document> result = metaCollection.findAndUpdate(clientSession, query, projection, null, inc, queryOptions);
        return result.getResults().get(0).getLong(field);
    }

    public void createIndexes(boolean uniqueIndexesOnly) {
        InputStream resourceAsStream = getClass().getResourceAsStream("/catalog-indexes.txt");
        ObjectMapper objectMapper = getDefaultObjectMapper();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream));
        // We store all the indexes that are in the file in the indexes object
        Map<String, List<Map<String, ObjectMap>>> indexes = new HashMap<>();
        bufferedReader.lines().filter(s -> !s.trim().isEmpty()).forEach(s -> {
            try {
                HashMap hashMap = objectMapper.readValue(s, HashMap.class);

                List<String> collections = (List<String>) hashMap.get("collections");
                for (String collection : collections) {
                    if (!indexes.containsKey(collection)) {
                        indexes.put(collection, new ArrayList<>());
                    }
                    Map<String, ObjectMap> myIndexes = new HashMap<>();
                    myIndexes.put("fields", new ObjectMap((Map) hashMap.get("fields")));
                    myIndexes.put("options", new ObjectMap((Map) hashMap.getOrDefault("options", Collections.emptyMap())));

                    if (!uniqueIndexesOnly || myIndexes.get("options").getBoolean("unique")) {
                        indexes.get(collection).add(myIndexes);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            bufferedReader.close();
        } catch (IOException e) {
            logger.error("Error closing the buffer reader", e);
            throw new UncheckedIOException(e);
        }

        createIndexes(MongoDBAdaptorFactory.USER_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.STUDY_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.FILE_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.COHORT_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.JOB_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.AUDIT_COLLECTION, indexes);

        // Versioned collections
        createIndexes(MongoDBAdaptorFactory.SAMPLE_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.FAMILY_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.PANEL_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, indexes);
        createIndexes(MongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION, indexes);
    }

    private void createIndexes(String collection, Map<String, List<Map<String, ObjectMap>>> indexCollectionMap) {
        MongoDBCollection mongoCollection = dbAdaptorFactory.getMongoDBCollectionMap().get(collection);
        List<Map<String, ObjectMap>> indexes = indexCollectionMap.get(collection);

        DataResult<Document> index = mongoCollection.getIndex();
        // We store the existing indexes
        Set<String> existingIndexes = index.getResults()
                .stream()
                .map(document -> (String) document.get("name"))
                .collect(Collectors.toSet());

        if (index.getNumResults() != indexes.size() + 1) { // It is + 1 because mongo always create the _id index by default
            for (Map<String, ObjectMap> userIndex : indexes) {
                String indexName = "";
                Document keys = new Document();
                Iterator fieldsIterator = userIndex.get("fields").entrySet().iterator();
                while (fieldsIterator.hasNext()) {
                    Map.Entry pair = (Map.Entry) fieldsIterator.next();
                    keys.append((String) pair.getKey(), pair.getValue());

                    if (!indexName.isEmpty()) {
                        indexName += "_";
                    }
                    indexName += pair.getKey() + "_" + pair.getValue();
                }

                if (!existingIndexes.contains(indexName)) {
                    mongoCollection.createIndex(keys, new ObjectMap(userIndex.get("options")));
                }
            }
        }
    }

    public void clean() {
        clean(Collections.singletonList(""));
    }

    public void clean(List<String> collections) {
        for (String collection : collections) {
            System.out.println(collection);
        }
    }

    public void initializeMetaCollection(Configuration configuration) throws CatalogException {
        Metadata metadata = new Metadata().setIdCounter(0L).setVersion(VERSION);

        // Ensure JWT secret key is long and secure
        String secretKey = configuration.getAdmin().getSecretKey();
        if (StringUtils.isEmpty(secretKey)) {
            throw new CatalogDBException("Missing JWT secret key");
        } else {
            JwtUtils.validateJWTKey(configuration.getAdmin().getAlgorithm(), secretKey);
        }

        Document metadataObject = getMongoDBDocument(metadata, "Metadata");
        metadataObject.put(ID, MongoDBAdaptorFactory.METADATA_OBJECT_ID);
        Document adminDocument = getMongoDBDocument(configuration.getAdmin(), "Admin");
        metadataObject.put("admin", adminDocument);
        metadataObject.put("_fullVersion", new Document()
                .append("version", 20003)
                .append("release", 1)
                .append("lastJsUpdate", 0)
                .append("lastJavaUpdate", 1)
        );

        metaCollection.insert(metadataObject, null);
    }

    @Override
    public String readSecretKey() throws CatalogDBException {
        Bson query = METADATA_QUERY;
        DataResult queryResult = this.metaCollection.find(query, new QueryOptions("include", "admin"));
        if (queryResult.getNumResults() == 1) {
            return (MongoDBUtils.parseObject((Document) ((Document) queryResult.first()).get("admin"), Admin.class)).getSecretKey();
        }
        return "";
    }

    @Override
    public String readAlgorithm() throws CatalogDBException {
        Bson query = METADATA_QUERY;
        DataResult queryResult = this.metaCollection.find(query, new QueryOptions("include", "admin"));
        if (queryResult.getNumResults() == 1) {
            return (MongoDBUtils.parseObject((Document) ((Document) queryResult.first()).get("admin"), Admin.class)).getAlgorithm();
        }
        return "";
    }

    @Override
    public OpenCGAResult updateJWTParameters(ObjectMap params) throws CatalogDBException {
        Bson query = METADATA_QUERY;

        Document adminDocument = new Document();
        if (StringUtils.isNotEmpty(params.getString(SECRET_KEY))) {
            String algorithm = readAlgorithm();
            String secretKey = params.getString(SECRET_KEY);

            JwtUtils.validateJWTKey(algorithm, secretKey);
            adminDocument.append("admin.secretKey", params.getString(SECRET_KEY));
        }

        if (adminDocument.size() > 0) {
            Bson update = new Document("$set", adminDocument);
            return new OpenCGAResult(this.metaCollection.update(query, update, QueryOptions.empty()));
        }

        return OpenCGAResult.empty();
    }
}
