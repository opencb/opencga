/*
 * Copyright 2015-2016 OpenCB
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.config.Admin;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Metadata;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getMongoDBDocument;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.parseObject;

/**
 * Created by imedina on 13/01/16.
 */
public class MetaMongoDBAdaptor extends MongoDBAdaptor implements MetaDBAdaptor {

    private final MongoDBCollection metaCollection;
    private static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public MetaMongoDBAdaptor(MongoDBCollection metaMongoDBCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(ProjectMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaMongoDBCollection;
    }

    public long getNewAutoIncrementId() {
        return getNewAutoIncrementId("idCounter"); //, metaCollection
    }

    public long getNewAutoIncrementId(String field) { //, MongoDBCollection metaCollection
        Bson query = Filters.eq(PRIVATE_ID, MongoDBAdaptorFactory.METADATA_OBJECT_ID);
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1L);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        QueryResult<Document> result = metaCollection.findAndUpdate(query, projection, null, inc, queryOptions);
        return result.getResult().get(0).getLong(field);
    }


    public void createIndexes() {
        InputStream resourceAsStream = getClass().getResourceAsStream("/catalog-indexes.txt");
        ObjectMapper objectMapper = new ObjectMapper();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream));
        // We store all the indexes that are in the file in the indexes object
        Map<String, List<Map<String, ObjectMap>>> indexes = new HashMap<>();
        bufferedReader.lines().filter(s -> !s.trim().isEmpty()).forEach(s -> {
            try {
                HashMap hashMap = objectMapper.readValue(s, HashMap.class);

                String collection = (String) hashMap.get("collection");
                if (!indexes.containsKey(collection)) {
                    indexes.put(collection, new ArrayList<>());
                }
                Map<String, ObjectMap> myIndexes = new HashMap<>();
                myIndexes.put("fields", new ObjectMap((Map) hashMap.get("fields")));
                myIndexes.put("options", new ObjectMap((Map) hashMap.getOrDefault("options", Collections.emptyMap())));
                indexes.get(collection).add(myIndexes);
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

        createIndexes(dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection(), indexes.get("user"));
        createIndexes(dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection(), indexes.get("study"));
        createIndexes(dbAdaptorFactory.getCatalogSampleDBAdaptor().getSampleCollection(), indexes.get("sample"));
        createIndexes(dbAdaptorFactory.getCatalogIndividualDBAdaptor().getIndividualCollection(), indexes.get("individual"));
        createIndexes(dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection(), indexes.get("file"));
        createIndexes(dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection(), indexes.get("cohort"));
        createIndexes(dbAdaptorFactory.getCatalogDatasetDBAdaptor().getDatasetCollection(), indexes.get("dataset"));
        createIndexes(dbAdaptorFactory.getCatalogJobDBAdaptor().getJobCollection(), indexes.get("job"));
        createIndexes(dbAdaptorFactory.getCatalogFamilyDBAdaptor().getFamilyCollection(), indexes.get("family"));

    }

    private void createIndexes(MongoDBCollection mongoCollection, List<Map<String, ObjectMap>> indexes) {
        QueryResult<Document> index = mongoCollection.getIndex();
        // We store the existent indexes
        Set<String> existingIndexes = index.getResult()
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
        Admin admin = configuration.getAdmin();
        admin.setPassword(CatalogAuthenticationManager.cypherPassword(admin.getPassword()));

        Metadata metadata = new Metadata().setIdCounter(configuration.getCatalog().getOffset()).setVersion(VERSION);

        Document metadataObject = getMongoDBDocument(metadata, "Metadata");
        metadataObject.put(PRIVATE_ID, "METADATA");
        Document adminDocument = getMongoDBDocument(admin, "Admin");
        metadataObject.put("admin", adminDocument);

        metaCollection.insert(metadataObject, null);
    }

    public void checkAdmin(String password) throws CatalogException {
        Bson query = Filters.eq("admin.password", CatalogAuthenticationManager.cypherPassword(password));
        if (metaCollection.count(query).getResult().get(0) == 0) {
            throw new CatalogDBException("The admin password is incorrect.");
        }
    }

    @Override
    public QueryResult<Session> addAdminSession(Session session) throws CatalogDBException {
        long startTime = startQuery();

        Bson query = new Document(PRIVATE_ID, "METADATA");
        Bson updates = Updates.push("admin.sessions",
                new Document("$each", Arrays.asList(getMongoDBDocument(session, "Session")))
                        .append("$slice", -5));
        QueryResult<UpdateResult> update = metaCollection.update(query, updates, null);

        if (update.first().getModifiedCount() == 0) {
            throw new CatalogDBException("An internal error occurred when logging the admin");
        }

        return endQuery("Login", startTime, Collections.singletonList(session));
    }

    @Override
    public void logout(String sessionId) throws CatalogDBException {
        Bson query = new Document("_id", "METADATA");
        Bson update = new Document("$pull", new Document("admin.sessions", new Document("id", sessionId)));
        QueryResult<UpdateResult> updateQueryResult = metaCollection.update(query, update, null);
        if (updateQueryResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("Internal error: Could not remove closed session for admin");
        }
    }

    @Override
    public String getAdminPassword() throws CatalogDBException {
        Bson query = Filters.eq(PRIVATE_ID, "METADATA");
        QueryResult<Document> queryResult = metaCollection.find(query, new QueryOptions(QueryOptions.INCLUDE, "admin"));
        return parseObject((Document) queryResult.first().get("admin"), Admin.class).getPassword();
    }

    @Override
    public boolean checkValidAdminSession(String id) {
        Document query = new Document(PRIVATE_ID, "METADATA").append("admin.sessions.id", id);
        return metaCollection.count(query).first() == 1;
    }

    @Override
    public void writeSecretKey(String secretKey) throws CatalogDBException {
        Bson query = Filters.eq("_id", "METADATA");
        Bson update = Updates.set("admin.secretKey", secretKey);
        this.metaCollection.update(query, update, QueryOptions.empty());
    }

    @Override
    public String readSecretKey() throws CatalogDBException {
        Bson query = Filters.eq("_id", "METADATA");
        QueryResult queryResult = this.metaCollection.find(query, new QueryOptions("include", "admin"));
        return ((Admin) MongoDBUtils.parseObject((Document) ((Document) queryResult.first()).get("admin"), Admin.class)).getSecretKey();
    }

    @Override
    public void writeAlgorithm(String algorithm) throws CatalogDBException {
        Bson query = Filters.eq("_id", "METADATA");
        Bson update = Updates.set("admin.algorithm", algorithm);
        this.metaCollection.update(query, update, QueryOptions.empty());
    }

    @Override
    public String readAlgorithm() throws CatalogDBException {
        Bson query = Filters.eq("_id", "METADATA");
        QueryResult queryResult = this.metaCollection.find(query, new QueryOptions("include", "admin"));
        return ((Admin) MongoDBUtils.parseObject((Document) ((Document) queryResult.first()).get("admin"), Admin.class)).getAlgorithm();
    }

    @Override
    public void updateAdmin(Admin admin) throws CatalogDBException {
        Bson query = Filters.eq("_id", "METADATA");
        Bson update = null;
        if (admin.getSecretKey() != null) {
            update = Updates.set("admin.secretKey", admin.getSecretKey());
        }

        if (admin.getAlgorithm() != null) {
            update = Updates.set("admin.algorithm", admin.getAlgorithm());
        }

        this.metaCollection.update(query, update, QueryOptions.empty());
    }
}
