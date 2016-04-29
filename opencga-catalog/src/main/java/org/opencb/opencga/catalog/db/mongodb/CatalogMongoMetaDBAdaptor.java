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

package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.config.Admin;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Metadata;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.getMongoDBDocument;

/**
 * Created by imedina on 13/01/16.
 */
public class CatalogMongoMetaDBAdaptor extends CatalogMongoDBAdaptor {

    private final MongoDBCollection metaCollection;

    public CatalogMongoMetaDBAdaptor(CatalogMongoDBAdaptorFactory dbAdaptorFactory, MongoDBCollection metaMongoDBCollection) {
        super(LoggerFactory.getLogger(CatalogMongoProjectDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaMongoDBCollection;
    }

    public long getNewAutoIncrementId() {
        return getNewAutoIncrementId("idCounter"); //, metaCollection
    }

    public long getNewAutoIncrementId(String field) { //, MongoDBCollection metaCollection
//        QueryResult<BasicDBObject> result = metaCollection.findAndModify(
//                new BasicDBObject("_id", CatalogMongoDBAdaptor.METADATA_OBJECT_ID),  //Query
//                new BasicDBObject(field, true),  //Fields
//                null,
//                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
//                new QueryOptions("returnNew", true),
//                BasicDBObject.class
//        );

        Bson query = Filters.eq("_id", CatalogMongoDBAdaptorFactory.METADATA_OBJECT_ID);
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1L);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        QueryResult<Document> result = metaCollection.findAndUpdate(query, projection, null, inc, queryOptions);
//        return (int) Float.parseFloat(result.getResult().get(0).get(field).toString());
        return result.getResult().get(0).getLong(field);
    }


//    public void createCollections() {
//        clean(Collections.singletonList(""));
////        metaCollection.createIndexes()
////        dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection().createIndexes()
//    }

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
                myIndexes.put("options", new ObjectMap((Map) hashMap.get("options")));
                indexes.get(collection).add(myIndexes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        createIndexes(dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection(), indexes.get("user"));
        createIndexes(dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection(), indexes.get("study"));
        createIndexes(dbAdaptorFactory.getCatalogSampleDBAdaptor().getSampleCollection(), indexes.get("sample"));
        createIndexes(dbAdaptorFactory.getCatalogIndividualDBAdaptor().getIndividualCollection(), indexes.get("individual"));
        createIndexes(dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection(), indexes.get("file"));
        createIndexes(dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection(), indexes.get("cohort"));
//        createIndexes(dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection(), indexes.get("job"));

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
                    Map.Entry pair = (Map.Entry)fieldsIterator.next();
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

    public void initializeMetaCollection(CatalogConfiguration catalogConfiguration) throws CatalogException {
        Admin admin = catalogConfiguration.getAdmin();
        admin.setPassword(CatalogAuthenticationManager.cipherPassword(admin.getPassword()));

        Document metadataObject = getMongoDBDocument(new Metadata(), "Metadata");
        metadataObject.put("_id", "METADATA");
        Document adminDocument = getMongoDBDocument(admin, "Admin");
        adminDocument.put("sessions", new ArrayList<>());
        metadataObject.put("admin", adminDocument);

        metaCollection.insert(metadataObject, null);
    }

    public void checkAdmin(String password) throws CatalogException {
        Bson query = Filters.eq("admin.password", CatalogAuthenticationManager.cipherPassword(password));
        if (metaCollection.count(query).getResult().get(0) == 0) {
            throw new CatalogDBException("The admin password is incorrect.");
        }
    }

}
