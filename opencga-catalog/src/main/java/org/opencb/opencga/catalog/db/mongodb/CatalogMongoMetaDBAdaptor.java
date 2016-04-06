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

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 13/01/16.
 */
public class CatalogMongoMetaDBAdaptor extends CatalogMongoDBAdaptor {

    private final MongoDBCollection metaCollection;

    private List<String> COLLECTIONS_LIST = Collections.singletonList("");

    public CatalogMongoMetaDBAdaptor(CatalogMongoDBAdaptorFactory dbAdaptorFactory, MongoDBCollection userCollection) {
        super(LoggerFactory.getLogger(CatalogMongoProjectDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = userCollection;
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


    public void createCollections() {
        clean(Collections.singletonList(""));
//        metaCollection.createIndex()
//        dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection().createIndex()
    }

    public void createIndex(List<String> collections) {
        for (String collection : collections) {
            System.out.println(collection);
            switch (collection) {
                case "user":
                    break;
                default:
                    break;
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

}
