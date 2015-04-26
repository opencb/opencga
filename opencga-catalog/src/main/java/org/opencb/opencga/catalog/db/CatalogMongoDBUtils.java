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

package org.opencb.opencga.catalog.db;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * Created by imedina on 21/11/14.
 */
class CatalogMongoDBUtils {


    static int getNewAutoIncrementId(MongoDBCollection metaCollection) {
        return getNewAutoIncrementId("idCounter", metaCollection);
    }

    static int getNewAutoIncrementId(String field, MongoDBCollection metaCollection){
        QueryResult<BasicDBObject> result = metaCollection.findAndModify(
                new BasicDBObject("_id", CatalogMongoDBAdaptor.METADATA_OBJECT_ID),  //Query
                new BasicDBObject(field, true),  //Fields
                null,
                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
                new QueryOptions("returnNew", true),
                BasicDBObject.class
        );
//        return (int) Float.parseFloat(result.getResult().get(0).get(field).toString());
        return result.getResult().get(0).getInt(field);
    }

    static void checkUserExist(String userId, boolean exists, MongoDBCollection UserMongoDBCollection) throws CatalogDBException {
        if(userId == null) {
            throw new CatalogDBException("userId param is null");
        }
        if(userId.equals("")) {
            throw new CatalogDBException("userId is empty");
        }

    }

}
