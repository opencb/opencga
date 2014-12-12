package org.opencb.opencga.catalog.db;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * Created by imedina on 21/11/14.
 */
public class CatalogMongoDBUtils {


    public CatalogMongoDBUtils() {
    }

    static int getNewAutoIncrementId(String field, MongoDBCollection metaCollection){
        QueryResult<DBObject> result = metaCollection.findAndModify(
                new BasicDBObject("_id", "METADATA"),  //Query
                new BasicDBObject(field, true),  //Fields
                null,
                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
                new QueryOptions("returnNew", true),
                null
        );
        return (int) Float.parseFloat(result.getResult().get(0).get(field).toString());
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
