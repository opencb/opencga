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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditMongoDBAdaptor extends MongoDBAdaptor implements AuditDBAdaptor {

    private final MongoDBCollection auditCollection;

    public AuditMongoDBAdaptor(MongoDBCollection auditCollection) {
        super(LoggerFactory.getLogger(AuditMongoDBAdaptor.class));
        this.auditCollection = auditCollection;
    }

    @Override
    public QueryResult<AuditRecord> insertAuditRecord(AuditRecord auditRecord) throws CatalogDBException {
        long startQuery = startQuery();

//        DBObject auditRecordDbObject = CatalogMongoDBUtils.getDbObject(auditRecord, "AuditRecord");
        Document auditRecordDbObject = MongoDBUtils.getMongoDBDocument(auditRecord, "AuditRecord");
//        WriteResult writeResult = auditCollection.insert(auditRecordDbObject, new QueryOptions()).first();
        auditCollection.insert(auditRecordDbObject, new QueryOptions());

        return endQuery("insertAuditRecord", startQuery, Collections.singletonList(auditRecord));
    }

    @Override
    public QueryResult<AuditRecord> get(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (MongoDBUtils.isDataStoreOption(key) || MongoDBUtils.isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                // FIXME Pedro!! fix this please
//                AuditFilterOption option = AuditFilterOption.valueOf(key);
//                switch (option) {
//                    default:
//                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
//                        addCompQueryFilter(option, entry.getKey(), query, queryKey, mongoQueryList);
//                        break;
//                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }
        QueryResult<Document> result = auditCollection.find(new BasicDBObject("$and", mongoQueryList), queryOptions);
        List<AuditRecord> individuals = MongoDBUtils.parseObjects(result, AuditRecord.class);
        return endQuery("getAuditRecord", startTime, individuals);
    }
}
