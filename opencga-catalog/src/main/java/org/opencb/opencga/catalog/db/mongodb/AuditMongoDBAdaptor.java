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

import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditMongoDBAdaptor extends MongoDBAdaptor implements AuditDBAdaptor {

    private final MongoDBCollection auditCollection;

    public AuditMongoDBAdaptor(MongoDBCollection auditCollection, Configuration configuration) {
        super(configuration, LoggerFactory.getLogger(AuditMongoDBAdaptor.class));
        this.auditCollection = auditCollection;
    }

    @Override
    public OpenCGAResult<AuditRecord> insertAuditRecord(AuditRecord auditRecord) throws CatalogDBException {
        long startQuery = startQuery();

        Document auditRecordDbObject = MongoDBUtils.getMongoDBDocument(auditRecord, "AuditRecord");
        auditCollection.insert(auditRecordDbObject, new QueryOptions());

        return endQuery(startQuery, Collections.singletonList(auditRecord));
    }

    @Override
    public OpenCGAResult<AuditRecord> insertAuditRecords(List<AuditRecord> auditRecords) throws CatalogDBException {
        List<Document> auditRecordDbObjects = new ArrayList<>(auditRecords.size());
        for (AuditRecord auditRecord : auditRecords) {
            auditRecordDbObjects.add(MongoDBUtils.getMongoDBDocument(auditRecord, "AuditRecord"));
        }

        return new OpenCGAResult<AuditRecord>(auditCollection.insert(auditRecordDbObjects, new QueryOptions()))
                .setResults(auditRecords);
    }

    @Override
    public OpenCGAResult<AuditRecord> get(Query query, QueryOptions queryOptions) throws CatalogDBException {
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
        DataResult<AuditRecord> result = auditCollection.find(new Document("$and", mongoQueryList), null, AuditRecord.class, queryOptions);
        return endQuery(startTime, result);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(auditCollection, bsonQuery, fields, "name", options);
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                continue;
            }
            try {
                switch (queryParam) {
                    case DATE:
                        addAutoOrQuery("timeStamp", queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case RESOURCE:
                    case ACTION:
                    case BEFORE:
                    case AFTER:
                    case USER_ID:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + query.toJson(), e);
                }
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
