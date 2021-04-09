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

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.OpenCgaMongoConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.catalog.db.api.AuditDBAdaptor.QueryParams.STATUS_NAME;


/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditMongoDBAdaptor extends MongoDBAdaptor implements AuditDBAdaptor {

    private final MongoDBCollection auditCollection;
    private final OpenCgaMongoConverter<AuditRecord> auditConverter;

    public AuditMongoDBAdaptor(MongoDBCollection auditCollection, Configuration configuration) {
        super(configuration, LoggerFactory.getLogger(AuditMongoDBAdaptor.class));
        this.auditCollection = auditCollection;
        this.auditConverter = new OpenCgaMongoConverter<>(AuditRecord.class);
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
        Bson bson = parseQuery(query);
        logger.debug("Audit query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        return new OpenCGAResult<>(auditCollection.find(bson, auditConverter, queryOptions));
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
                    case STATUS:
                    case STATUS_NAME:
                        addAutoOrQuery(STATUS_NAME.key(), queryParam.key(), query, STATUS_NAME.type(), andBsonList);
                        break;
                    case OPERATION_ID:
                    case USER_ID:
                    case ACTION:
                    case RESOURCE:
                    case RESOURCE_ID:
                    case RESOURCE_UUID:
                    case STUDY_ID:
                    case STUDY_UUID:
                    case DATE:
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
