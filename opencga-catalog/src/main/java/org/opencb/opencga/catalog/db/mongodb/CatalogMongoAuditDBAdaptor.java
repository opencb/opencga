package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.audit.AuditFilterOption;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.CatalogAuditDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.parseObjects;


/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoAuditDBAdaptor extends CatalogDBAdaptor implements CatalogAuditDBAdaptor {


    private final MongoDBCollection auditCollection;

    public CatalogMongoAuditDBAdaptor(MongoDBCollection auditCollection) {
        super(LoggerFactory.getLogger(CatalogMongoAuditDBAdaptor.class));
        this.auditCollection = auditCollection;
    }

    @Override
    public QueryResult<AuditRecord> insertAuditRecord(AuditRecord auditRecord) throws CatalogDBException {
        long startQuery = startQuery();

        DBObject auditRecordDbObject = CatalogMongoDBUtils.getDbObject(auditRecord, "AuditRecord");
        WriteResult writeResult = auditCollection.insert(auditRecordDbObject, new QueryOptions()).first();

        return endQuery("insertAuditRecord", startQuery, Collections.singletonList(auditRecord));
    }

    @Override
    public QueryResult<AuditRecord> get(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                AuditFilterOption option = AuditFilterOption.valueOf(key);
                switch (option) {
                    default:
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), query, queryKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }
        QueryResult<DBObject> result = auditCollection.find(new BasicDBObject("$and", mongoQueryList), queryOptions);
        List<AuditRecord> individuals = parseObjects(result, AuditRecord.class);
        return endQuery("getAuditRecord", startTime, individuals);    }
}
