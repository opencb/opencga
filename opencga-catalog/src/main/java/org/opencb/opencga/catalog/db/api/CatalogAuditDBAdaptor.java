package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditRecord;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogAuditDBAdaptor {

    QueryResult<AuditRecord> insertAuditRecord(AuditRecord auditRecord);

    QueryResult<AuditRecord> get(Query query, QueryOptions queryOptions);

}
