package org.opencb.opencga.catalog.audit;

import org.opencb.datastore.core.*;
import org.opencb.opencga.catalog.audit.AuditRecord.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuditManager {

    /**
     * Records an object creation over the Catalog Database
     *
     * @param resource          Resource type
     * @param id                Resource id (either String or Integer)
     * @param userId            User who performs the creation
     * @param object            Created object
     * @param description       Optional description
     * @param attributes        Optional attributes
     * @return                  Generated AuditRecord
     */
    AuditRecord recordCreation(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
    throws CatalogException;

    /**
     * Record an atomic change over the Catalog Database
     *
     * @param resource          Resource type
     * @param id                Resource id (either String or Integer)
     * @param userId            User who performs the update
     * @param update            Update params
     * @param description       Optional description
     * @param attributes        Optional attributes
     * @return                  Generated AuditRecord
     */
    AuditRecord recordUpdate(Resource resource, Object id, String userId, ObjectMap update, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Records a permanent delete over the Catalog Database
     *
     * @param resource          Resource type
     * @param id                Resource id (either String or Integer)
     * @param userId            User who performs the deletion
     * @param object            Deleted object
     * @param description       Optional description
     * @param attributes        Optional attributes
     * @return                  Generated AuditRecord
     */
    AuditRecord recordDelete(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Records an object creation over the Catalog Database
     *
     * @param resource          Resource type
     * @param action            Executed action
     * @param id                Resource id (either String or Integer)
     * @param userId            User who performs the action
     * @param before            Optional Previous object state
     * @param after             Optional Posterior object state
     * @param description       Optional description
     * @param attributes        Optional attributes
     * @return                  Generated AuditRecord
     */
    AuditRecord recordAction(Resource resource, String action, Object id, String userId, ObjectMap before, ObjectMap after,
                             String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Executes a query over the audit log
     *
     * @param query             Query to execute. Must validate with AuditQueryParam
     * @param queryOptions      Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return                  A QueryResult with a list of all matching AuditRecords
     */
    QueryResult<AuditRecord> getRecords(Query query, QueryOptions queryOptions, String sessionId);

}
