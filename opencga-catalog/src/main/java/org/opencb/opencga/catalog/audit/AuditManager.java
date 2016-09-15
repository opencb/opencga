package org.opencb.opencga.catalog.audit;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditRecord.Resource;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created on 18/08/15.
 * <p>
 * Create the AuditRecord from simple params
 * Select which actions will be recorded
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuditManager {

    /**
     * Records an object creation over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the creation
     * @param object      Created object
     * @param description Optional description
     * @param attributes  Optional attributes
     * @return Generated AuditRecord
     * @throws CatalogException CatalogException
     */
    @Deprecated
    AuditRecord recordCreation(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Records a object reading over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the creation
     * @param description Optional description
     * @param attributes  Optional attributes
     * @return Generated AuditRecord
     * @throws CatalogException CatalogException
     */
    @Deprecated
    AuditRecord recordRead(Resource resource, Object id, String userId, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Record an atomic change over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the update
     * @param update      Update params
     * @param description Optional description
     * @param attributes  Optional attributes
     * @return Generated AuditRecord
     * @throws CatalogException CatalogException
     */
    @Deprecated
    AuditRecord recordUpdate(Resource resource, Object id, String userId, ObjectMap update, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Records a permanent delete over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the deletion
     * @param object      Deleted object
     * @param description Optional description
     * @param attributes  Optional attributes
     * @return Generated AuditRecord
     * @throws CatalogException CatalogException
     */
    @Deprecated
    AuditRecord recordDeletion(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Records an object creation over the Catalog Database.
     *
     * @param resource    Resource type
     * @param action      Executed action
     * @param importance  Importance of the document being audited (high, medium or low)
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the action
     * @param before      Optional Previous object state
     * @param after       Optional Posterior object state
     * @param description Optional description
     * @param attributes  Optional attributes
     * @return Generated AuditRecord
     * @throws CatalogException CatalogException
     */
    AuditRecord recordAction(Resource resource, AuditRecord.Action action, AuditRecord.Magnitude importance, Object id, String userId,
                             Object before, Object after, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Executes a query over the audit log.
     *
     * @param query        Query to execute. Must validate with AuditQueryParam
     * @param queryOptions Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @param sessionId    sessionId
     * @return A QueryResult with a list of all matching AuditRecords
     * @throws CatalogException CatalogException
     */
    QueryResult<AuditRecord> getRecords(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException;

}
