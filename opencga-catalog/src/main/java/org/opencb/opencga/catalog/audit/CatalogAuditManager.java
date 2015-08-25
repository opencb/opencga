package org.opencb.opencga.catalog.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.CatalogAuditDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opencb.opencga.catalog.audit.AuditRecord.*;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuditManager implements AuditManager {

    protected static Logger logger = LoggerFactory.getLogger(CatalogAuditManager.class);
    final private CatalogAuditDBAdaptor auditDBAdaptor;
    final private CatalogUserDBAdaptor userDBAdaptor;
    final private AuthorizationManager authorizationManager;

    public CatalogAuditManager(CatalogAuditDBAdaptor auditDBAdaptor, CatalogUserDBAdaptor userDBAdaptor, AuthorizationManager authorizationManager) {
        this.auditDBAdaptor = auditDBAdaptor;
        this.userDBAdaptor = userDBAdaptor;
        this.authorizationManager = authorizationManager;
    }

    @Override
    public AuditRecord recordCreation(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, AuditRecord.CREATE, null, toObjectMap(object), System.currentTimeMillis(), userId, description, attributes);
        logger.info("Creation {}", auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Override
    public AuditRecord recordRead(Resource resource, Object id, String userId, String description, ObjectMap attributes)
            throws CatalogException {
        return null;
    }

    @Override
    public AuditRecord recordUpdate(Resource resource, Object id, String userId, ObjectMap update, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, AuditRecord.UPDATE, null, update, System.currentTimeMillis(), userId, description, attributes);
        logger.info("Update {}", auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Override
    public AuditRecord recordDeletion(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, AuditRecord.DELETE, toObjectMap(object), null, System.currentTimeMillis(), userId, description, attributes);
        logger.info("Deletion {}", auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Override
    public AuditRecord recordAction(Resource resource, String action, Object id, String userId, ObjectMap before, ObjectMap after, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, action, before, after, System.currentTimeMillis(), userId, description, attributes);
        logger.info("{} {}", action, auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Override
    public QueryResult<AuditRecord> getRecords(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            throw new CatalogAuthorizationException("User " + userId + " can't read AuditRecords");
        }
        return auditDBAdaptor.get(query, queryOptions);
    }

    private ObjectMap toObjectMap(Object object) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return new ObjectMap(objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return new ObjectMap("object", object);
        }
    }

}
