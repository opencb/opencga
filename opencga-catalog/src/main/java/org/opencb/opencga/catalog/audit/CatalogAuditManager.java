package org.opencb.opencga.catalog.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.NotImplementedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.opencb.opencga.catalog.audit.AuditRecord.Resource;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuditManager implements AuditManager {

    protected static Logger logger = LoggerFactory.getLogger(CatalogAuditManager.class);
    private final AuditDBAdaptor auditDBAdaptor;
    private final UserDBAdaptor userDBAdaptor;
    private final AuthorizationManager authorizationManager;
    private final Properties catalogProperties;
    private final CatalogConfiguration catalogConfiguration;

    @Deprecated
    public CatalogAuditManager(AuditDBAdaptor auditDBAdaptor, UserDBAdaptor userDBAdaptor,
                               AuthorizationManager authorizationManager, Properties catalogProperties) {
        this.auditDBAdaptor = auditDBAdaptor;
        this.userDBAdaptor = userDBAdaptor;
        this.authorizationManager = authorizationManager;
        this.catalogProperties = catalogProperties;
        this.catalogConfiguration = null;
    }

    public CatalogAuditManager(AuditDBAdaptor auditDBAdaptor, UserDBAdaptor userDBAdaptor,
                               AuthorizationManager authorizationManager, CatalogConfiguration catalogConfiguration) {
        this.auditDBAdaptor = auditDBAdaptor;
        this.userDBAdaptor = userDBAdaptor;
        this.authorizationManager = authorizationManager;
        this.catalogConfiguration = catalogConfiguration;
        this.catalogProperties = null;
    }

    @Deprecated
    @Override
    public AuditRecord recordCreation(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, AuditRecord.Action.create, AuditRecord.Magnitude.medium, null,
                toObjectMap(object), System.currentTimeMillis(), userId, description, attributes);
        logger.debug("{}", auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Deprecated
    @Override
    public AuditRecord recordRead(Resource resource, Object id, String userId, String description, ObjectMap attributes)
            throws CatalogException {
        return null;
    }

    @Deprecated
    @Override
    public AuditRecord recordUpdate(Resource resource, Object id, String userId, ObjectMap update, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, AuditRecord.Action.update, AuditRecord.Magnitude.medium, null, update,
                System.currentTimeMillis(), userId, description, attributes);
        logger.debug("{}", auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Deprecated
    @Override
    public AuditRecord recordDeletion(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, AuditRecord.Action.delete, AuditRecord.Magnitude.medium,
                toObjectMap(object), null, System.currentTimeMillis(), userId, description, attributes);
        logger.debug("{}", auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Override
    public AuditRecord recordAction(Resource resource, AuditRecord.Action action, AuditRecord.Magnitude importance, Object id,
                                    String userId, Object before, Object after, String description, ObjectMap attributes)
            throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, action, importance, toObjectMap(before), toObjectMap(after),
                System.currentTimeMillis(), userId, description, attributes);
        logger.debug("{}", action, auditRecord);
        return auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    @Override
    public QueryResult<AuditRecord> getRecords(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        // TODO: Check if this will be useful to implement
        throw new NotImplementedException("Get records still to implement");
//        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
//            throw new CatalogAuthorizationException("User " + userId + " can't read AuditRecords");
//        }
//        return auditDBAdaptor.get(query, queryOptions);
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
