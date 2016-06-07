package org.opencb.opencga.catalog.session;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogMetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Session;

/**
 * Created by pfurio on 24/05/16.
 */
public class CatalogSessionManager implements SessionManager {

    protected final CatalogUserDBAdaptor userDBAdaptor;
    protected final CatalogMetaDBAdaptor metaDBAdaptor;
    protected final CatalogConfiguration catalogConfiguration;

    public CatalogSessionManager(CatalogDBAdaptorFactory dbAdaptorFactory, CatalogConfiguration catalogConfiguration) {
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
        this.metaDBAdaptor = dbAdaptorFactory.getCatalogMetaDBAdaptor();
        this.catalogConfiguration = catalogConfiguration;
    }

    @Override
    public QueryResult<ObjectMap> createToken(String userId, String ip) throws CatalogException {
        int length = 20;
        if (userId.equals("admin")) {
            length = 40;
        }

        // Create the session
        Session session = new Session(ip, length);
        while (true) {
            try {
                checkUniqueSessionId(session.getId());
                break;
            } catch (CatalogException e) {
                session.generateNewId(length);
            }
        }

        QueryResult<ObjectMap> result;
        // Add the session to the user
        if (userId.equals("admin")) {
            result = metaDBAdaptor.addAdminSession(session);
        } else {
            result = userDBAdaptor.addSession(userId, session);
        }

        return result;
    }

    private void checkUniqueSessionId(String id) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(id);
        if (!userId.isEmpty()) {
            throw new CatalogException("");
        }
        if (metaDBAdaptor.checkValidAdminSession(id)) {
            throw new CatalogException("");
        }
    }

    @Override
    public void checkAdminSession(String sessionId) throws CatalogException {
        if (!metaDBAdaptor.checkValidAdminSession(sessionId)) {
            throw new CatalogException("The admin session id is not valid.");
        }
    }

    @Override
    public void clearToken(String userId, String sessionId) throws CatalogException {

    }


}
