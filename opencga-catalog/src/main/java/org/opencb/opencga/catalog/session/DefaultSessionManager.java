package org.opencb.opencga.catalog.session;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Session;

/**
 * Created by pfurio on 24/05/16.
 */
public class DefaultSessionManager implements SessionManager {

    private final UserDBAdaptor userDBAdaptor;
    private final MetaDBAdaptor metaDBAdaptor;

    private final int USER_SESSION_LENGTH = 20;
    private final int ADMIN_SESSION_LENGTH = 40;

    public DefaultSessionManager(DBAdaptorFactory dbAdaptorFactory) {
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
        this.metaDBAdaptor = dbAdaptorFactory.getCatalogMetaDBAdaptor();
    }

    @Override
    public QueryResult<ObjectMap> createToken(String userId, String ip) throws CatalogException {
        int length = USER_SESSION_LENGTH;
        if (userId.equals("admin")) {
            length = ADMIN_SESSION_LENGTH;
        }

        // Create the session
        Session session = new Session(ip, length);
        while (true) {
            if (length == USER_SESSION_LENGTH) {
                if (userDBAdaptor.getUserIdBySessionId(session.getId()).isEmpty()) {
                    break;
                }
            } else {
                if (!metaDBAdaptor.checkValidAdminSession(session.getId())) {
                    break;
                }
            }
            session.generateNewId(length);
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
