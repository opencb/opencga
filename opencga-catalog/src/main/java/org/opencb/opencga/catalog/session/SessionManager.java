package org.opencb.opencga.catalog.session;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created by pfurio on 24/05/16.
 */
public interface SessionManager {

    /**
     * Generates a unique and valid session id for the user.
     *
     * @param userId valid user id.
     * @param ip current ip of the user.
     * @return A queryResult object containing the generated session id.
     * @throws CatalogException if the user is not valid.
     */
    QueryResult<ObjectMap> createToken(String userId, String ip) throws CatalogException;

    /**
     * Closes the session.
     *
     * @param userId user id.
     * @param sessionId session id.
     * @throws CatalogException when the user id or the session id are not valid.
     */
    void clearToken(String userId, String sessionId) throws CatalogException;

    /**
     * Checks if the session id is a valid admin session id.
     *
     * @param sessionId session id.
     * @throws CatalogException when the session id is not valid.
     */
    void checkAdminSession(String sessionId) throws CatalogException;
}
