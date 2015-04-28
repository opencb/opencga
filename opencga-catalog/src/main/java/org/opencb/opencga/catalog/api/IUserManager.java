package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.User;

import java.io.IOException;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IUserManager extends ResourceManager<String, User> {

    /**
     * Get the userId from the sessionId
     *
     * @param sessionId     SessionId
     * @return              UserId owner of the sessionId. Empty string if SessionId does not match.
     */
    public String getUserId(String sessionId);

    /**
     * Create a new user
     *
     * @param userId                User id
     * @param name                  Name
     * @param email                 Email
     * @param password              Encrypted Password
     * @param organization          Optional organization
     * @param options               Optional options
     * @param sessionId             Optional sessionId.
     * @return                      The created user
     * @throws CatalogException     If user already exists, or unable to create a new user.
     */
    public QueryResult<User> create(String userId, String name, String email, String password, String organization,
                                    QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Gets the user information
     * @param userId                User id
     * @param lastActivity          If lastActivity matches with the one in Catalog, return an empty QueryResult.
     * @param options               QueryOptions
     * @param sessionId             SessionId of the user performing this operation.
     * @return                      The requested user
     * @throws CatalogException
     */
    public QueryResult<User> read(String userId, String lastActivity, QueryOptions options, String sessionId)
            throws CatalogException;

    public void changePassword(String userId, String oldPassword, String newPassword)
            throws CatalogException;


    QueryResult<ObjectMap> login(String userId, String password, String sessionIp)
            throws CatalogException, IOException;

    QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException;

    QueryResult logout(String userId, String sessionId) throws CatalogException;

    QueryResult logoutAnonymous(String sessionId) throws CatalogException;
}
