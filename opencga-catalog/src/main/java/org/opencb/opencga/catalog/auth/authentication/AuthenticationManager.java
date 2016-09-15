package org.opencb.opencga.catalog.auth.authentication;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuthenticationManager {

    /**
     * Authenticate the user against the Authentication server.
     *
     * @param userId         UserId to authenticate
     * @param password       Users password or sessionId.
     * @param throwException Throw exception if authentication fails
     * @return User's authentication
     * @throws CatalogException CatalogException
     */
    boolean authenticate(String userId, String password, boolean throwException) throws CatalogException;

    /**
     * Obtains the userId corresponding to the token.
     *
     * @param token token that have been assigned to a user.
     * @return the user id corresponding to the token given.
     * @throws CatalogException when the token does not correspond to any user or the token has expired.
     */
    String getUserId(String token) throws CatalogException;

    /**
     * Change users password. Could throw "UnsupportedOperationException" depending if the implementation supports password changes.
     *
     * @param userId      UserId
     * @param oldPassword Old password
     * @param newPassword New password
     * @throws CatalogException CatalogException
     */
    void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;

    /**
     * Reset the user password. Sets an automatically generated password and sends an email to the user.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param userId UserId
     * @return QueryResult QueryResult
     * @throws CatalogException CatalogException
     */
    QueryResult resetPassword(String userId) throws CatalogException;

    /**
     * Set a password to a user without a password.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param userId      UserId without password
     * @param newPassword New password
     * @throws CatalogException CatalogException
     */
    void newPassword(String userId, String newPassword) throws CatalogException;
}
