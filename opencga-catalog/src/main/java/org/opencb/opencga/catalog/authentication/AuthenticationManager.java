package org.opencb.opencga.catalog.authentication;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuthenticationManager {

    /**
     * Authenticate the user against the Authentication server.
     *
     * @param userId            UserId to authenticate
     * @param password          Users password
     * @param throwException    Throw exception if authentication fails
     * @return                  User's authentication
     * @throws CatalogException
     */
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException;

    /**
     * Change users password. Could throw "UnsupportedOperationException" depending
     * if the implementation supports password changes.
     *
     * @param userId            UserId
     * @param oldPassword       Old password
     * @param newPassword       New password
     * @throws CatalogException
     */
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;

    /**
     * Reset the user password. Sets an automatically generated password and sends
     * an email to the user. Throws "UnsupportedOperationException" is the implementation
     * does not support this operation.
     *
     * @param userId            UserId
     * @param email             UserId's email
     * @return
     * @throws CatalogException
     */
    QueryResult resetPassword(String userId, String email) throws CatalogException;

    /**
     * Set a password to a user without a password. Throws "UnsupportedOperationException" is
     * the implementation does not support this operation.
     *
     * @param userId            UserId without password
     * @param newPassword       New password
     * @throws CatalogException
     */
    public void newPassword(String userId, String newPassword) throws CatalogException;
}
