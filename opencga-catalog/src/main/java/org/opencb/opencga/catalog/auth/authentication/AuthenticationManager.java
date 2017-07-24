/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.auth.authentication;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.session.JwtSessionManager;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AuthenticationManager {

    protected Configuration configuration;
    protected JwtSessionManager jwtSessionManager;

    AuthenticationManager(Configuration configuration) {
        this.configuration = configuration;
        this.jwtSessionManager = new JwtSessionManager(configuration);
    }

    /**
     * Authenticate the user against the Authentication server.
     *
     * @param userId         UserId to authenticate
     * @param password       Users password or sessionId.
     * @param throwException Throw exception if authentication fails
     * @return User's authentication
     * @throws CatalogException CatalogException
     */
    public abstract boolean authenticate(String userId, String password, boolean throwException) throws CatalogException;

    /**
     * Obtains the userId corresponding to the token.
     *
     * @param token token that have been assigned to a user.
     * @return the user id corresponding to the token given.
     * @throws CatalogException when the token does not correspond to any user or the token has expired.
     */
    public String getUserId(String token) throws CatalogException {
        if (token == null || token.isEmpty() || token.equalsIgnoreCase("null")) {
            return "*";
        }

        return jwtSessionManager.getUserId(token);
    }

    /**
     * Change users password. Could throw "UnsupportedOperationException" depending if the implementation supports password changes.
     *
     * @param userId      UserId
     * @param oldPassword Old password
     * @param newPassword New password
     * @throws CatalogException CatalogException
     */
    public abstract void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;

    /**
     * Reset the user password. Sets an automatically generated password and sends an email to the user.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param userId UserId
     * @return QueryResult QueryResult
     * @throws CatalogException CatalogException
     */
    public abstract QueryResult resetPassword(String userId) throws CatalogException;

    /**
     * Set a password to a user without a password.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param userId      UserId without password
     * @param newPassword New password
     * @throws CatalogException CatalogException
     */
    public abstract void newPassword(String userId, String newPassword) throws CatalogException;

    public QueryResult<Session> createToken(String userId, String ip, Session.Type type) {
        return jwtSessionManager.createToken(userId, ip, type);
    }
}
