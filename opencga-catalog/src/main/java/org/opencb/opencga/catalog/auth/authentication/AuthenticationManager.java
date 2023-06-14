/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AuthenticationManager {

    protected JwtManager jwtManager;

    private final long expiration;

    protected Logger logger;

    protected int DEFAULT_CONNECTION_TIMEOUT = 500; // In milliseconds
    protected int DEFAULT_READ_TIMEOUT = 1000; // In milliseconds

    AuthenticationManager(long expiration) {
        this.expiration = expiration;

        // Any class extending this one must properly initialise JwtManager
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    Key converStringToKeyObject(String keyString, String jcaAlgorithm) {
        return new SecretKeySpec(keyString.getBytes(), jcaAlgorithm);
    }

    /**
     * Authenticate the user against the Authentication server.
     *
     * @param userId       User to authenticate
     * @param password       Password.
     * @return AuthenticationResponse object.
     * @throws CatalogAuthenticationException CatalogAuthenticationException if any of the credentials are wrong or the access is denied
     * for any other reason.
     */
    public abstract AuthenticationResponse authenticate(String userId, String password) throws CatalogAuthenticationException;

    /**
     * Authenticate the user against the Authentication server.
     *
     * @param refreshToken   Valid refresh token.
     * @return AuthenticationResponse object.
     * @throws CatalogAuthenticationException CatalogAuthenticationException if any of the credentials are wrong or the access is denied
     * for any other reason.
     */
    public abstract AuthenticationResponse refreshToken(String refreshToken) throws CatalogAuthenticationException;

    /**
     * Obtains the userId corresponding to the token.
     *
     * @param token token that have been assigned to a user.
     * @return the user id corresponding to the token given.
     * @throws CatalogAuthenticationException when the token does not correspond to any user or the token has expired.
     */
    public String getUserId(String token) throws CatalogAuthenticationException {
        if (StringUtils.isEmpty(token) || "null".equalsIgnoreCase(token)) {
            return ParamConstants.ANONYMOUS_USER_ID;
        }

        return jwtManager.getUser(token);
    }

    public abstract List<User> getUsersFromRemoteGroup(String group) throws CatalogException;

    public abstract List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException;

    public abstract List<String> getRemoteGroups(String token) throws CatalogException;

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
     * @return OpenCGAResult OpenCGAResult
     * @throws CatalogException CatalogException
     */
    public abstract OpenCGAResult resetPassword(String userId) throws CatalogException;

    /**
     * Set a password to a user without a password.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param userId      UserId without password
     * @param newPassword New password
     * @throws CatalogException CatalogException
     */
    public abstract void newPassword(String userId, String newPassword) throws CatalogException;

    /**
     * Create a token for the user with default expiration time.
     *
     * @param userId user.
     * @return A token.
     */
    public String createToken(String userId) {
        return createToken(userId, Collections.emptyMap(), expiration);
    }

    /**
     * Create a token for the user with default expiration time.
     *
     * @param userId user.
     * @param expiration expiration time.
     * @return A token.
     */
    public String createToken(String userId, long expiration) {
        return createToken(userId, Collections.emptyMap(), expiration);
    }

    /**
     * Create a token for the user with default expiration time.
     *
     * @param userId user.
     * @param claims claims.
     * @return A token.
     */
    public String createToken(String userId, Map<String, Object> claims) {
        return createToken(userId, claims, expiration);
    }

    /**
     * Create a token for the user.
     *
     * @param userId user.
     * @param claims claims.
     * @param expiration Expiration time in seconds.
     * @return A token.
     */
    public abstract String createToken(String userId, Map<String, Object> claims, long expiration);

    /**
     * Create a token for the user with no expiration time.
     *
     * @param userId user.
     * @return A token.
     */
    public String createNonExpiringToken(String userId) {
        return createNonExpiringToken(userId, Collections.emptyMap());
    }

    /**
     * Create a token for the user with no expiration time.
     *
     * @param userId user.
     * @param claims claims.
     * @return A token.
     */
    public abstract String createNonExpiringToken(String userId, Map<String, Object> claims);

    public Date getExpirationDate(String token) throws CatalogAuthenticationException {
        return jwtManager.getExpiration(token);
    }

}
