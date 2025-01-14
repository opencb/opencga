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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.Closeable;
import java.security.Key;
import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AuthenticationManager implements Closeable {

    protected JwtManager jwtManager;

    protected final DBAdaptorFactory dbAdaptorFactory;
    private final long expiration;

    protected Logger logger;

    protected int DEFAULT_CONNECTION_TIMEOUT = 500; // In milliseconds
    protected int DEFAULT_READ_TIMEOUT = 1000; // In milliseconds

    AuthenticationManager(DBAdaptorFactory dbAdaptorFactory, long expiration) {
        this.dbAdaptorFactory = dbAdaptorFactory;
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
     * @param organizationId Organization id.
     * @param userId         User to authenticate
     * @param password       Password.
     * @return AuthenticationResponse object.
     * @throws CatalogAuthenticationException CatalogAuthenticationException if any of the credentials are wrong or the access is denied
     *                                        for any other reason.
     */
    public abstract AuthenticationResponse authenticate(String organizationId, String userId, String password)
            throws CatalogAuthenticationException;

    /**
     * Authenticate the user against the Authentication server.
     *
     * @param refreshToken   Valid refresh token.
     * @return AuthenticationResponse object.
     * @throws CatalogAuthenticationException CatalogAuthenticationException if any of the credentials are wrong or the access is denied
     * for any other reason.
     */
    public AuthenticationResponse refreshToken(String refreshToken) throws CatalogAuthenticationException {
        JwtPayload payload = getPayload(refreshToken);
        if (!ParamConstants.ANONYMOUS_USER_ID.equals(payload.getUserId())) {
            return new AuthenticationResponse(createToken(payload.getOrganization(), payload.getUserId()));
        } else {
            throw new CatalogAuthenticationException("Cannot refresh token for '" + ParamConstants.ANONYMOUS_USER_ID + "'.");
        }
    }

    /**
     * Obtains the userId corresponding to the token.
     *
     * @param token token that have been assigned to a user.
     * @return the user id corresponding to the token given.
     * @throws CatalogAuthenticationException when the token does not correspond to any user or the token has expired.
     */
    public String getUserId(String token) throws CatalogAuthenticationException {
        if (StringUtils.isEmpty(token) || "null".equalsIgnoreCase(token)) {
            throw new CatalogAuthenticationException("Token is null or empty.");
        }

        return jwtManager.getUser(token);
    }

    /**
     * Obtains the userId corresponding to the token.
     *
     * @param token token that have been assigned to a user.
     * @return the user id corresponding to the token given.
     * @throws CatalogAuthenticationException when the token does not correspond to any user or the token has expired.
     */
    public JwtPayload getPayload(String token) throws CatalogAuthenticationException {
        if (StringUtils.isEmpty(token) || "null".equalsIgnoreCase(token)) {
            throw new CatalogAuthenticationException("Token is null or empty.");
        }

        return jwtManager.getPayload(token);
    }

    public abstract List<User> getUsersFromRemoteGroup(String group) throws CatalogException;

    public abstract List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException;

    public abstract List<String> getRemoteGroups(String token) throws CatalogException;

    /**
     * Change users password. Could throw "UnsupportedOperationException" depending if the implementation supports password changes.
     *
     * @param organizationId Organization id.
     * @param userId         UserId
     * @param oldPassword    Old password
     * @param newPassword    New password
     * @throws CatalogException CatalogException
     */
    public abstract void changePassword(String organizationId, String userId, String oldPassword, String newPassword)
            throws CatalogException;

    /**
     * Reset the user password. Sets an automatically generated password and sends an email to the user.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param organizationId Organization id.
     * @param userId         UserId
     * @return OpenCGAResult OpenCGAResult
     * @throws CatalogException CatalogException
     */
    public abstract OpenCGAResult resetPassword(String organizationId, String userId) throws CatalogException;

    /**
     * Set a password to a user without a password.
     * Throws "UnsupportedOperationException" is the implementation does not support this operation.
     *
     * @param organizationId Organization id.
     * @param userId         UserId without password
     * @param newPassword    New password
     * @throws CatalogException CatalogException
     */
    public abstract void newPassword(String organizationId, String userId, String newPassword) throws CatalogException;

    /**
     * Create a token for the user with default expiration time.
     *
     * @param organizationId Organization id.
     * @param userId         user.
     * @throws CatalogAuthenticationException CatalogAuthenticationException
     * @return A token.
     */
    public String createToken(String organizationId, String userId) throws CatalogAuthenticationException {
        return createToken(organizationId, userId, Collections.emptyMap(), expiration);
    }

    /**
     * Create a token for the user with default expiration time.
     *
     * @param organizationId Organization id.
     * @param userId         user.
     * @param expiration     expiration time.
     * @throws CatalogAuthenticationException CatalogAuthenticationException
     * @return A token.
     */
    public String createToken(String organizationId, String userId, long expiration) throws CatalogAuthenticationException {
        return createToken(organizationId, userId, Collections.emptyMap(), expiration);
    }

    /**
     * Create a token for the user with default expiration time.
     *
     * @param organizationId Organization id.
     * @param userId         user.
     * @param claims         claims.
     * @throws CatalogAuthenticationException CatalogAuthenticationException
     * @return A token.
     */
    public String createToken(String organizationId, String userId, Map<String, Object> claims) throws CatalogAuthenticationException {
        return createToken(organizationId, userId, claims, expiration);
    }

    /**
     * Create a token for the user.
     *
     * @param organizationId Organization id.
     * @param userId         user.
     * @param claims         claims.
     * @param expiration     Expiration time in seconds.
     * @throws CatalogAuthenticationException CatalogAuthenticationException
     * @return A token.
     */
    public abstract String createToken(String organizationId, String userId, Map<String, Object> claims, long expiration)
            throws CatalogAuthenticationException;

    /**
     * Create a token for the user with no expiration time.
     *
     * @param organizationId Organization id.
     * @param userId         user.
     * @param claims         claims.
     * @throws CatalogAuthenticationException CatalogAuthenticationException
     * @return A token.
     */
    public abstract String createNonExpiringToken(String organizationId, String userId, Map<String, Object> claims)
            throws CatalogAuthenticationException;

    public Date getExpirationDate(String token) throws CatalogAuthenticationException {
        return jwtManager.getExpiration(token);
    }

    protected List<JwtPayload.FederationJwtPayload> getFederations(String organizationId, String userId)
            throws CatalogAuthenticationException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.INTERNAL_FEDERATED.key(), true);
        OpenCGAResult<Project> result;
        try {
             result = dbAdaptorFactory.getCatalogProjectDbAdaptor(organizationId).get(query, QueryOptions.empty(), userId);
        } catch (Exception e) {
            throw new CatalogAuthenticationException("Could not obtain federated projects for user " + userId, e);
        }
        if (result.getNumResults() == 0) {
            return Collections.emptyList();
        }

        // Build the federations list
        Map<String, JwtPayload.FederationJwtPayload> federationMap = new HashMap<>();
        for (Project project : result.getResults()) {
            federationMap.putIfAbsent(project.getFederation().getId(), new JwtPayload.FederationJwtPayload(project.getFederation().getId(),
                    new LinkedList<>(), new LinkedList<>()));
            JwtPayload.FederationJwtPayload federation = federationMap.get(project.getFederation().getId());
            federation.getProjectIds().add(project.getFqn());
            federation.getProjectIds().add(project.getUuid());
            for (Study study : project.getStudies()) {
                federation.getStudyIds().add(study.getFqn());
                federation.getStudyIds().add(study.getUuid());
            }
        }
        return new ArrayList<>(federationMap.values());
    }

}
