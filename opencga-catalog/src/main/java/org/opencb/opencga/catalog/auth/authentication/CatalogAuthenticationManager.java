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

import io.jsonwebtoken.SignatureAlgorithm;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.MailUtils;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Email;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthenticationManager extends AuthenticationManager {

    // TODO: Remove INTERNAL field and its usages after several releases (TASK-5923)
    @Deprecated
    public static final String INTERNAL = "internal";
    public static final String OPENCGA = "OPENCGA";
    private final Email emailConfig;

    public CatalogAuthenticationManager(DBAdaptorFactory dbAdaptorFactory, Email emailConfig, String algorithm, String secretKeyString,
                                        long expiration) {
        super(dbAdaptorFactory, expiration);

        this.emailConfig = emailConfig;

        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.valueOf(algorithm);
        Key secretKey = this.converStringToKeyObject(secretKeyString, signatureAlgorithm.getJcaName());
        this.jwtManager = new JwtManager(signatureAlgorithm.getValue(), secretKey);

        this.logger = LoggerFactory.getLogger(CatalogAuthenticationManager.class);
    }

    public static void validateAuthenticationOriginConfiguration(AuthenticationOrigin authenticationOrigin) throws CatalogException {
        if (!OPENCGA.equals(authenticationOrigin.getId())) {
            throw new CatalogException("Unknown authentication origin. Expected origin id '" + OPENCGA + "' but received '"
                    + authenticationOrigin.getId() + "'.");
        }
        if (authenticationOrigin.getType() != AuthenticationOrigin.AuthenticationType.OPENCGA) {
            throw new CatalogException("Unknown authentication type. Expected type '" + AuthenticationOrigin.AuthenticationType.OPENCGA
                    + "' but received '" + authenticationOrigin.getType() + "'.");
        }
    }

    @Override
    public AuthenticationOrigin.AuthenticationType getAuthenticationType() {
        return AuthenticationOrigin.AuthenticationType.OPENCGA;
    }

    @Override
    public AuthenticationResponse authenticate(String organizationId, String userId, String password)
            throws CatalogAuthenticationException {
        try {
            dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).authenticate(userId, password);
            return new AuthenticationResponse(createToken(organizationId, userId));
        } catch (CatalogDBException e) {
            throw new CatalogAuthenticationException("Could not validate '" + userId + "' password\n" + e.getMessage(), e);
        }
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changePassword(String organizationId, String userId, String oldPassword, String newPassword) throws CatalogException {
        dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).changePassword(userId, oldPassword, newPassword);
    }

    @Override
    public void newPassword(String organizationId, String userId, String newPassword) throws CatalogException {
        dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).changePassword(userId, "", newPassword);
    }

    @Override
    public String createToken(String organizationId, String userId, Map<String, Object> claims, long expiration)
            throws CatalogAuthenticationException {
        List<JwtPayload.FederationJwtPayload> federations = getFederations(organizationId, userId);
        return jwtManager.createJWTToken(organizationId, AuthenticationOrigin.AuthenticationType.OPENCGA, userId, claims, federations,
                expiration);
    }

    @Override
    public String createNonExpiringToken(String organizationId, String userId, Map<String, Object> claims)
            throws CatalogAuthenticationException {
        List<JwtPayload.FederationJwtPayload> federations = getFederations(organizationId, userId);
        return jwtManager.createJWTToken(organizationId, AuthenticationOrigin.AuthenticationType.OPENCGA, userId, claims, federations, 0L);
    }

    @Override
    public OpenCGAResult resetPassword(String organizationId, String userId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        OpenCGAResult result = null;
        String newPassword = PasswordUtils.getStrongRandomPassword();

        OpenCGAResult<User> user =
                dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId)
                        .get(userId, new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.EMAIL.key()));

        if (user == null || user.getNumResults() != 1) {
            throw new CatalogException("Could not retrieve the user e-mail.");
        }

        try {
            String email = user.first().getEmail();
            String resetMailContent = MailUtils.getResetMailContent(userId, newPassword);
            MailUtils.configure(this.emailConfig).sendMail(email, "XetaBase: Password Reset", resetMailContent);
            result = dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).resetPassword(userId, email, newPassword);
        } catch (Exception e) {
            throw new CatalogException("Email could not be sent.", e);
        }

        return result;
    }

    public static AuthenticationOrigin createOpencgaAuthenticationOrigin() {
        return new AuthenticationOrigin()
                .setId(CatalogAuthenticationManager.OPENCGA)
                .setType(AuthenticationOrigin.AuthenticationType.OPENCGA);
    }

    @Override
    public void close() {
    }
}
