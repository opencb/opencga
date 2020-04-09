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

import io.jsonwebtoken.SignatureAlgorithm;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.MailUtils;
import org.opencb.opencga.core.config.Email;
import org.opencb.opencga.core.models.AuthenticationResponse;
import org.opencb.opencga.core.models.User;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthenticationManager extends AuthenticationManager {

    public static final String INTERNAL = "internal";
    private final Email emailConfig;

    private final UserDBAdaptor userDBAdaptor;
    private final MetaDBAdaptor metaDBAdaptor;

    private long expiration;

    public CatalogAuthenticationManager(DBAdaptorFactory dbAdaptorFactory, Email emailConfig, String secretKeyString, long expiration) {
        super();

        this.emailConfig = emailConfig;
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
        this.metaDBAdaptor = dbAdaptorFactory.getCatalogMetaDBAdaptor();

        this.expiration = expiration;

        Key secretKey = this.converStringToKeyObject(secretKeyString, SignatureAlgorithm.HS256.getJcaName());
        this.jwtManager = new JwtManager(SignatureAlgorithm.HS256.getValue(), secretKey);

        this.logger = LoggerFactory.getLogger(CatalogAuthenticationManager.class);
    }

    public static String cypherPassword(String password) throws CatalogException {
        if (password.matches("^[a-fA-F0-9]{40}$")) {
            // Password already cyphered
            return password;
        }

        try {
            return StringUtils.sha1(password);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogDBException("Could not encode password", e);
        }
    }

    @Override
    public AuthenticationResponse authenticate(String username, String password) throws CatalogAuthenticationException {
        String cypherPassword;
        try {
            cypherPassword = cypherPassword(password);
        } catch (CatalogException e) {
            throw new CatalogAuthenticationException(e.getMessage(), e);
        }

        String storedPassword;
        boolean validSessionId = false;
        if (username.equals("admin")) {
            try {
                storedPassword = metaDBAdaptor.getAdminPassword();
            } catch (CatalogDBException e) {
                throw new CatalogAuthenticationException("Could not validate 'admin' password\n" + e.getMessage(), e);
            }
            validSessionId = storedPassword.equals(cypherPassword);
        } else {
            try {
                storedPassword = userDBAdaptor.get(username, new QueryOptions(QueryOptions.INCLUDE, "password"), null)
                        .first().getPassword();
            } catch (CatalogDBException e) {
                throw new CatalogAuthenticationException("Could not validate '" + username + "' password\n" + e.getMessage(), e);
            }
        }
        if (storedPassword.equals(cypherPassword) || validSessionId) {
            return new AuthenticationResponse(jwtManager.createJWTToken(username, expiration));
        } else {
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        }
    }

    @Override
    public AuthenticationResponse refreshToken(String username, String token) throws CatalogAuthenticationException {
        if (!username.equals(getUserId(token))) {
            throw new CatalogAuthenticationException("Cannot refresh token. The token received does not correspond to " + username);
        }
        return new AuthenticationResponse(createToken(username));
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
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        String oldCryptPass = (oldPassword.length() != 40) ? cypherPassword(oldPassword) : oldPassword;
        String newCryptPass = (newPassword.length() != 40) ? cypherPassword(newPassword) : newPassword;
        userDBAdaptor.changePassword(userId, oldCryptPass, newCryptPass);
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        String newCryptPass = (newPassword.length() != 40) ? cypherPassword(newPassword) : newPassword;
        userDBAdaptor.changePassword(userId, "", newCryptPass);
    }

    @Override
    public String createToken(String userId) {
        return jwtManager.createJWTToken(userId, expiration);
    }

    @Override
    public QueryResult resetPassword(String userId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        userDBAdaptor.updateUserLastModified(userId);

        String newPassword = StringUtils.randomString(6);

        String newCryptPass = cypherPassword(newPassword);

        QueryResult<User> user =
                userDBAdaptor.get(userId, new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.EMAIL.key()), "");

        if (user == null || user.getNumResults() != 1) {
            throw new CatalogException("Could not retrieve the user e-mail.");
        }

        String email = user.first().getEmail();

        QueryResult queryResult = userDBAdaptor.resetPassword(userId, email, newCryptPass);

        String mailUser = this.emailConfig.getFrom();
        String mailPassword = this.emailConfig.getPassword();
        String mailHost = this.emailConfig.getHost();
        String mailPort = this.emailConfig.getPort();

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return queryResult;
    }
}
