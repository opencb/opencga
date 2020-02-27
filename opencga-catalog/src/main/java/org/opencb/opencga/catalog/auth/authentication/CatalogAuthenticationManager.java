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
import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.MailUtils;
import org.opencb.opencga.core.config.Email;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthenticationManager extends AuthenticationManager {

    public static final String INTERNAL = "internal";
    private final Email emailConfig;

    private final UserDBAdaptor userDBAdaptor;

    private long expiration;

    public CatalogAuthenticationManager(DBAdaptorFactory dbAdaptorFactory, Email emailConfig, String secretKeyString, long expiration) {
        super();

        this.emailConfig = emailConfig;
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();

        this.expiration = expiration;

        Key secretKey = this.converStringToKeyObject(secretKeyString, SignatureAlgorithm.HS256.getJcaName());
        this.jwtManager = new JwtManager(SignatureAlgorithm.HS256.getValue(), secretKey);

        this.logger = LoggerFactory.getLogger(CatalogAuthenticationManager.class);
    }

    @Override
    public String authenticate(String username, String password) throws CatalogAuthenticationException {
        try {
            userDBAdaptor.authenticate(username, password);
            return jwtManager.createJWTToken(username, expiration);
        } catch (CatalogDBException e) {
            throw new CatalogAuthenticationException("Could not validate '" + username + "' password\n" + e.getMessage(), e);
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
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        userDBAdaptor.changePassword(userId, oldPassword, newPassword);
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        userDBAdaptor.changePassword(userId, "", newPassword);
    }

    @Override
    public String createToken(String userId) {
        return jwtManager.createJWTToken(userId, expiration);
    }

    @Override
    public OpenCGAResult resetPassword(String userId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");

        String newPassword = RandomStringUtils.randomAlphanumeric(12);

        OpenCGAResult<User> user =
                userDBAdaptor.get(userId, new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.EMAIL.key()));

        if (user == null || user.getNumResults() != 1) {
            throw new CatalogException("Could not retrieve the user e-mail.");
        }

        String email = user.first().getEmail();

        OpenCGAResult result = userDBAdaptor.resetPassword(userId, email, newPassword);

        String mailUser = this.emailConfig.getFrom();
        String mailPassword = this.emailConfig.getPassword();
        String mailHost = this.emailConfig.getHost();
        String mailPort = this.emailConfig.getPort();

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return result;
    }
}
