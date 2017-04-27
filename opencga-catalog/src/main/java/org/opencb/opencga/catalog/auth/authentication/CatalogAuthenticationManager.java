/*
 * Copyright 2015-2016 OpenCB
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

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.MailUtils;

import java.security.NoSuchAlgorithmException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthenticationManager implements AuthenticationManager {

    protected final UserDBAdaptor userDBAdaptor;
    protected final MetaDBAdaptor metaDBAdaptor;
    protected final Configuration configuration;

    public CatalogAuthenticationManager(DBAdaptorFactory dbAdaptorFactory, Configuration configuration) {
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
        this.metaDBAdaptor = dbAdaptorFactory.getCatalogMetaDBAdaptor();
        this.configuration = configuration;
    }

    public static String cypherPassword(String password) throws CatalogException {
        try {
            return StringUtils.sha1(password);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogDBException("Could not encode password", e);
        }
    }

    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        String cypherPassword = cypherPassword(password);
        String storedPassword;
        boolean validSessionId = false;
        if (userId.equals("admin")) {
            storedPassword = metaDBAdaptor.getAdminPassword();
            validSessionId = metaDBAdaptor.checkValidAdminSession(password);
        } else {
            storedPassword = userDBAdaptor.get(userId, new QueryOptions(QueryOptions.INCLUDE, "password"), null).first().getPassword();
            QueryResult<Session> session = userDBAdaptor.getSession(userId, password);
            if (session.getNumResults() > 0) {
                validSessionId = true;
            }
        }
        if (storedPassword.equals(cypherPassword) || validSessionId) {
            return true;
        } else {
            if (throwException) {
                throw new CatalogException("Bad user or password");
            } else {
                return false;
            }
        }
    }

    @Override
    public String getUserId(String token) throws CatalogException {
        if (token == null || token.isEmpty() || token.equalsIgnoreCase("null")) {
            return "anonymous";
        }

        // Check admin
        if (token.length() == 40) {
            // TODO: Replace the dbAdaptor method to return the whole session structure to check if it has expired.
            if (metaDBAdaptor.checkValidAdminSession(token)) {
                return "admin";
            }
            throw new CatalogException("The session id does not correspond to any user.");
        }

        // Check user
        if (token.length() == 20) {
            // TODO: Replace the dbAdaptor method to return the whole session structure to check if it has expired.
            String userId = userDBAdaptor.getUserIdBySessionId(token);
            if (userId.isEmpty()) {
                throw new CatalogException("The session id does not correspond to any user.");
            }
            return userId;
        }

        throw new CatalogException("The session id introduced is not correct.");
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
    public QueryResult resetPassword(String userId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        userDBAdaptor.updateUserLastModified(userId);

        String newPassword = StringUtils.randomString(6);

        String newCryptPass = cypherPassword(newPassword);

        QueryResult<User> user =
                userDBAdaptor.get(userId, new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.EMAIL.key()), "");

        if (user == null && user.getNumResults() != 1) {
            throw new CatalogException("Could not retrieve the user e-mail.");
        }

        String email = user.first().getEmail();

        QueryResult qr = userDBAdaptor.resetPassword(userId, email, newCryptPass);

        /*
        String mailUser = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_USER);
        String mailPassword = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PASSWORD);
        String mailHost = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_HOST);
        String mailPort = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PORT);
*/
        String mailUser = configuration.getEmail().getFrom();
        String mailPassword = configuration.getEmail().getPassword();
        String mailHost = configuration.getEmail().getHost();
        String mailPort = configuration.getEmail().getPort();

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return qr;
    }

}
