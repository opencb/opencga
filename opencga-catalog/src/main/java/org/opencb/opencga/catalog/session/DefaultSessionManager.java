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

package org.opencb.opencga.catalog.session;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogTokenException;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by pfurio on 24/05/16.
 */
public class DefaultSessionManager implements SessionManager {

    private final UserDBAdaptor userDBAdaptor;
    private final MetaDBAdaptor metaDBAdaptor;

    private final int USER_SESSION_LENGTH = 20;
    private final int ADMIN_SESSION_LENGTH = 40;

    public DefaultSessionManager(DBAdaptorFactory dbAdaptorFactory) {
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
        this.metaDBAdaptor = dbAdaptorFactory.getCatalogMetaDBAdaptor();
    }

    @Override
    public QueryResult<Session> createToken(String userId, String ip, Session.Type type) throws CatalogException {
        int length = USER_SESSION_LENGTH;
        if (userId.equals("admin")) {
            length = ADMIN_SESSION_LENGTH;
        }

        // Create the session
        Session session = new Session(StringUtils.randomString(length), ip, TimeUtils.getTime(), type);
        while (true) {
            if (length == USER_SESSION_LENGTH) {
                if (userDBAdaptor.getUserIdBySessionId(session.getId()).isEmpty()) {
                    break;
                }
            } else {
                if (!metaDBAdaptor.checkValidAdminSession(session.getId())) {
                    break;
                }
            }
            session.generateNewId(length);
        }

        QueryResult<Session> result;
        // Add the session to the user
        if (userId.equals("admin")) {
            result = metaDBAdaptor.addAdminSession(session);
        } else {
            result = userDBAdaptor.addSession(userId, session);
        }

        return result;
    }

    @Override
    public void checkAdminSession(String sessionId) throws CatalogException {
        if (!metaDBAdaptor.checkValidAdminSession(sessionId)) {
            throw new CatalogException("The admin session id is not valid.");
        }
    }

    @Override
    public Jws<Claims> parseClaims(String jwtToken) throws CatalogTokenException {
        return null;
    }

    @Override
    public String getUserId(String jwtToken) throws CatalogTokenException {
        return null;
    }

    @Override
    public void clearToken(String userId, String sessionId) throws CatalogException {

    }


}
