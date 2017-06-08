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
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogTokenException;
import org.opencb.opencga.catalog.models.Session;

/**
 * Created by pfurio on 24/05/16.
 */
public interface SessionManager {

    /**
     * Generates a unique and valid session id for the user.
     *
     * @param userId valid user id.
     * @param ip current ip of the user.
     * @param type type of the session to be generated.
     * @return A queryResult object containing the generated session id.
     * @throws CatalogException if the user is not valid.
     */
    QueryResult<Session> createToken(String userId, String ip, Session.Type type) throws CatalogException;

//    QueryResult<Session> createToken(String userId, String ip, Session.Type type) throws CatalogException;

    /**
     * Closes the session.
     *
     * @param userId user id.
     * @param sessionId session id.
     * @throws CatalogException when the user id or the session id are not valid.
     */
    void clearToken(String userId, String sessionId) throws CatalogException;

    /**
     * Checks if the session id is a valid admin session id.
     *
     * @param sessionId session id.
     * @throws CatalogException when the session id is not valid.
     */
    void checkAdminSession(String sessionId) throws CatalogException;

    Jws<Claims> parseClaims(String jwtToken) throws CatalogTokenException;

    String getUserId(String jwtToken) throws CatalogTokenException;


}
