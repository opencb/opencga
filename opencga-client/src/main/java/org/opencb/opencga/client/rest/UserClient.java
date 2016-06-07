/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 04/05/16.
 */
public class UserClient extends AbstractParentClient<User> {

    private static final String USERS_URL = "users";

    UserClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);

        this.category = USERS_URL;
        this.clazz = User.class;
    }

    QueryResponse<ObjectMap> login(String user, String password) {
        QueryResponse<ObjectMap> response = null;
        try {
            response = execute(USERS_URL, user, "login", createParamsMap("password", password), ObjectMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    QueryResponse<ObjectMap> logout(String password) {
        QueryResponse<ObjectMap> response = null;
        try {
            response = execute(USERS_URL, null, "logout", null, ObjectMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public QueryResponse<Project> getProjects(String userId, QueryOptions options) throws CatalogException, IOException {
        return execute(USERS_URL, userId, "projects", options, Project.class);
    }

    public QueryResponse<User> changePassword(String userId, String currentPassword, String newPassword, ObjectMap params) throws CatalogException, IOException {
        addParamsToObjectMap(params, "password", currentPassword, "npassword", newPassword);
        return execute(USERS_URL, userId, "change-password", params, User.class);
    }

    public QueryResponse<User> resetPassword(String userId, String email, ObjectMap params) throws CatalogException, IOException {
        addParamsToObjectMap(params, "email", email);
        return execute(USERS_URL, userId, "change-password", params, User.class);
    }

    public QueryResponse<User> update(String userId, ObjectMap params) throws CatalogException, IOException {
        return execute(USERS_URL, userId, "update", params, User.class);
    }

}
