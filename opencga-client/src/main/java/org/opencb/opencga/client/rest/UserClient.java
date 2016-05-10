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

import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 04/05/16.
 */
public class UserClient extends AbstractParentClient {

    private static final String USERS_URL = "users";

    UserClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);
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

    public QueryResponse<User> get(String userId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<User> user = execute(USERS_URL, userId, "info", options, User.class);
        return user;
    }

    public QueryResponse<Project> getProjects(String userId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Project> projects = execute(USERS_URL, userId, "projects", options, Project.class);
        return projects;
    }

}
