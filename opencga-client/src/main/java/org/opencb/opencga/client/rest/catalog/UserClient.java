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

package org.opencb.opencga.client.rest.catalog;

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.User;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Created by imedina on 04/05/16.
 */
public class UserClient extends CatalogClient<User, User> {

    private static final String USERS_URL = "users";

    public UserClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = USERS_URL;
        this.clazz = User.class;
    }

    public DataResponse<User> create(String user, String password, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "id", user, "password", password);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        return execute(USERS_URL, "create", p, POST, User.class);
    }

    public DataResponse<ObjectMap> login(String user, String password) {
        DataResponse<ObjectMap> response = null;
        ObjectMap p = new ObjectMap("password", password);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(p);
            ObjectMap objectMap = new ObjectMap("body", json);
            response = execute(USERS_URL, user, "login", objectMap, POST, ObjectMap.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return response;
    }

    public DataResponse<ObjectMap> refresh() throws ClientException {
        DataResponse<ObjectMap> response;
        ObjectMap p = new ObjectMap();
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(p);
            ObjectMap objectMap = new ObjectMap("body", json);
            response = execute(USERS_URL, this.getUserId(null), "login", objectMap, POST, ObjectMap.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return response;
    }

    public DataResponse<User> get(QueryOptions options) throws IOException, ClientException {
        return super.get(getUserId(options), options);
    }

    public DataResponse<Project> getProjects(QueryOptions options) throws IOException, ClientException {
        String userId = getUserId(options);
        return execute(USERS_URL, userId, "projects", options, GET, Project.class);
    }

    public DataResponse<User> changePassword(String currentPassword, String newPassword, ObjectMap params)
            throws ClientException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        params = addParamsToObjectMap(params, "password", currentPassword, "newPassword", newPassword);
        String json = mapper.writeValueAsString(params);
        ObjectMap objectMap = new ObjectMap("body", json);
        return execute(USERS_URL, getUserId(params), "password", objectMap, POST, User.class);
    }

    public DataResponse<User> resetPassword(ObjectMap params) throws ClientException, IOException {
        return execute(USERS_URL, getUserId(params), "change-password", params, GET, User.class);
    }
}
