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

import org.codehaus.jackson.map.ObjectMapper;
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
public class UserClient extends AbstractParentClient<User, User> {

    private static final String USERS_URL = "users";

    UserClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = USERS_URL;
        this.clazz = User.class;
    }

    /*public QueryResponse<User> create(String user, String password, ObjectMap params)throws IOException {
        //TODO param: method for GET o POST
        QueryResponse<User> response = null;
        if (params.containsKey("method") && params.get("method").equals("GET")) {
            params = addParamsToObjectMap(params, "userId", user, "password", password);
            try {
                response = execute(USERS_URL, "create", params, GET, User.class);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }else {
            params = addParamsToObjectMap(params, "userId", user, "password", password);
            ObjectMapper mapper = new ObjectMapper();
            try {
                String json = mapper.writeValueAsString(params);
                ObjectMap p = new ObjectMap("body", json);
                response = execute(USERS_URL, "create", p, POST, User.class);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        return response;
    }*/
    public QueryResponse<User> create(String user, String password, ObjectMap params)throws IOException {
        //TODO TEST
        if (params.containsKey("method") && params.get("method").equals("GET")) {
            params = addParamsToObjectMap(params, "userId", user, "password", password);
                return execute(USERS_URL, "create", params, GET, User.class);
        }
        params = addParamsToObjectMap(params, "userId", user, "password", password);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        return execute(USERS_URL, "create", p, POST, User.class);
    }
    /*
    Deprecated
    QueryResponse<ObjectMap> login(String user, String password) {
        QueryResponse<ObjectMap> response = null;
        try {
            response = execute(USERS_URL, user, "login", createParamsMap("password", password), GET, ObjectMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }*/
    QueryResponse<ObjectMap> login(String user, String password) {
        QueryResponse<ObjectMap> response = null;
        ObjectMap p = new ObjectMap("password", password);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(p);
            ObjectMap objectMap = new ObjectMap("body", json);
            response = execute(USERS_URL, user, "login", objectMap, POST, ObjectMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    QueryResponse<ObjectMap> logout() {
        QueryResponse<ObjectMap> response = null;
        try {
            response = execute(USERS_URL, getUserId(), "logout", null, GET, ObjectMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public QueryResponse<User> get(QueryOptions options) throws CatalogException, IOException {
        return super.get(getUserId(), options);
    }

    public QueryResponse<Project> getProjects(QueryOptions options) throws CatalogException, IOException {
        return execute(USERS_URL, getUserId(), "projects", options, GET, Project.class);
    }
    /*Deprecated
    public QueryResponse<User> changePassword(String currentPassword, String newPassword, ObjectMap params)
            throws CatalogException, IOException {

        params = addParamsToObjectMap(params, "password", currentPassword, "npassword", newPassword);
        QueryResponse<User> execute = execute(USERS_URL, getUserId(), "change-password", params, POST, User.class);
        if (!execute.getError().isEmpty()) {
            throw new CatalogException(execute.getError());
        }
        return execute;
    }
    */
    public QueryResponse<User> changePassword(String currentPassword, String newPassword, ObjectMap params)
            throws CatalogException, IOException {
        if (params.containsKey("method") && params.get("method").equals("GET")) {
            params = addParamsToObjectMap(params, "password", currentPassword, "npassword", newPassword);
            return execute(USERS_URL, getUserId(), "change-password", params, GET, User.class);
        }
        ObjectMapper mapper = new ObjectMapper();
        params = addParamsToObjectMap(params, "password", currentPassword, "npassword", newPassword);
        String json = mapper.writeValueAsString(params);
        ObjectMap objectMap = new ObjectMap("body", json);
        return execute(USERS_URL, getUserId(), "change-password", objectMap, POST, User.class);
    }
    public QueryResponse<User> resetPassword(ObjectMap params) throws CatalogException, IOException {
        return execute(USERS_URL, getUserId(), "change-password", params, GET, User.class);
    }



//    public QueryResponse<User> delete(ObjectMap params) throws CatalogException, IOException {
//        return super.delete(getUserId(), params);
//    }
}
