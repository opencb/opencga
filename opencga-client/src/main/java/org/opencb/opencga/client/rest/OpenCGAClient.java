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
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public class OpenCGAClient {

    private Map<String, AbstractParentClient> clients;

    private String sessionId;
    private ClientConfiguration clientConfiguration;

    public OpenCGAClient() {
        // create a default configuration to localhost
    }

    public OpenCGAClient(ClientConfiguration clientConfiguration) {
        this(null, clientConfiguration);
    }

    public OpenCGAClient(String user, String password, ClientConfiguration clientConfiguration) {
        init(null, clientConfiguration);

        this.sessionId = login(user, password);
    }

    public OpenCGAClient(String sessionId, ClientConfiguration clientConfiguration) {
        init(sessionId, clientConfiguration);
    }

    private void init(String sessionId, ClientConfiguration clientConfiguration) {
        this.sessionId = sessionId;
        this.clientConfiguration = clientConfiguration;

        clients = new HashMap<>(20);
    }


    public UserClient getUserClient() {
        clients.putIfAbsent("USER", new UserClient(sessionId, clientConfiguration));
        return (UserClient) clients.get("USER");
    }

    public ProjectClient getProjectClient() {
        clients.putIfAbsent("PROJECT", new ProjectClient(sessionId, clientConfiguration));
        return (ProjectClient) clients.get("PROJECT");
    }


    public String login(String user, String password) {
        UserClient userClient = getUserClient();
        QueryResponse<ObjectMap> login = userClient.login(user, password);
        this.sessionId = login.firstResult().getString("sessionId");

        // Update sessionId for all clients
        for (AbstractParentClient abstractParentClient : clients.values()) {
            if (abstractParentClient != null) {
                abstractParentClient.setSessionId(this.sessionId);
            }
        }
        return sessionId;
    }

    public void logout() {
        // Remove sessionId for all clients
        for (AbstractParentClient abstractParentClient : clients.values()) {
            if (abstractParentClient != null) {
                abstractParentClient.setSessionId(null);
            }
        }
    }

}
