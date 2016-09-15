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
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public class OpenCGAClient {

    private String userId;
    private String sessionId;
    private ClientConfiguration clientConfiguration;

    private Map<String, AbstractParentClient> clients;

    public OpenCGAClient() {
        // create a default configuration to localhost
    }

    public OpenCGAClient(ClientConfiguration clientConfiguration) {
        this(null, clientConfiguration);
    }

    public OpenCGAClient(String user, String password, ClientConfiguration clientConfiguration) throws CatalogException {
        init(null, clientConfiguration);

        login(user, password);
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
        clients.putIfAbsent("USER", new UserClient(userId, sessionId, clientConfiguration));
        return (UserClient) clients.get("USER");
    }

    public ProjectClient getProjectClient() {
        clients.putIfAbsent("PROJECT", new ProjectClient(userId, sessionId, clientConfiguration));
        return (ProjectClient) clients.get("PROJECT");
    }

    public StudyClient getStudyClient() {
        clients.putIfAbsent("STUDY", new StudyClient(userId, sessionId, clientConfiguration));
        return (StudyClient) clients.get("STUDY");
    }

    public FileClient getFileClient() {
        clients.putIfAbsent("FILE", new FileClient(userId, sessionId, clientConfiguration));
        return (FileClient) clients.get("FILE");
    }

    public JobClient getJobClient() {
        clients.putIfAbsent("JOB", new JobClient(userId, sessionId, clientConfiguration));
        return (JobClient) clients.get("JOB");
    }

    public IndividualClient getIndividualClient() {
        clients.putIfAbsent("INDIVIDUAL", new IndividualClient(userId, sessionId, clientConfiguration));
        return (IndividualClient) clients.get("INDIVIDUAL");
    }

    public SampleClient getSampleClient() {
        clients.putIfAbsent("SAMPLE", new SampleClient(userId, sessionId, clientConfiguration));
        return (SampleClient) clients.get("SAMPLE");
    }

    public VariableSetClient getVariableClient() {
        clients.putIfAbsent("VARIABLE", new VariableSetClient(sessionId, clientConfiguration, userId));
        return (VariableSetClient) clients.get("VARIABLE");
    }

    public CohortClient getCohortClient() {
        clients.putIfAbsent("COHORT", new CohortClient(userId, sessionId, clientConfiguration));
        return (CohortClient) clients.get("COHORT");
    }

    public PanelClient getPanelClient() {
        clients.putIfAbsent("PANEL", new PanelClient(userId, sessionId, clientConfiguration));
        return (PanelClient) clients.get("PANEL");
    }

    public ToolClient getToolClient() {
        clients.putIfAbsent("TOOL", new ToolClient(userId, sessionId, clientConfiguration));
        return (ToolClient) clients.get("TOOL");
    }
    /**
     * Logs in the user.
     *
     * @param user userId.
     * @param password Password.
     * @return the sessionId of the user logged in. Null if the user or password is incorrect.
     * @throws CatalogException when it is not possible logging in.
     */
    public String login(String user, String password) throws CatalogException {
        UserClient userClient = getUserClient();
        QueryResponse<ObjectMap> login = userClient.login(user, password);
        String sessionId;
        if (login.allResultsSize() == 1) {
            sessionId = login.firstResult().getString("sessionId");

            if (this.sessionId != null) { // If the latest sessionId is still active
                try {
                    userClient.logout();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            setSessionId(sessionId);
            setUserId(user);
        } else {
            throw new CatalogException(login.getError());
        }

        return sessionId;
    }

    public void logout() {
        if (this.sessionId != null) {
            UserClient userClient = getUserClient();
            userClient.logout();

            // Remove sessionId and userId for all clients
            setSessionId(null);
            setUserId(null);
        }
    }


    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;

        // Update sessionId for all clients
        clients.values().stream()
                .filter(abstractParentClient -> abstractParentClient != null)
                .forEach(abstractParentClient -> {
                    abstractParentClient.setSessionId(this.sessionId);
                });
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;

        // Update userId for all clients
        clients.values().stream()
                .filter(abstractParentClient -> abstractParentClient != null)
                .forEach(abstractParentClient -> {
                    abstractParentClient.setUserId(this.userId);
                });
    }
}
