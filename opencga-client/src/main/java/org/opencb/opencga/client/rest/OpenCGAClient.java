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

package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.analysis.AlignmentClient;
import org.opencb.opencga.client.rest.analysis.VariantClient;
import org.opencb.opencga.client.rest.catalog.*;
import org.opencb.opencga.client.rest.operations.OperationClient;
import org.opencb.opencga.core.response.RestResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

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

    public OpenCGAClient(String user, String password, ClientConfiguration clientConfiguration) throws ClientException {
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
        return getClient(UserClient.class, () -> new UserClient(userId, sessionId, clientConfiguration));
    }

    public ProjectClient getProjectClient() {
        return getClient(ProjectClient.class, () -> new ProjectClient(userId, sessionId, clientConfiguration));
    }

    public StudyClient getStudyClient() {
        return getClient(StudyClient.class, () -> new StudyClient(userId, sessionId, clientConfiguration));
    }

    public FileClient getFileClient() {
        return getClient(FileClient.class, () -> new FileClient(userId, sessionId, clientConfiguration));
    }

    public JobClient getJobClient() {
        return getClient(JobClient.class, () -> new JobClient(userId, sessionId, clientConfiguration));
    }

    public IndividualClient getIndividualClient() {
        return getClient(IndividualClient.class, () -> new IndividualClient(userId, sessionId, clientConfiguration));
    }

    public SampleClient getSampleClient() {
        return getClient(SampleClient.class, () -> new SampleClient(userId, sessionId, clientConfiguration));
    }

    public VariableSetClient getVariableClient() {
        return getClient(VariableSetClient.class, () -> new VariableSetClient(sessionId, clientConfiguration, userId));
    }

    public CohortClient getCohortClient() {
        return getClient(CohortClient.class, () -> new CohortClient(userId, sessionId, clientConfiguration));
    }

    public ClinicalAnalysisClient getClinicalAnalysisClient() {
        return getClient(ClinicalAnalysisClient.class, () -> new ClinicalAnalysisClient(userId, sessionId, clientConfiguration));
    }

    public PanelClient getPanelClient() {
        return getClient(PanelClient.class, () -> new PanelClient(userId, sessionId, clientConfiguration));
    }

    public FamilyClient getFamilyClient() {
        return getClient(FamilyClient.class, () -> new FamilyClient(userId, sessionId, clientConfiguration));
    }

    public ToolClient getToolClient() {
        return getClient(ToolClient.class, () -> new ToolClient(userId, sessionId, clientConfiguration));
    }

    public AlignmentClient getAlignmentClient() {
        return getClient(AlignmentClient.class, () -> new AlignmentClient(userId, sessionId, clientConfiguration));
    }

    public VariantClient getVariantClient() {
        return getClient(VariantClient.class, () -> new VariantClient(userId, sessionId, clientConfiguration));
    }

    public OperationClient getOperationClient() {
        return getClient(OperationClient.class, () -> new OperationClient(userId, sessionId, clientConfiguration));
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractParentClient> T getClient(Class<T> clazz, Supplier<T> constructor) {
        return (T) clients.computeIfAbsent(clazz.getName(), (k) -> constructor.get());
    }

    /**
     * Logs in the user.
     *
     * @param user userId.
     * @param password Password.
     * @return the sessionId of the user logged in. Null if the user or password is incorrect.
     * @throws ClientException when it is not possible logging in.
     */
    public String login(String user, String password) throws ClientException {
        UserClient userClient = getUserClient();
        RestResponse<ObjectMap> login = userClient.login(user, password);
        String sessionId = "";
        if (login.allResultsSize() == 1) {
            sessionId = login.firstResult().getString("token");
            setSessionId(sessionId);
            setUserId(user);
        } else {
            for (Event event : login.getEvents()) {
                if (event.getType() == Event.Type.ERROR) {
                    throw new ClientException(event.getMessage());
                }
            }
        }

        return sessionId;
    }

    /**
     * Refresh the user token.
     *
     * @return the new token of the user. Null if the current session id is no longer valid.
     * @throws ClientException when it is not possible refreshing.
     */
    public String refresh() throws ClientException {
        UserClient userClient = getUserClient();
        RestResponse<ObjectMap> refresh = userClient.refresh();
        String sessionId = "";
        if (refresh.allResultsSize() == 1) {
            sessionId = refresh.firstResult().getString("token");
            setSessionId(sessionId);
        } else {
            for (Event event : refresh.getEvents()) {
                if (event.getType() == Event.Type.ERROR) {
                    throw new ClientException(event.getMessage());
                }
            }
        }

        return sessionId;
    }

    public void logout() {
        if (this.sessionId != null) {
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
