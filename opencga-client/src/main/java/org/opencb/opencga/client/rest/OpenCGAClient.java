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

import io.jsonwebtoken.Jwts;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.response.RestResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Created by imedina on 04/05/16.
 */
public class OpenCGAClient {

    private String userId;
    private String token;
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

    public OpenCGAClient(String token, ClientConfiguration clientConfiguration) {
        init(token, clientConfiguration);
    }

    private void init(String token, ClientConfiguration clientConfiguration) {
        setToken(token);
        this.clientConfiguration = clientConfiguration;

        this.userId = Jwts.parser().parseClaimsJws(token).getBody().getSubject();
        clients = new HashMap<>(20);
    }


    public UserClient getUserClient() {
        return getClient(UserClient.class, () -> new UserClient(token, clientConfiguration));
    }

    public ProjectClient getProjectClient() {
        return getClient(ProjectClient.class, () -> new ProjectClient(token, clientConfiguration));
    }

    public StudyClient getStudyClient() {
        return getClient(StudyClient.class, () -> new StudyClient(token, clientConfiguration));
    }

    public FileClient getFileClient() {
        return getClient(FileClient.class, () -> new FileClient(token, clientConfiguration));
    }

    public JobClient getJobClient() {
        return getClient(JobClient.class, () -> new JobClient(token, clientConfiguration));
    }

    public IndividualClient getIndividualClient() {
        return getClient(IndividualClient.class, () -> new IndividualClient(token, clientConfiguration));
    }

    public SampleClient getSampleClient() {
        return getClient(SampleClient.class, () -> new SampleClient(token, clientConfiguration));
    }

    public CohortClient getCohortClient() {
        return getClient(CohortClient.class, () -> new CohortClient(token, clientConfiguration));
    }

    public ClinicalClient getClinicalAnalysisClient() {
        return getClient(ClinicalClient.class, () -> new ClinicalClient(token, clientConfiguration));
    }

    public DiseasePanelClient getDiseasePanelClient() {
        return getClient(DiseasePanelClient.class, () -> new DiseasePanelClient(token, clientConfiguration));
    }

    public FamilyClient getFamilyClient() {
        return getClient(FamilyClient.class, () -> new FamilyClient(token, clientConfiguration));
    }

    public AlignmentClient getAlignmentClient() {
        return getClient(AlignmentClient.class, () -> new AlignmentClient(token, clientConfiguration));
    }

    public VariantClient getVariantClient() {
        return getClient(VariantClient.class, () -> new VariantClient(token, clientConfiguration));
    }

    public VariantOperationClient getVariantOperationClient() {
        return getClient(VariantOperationClient.class, () -> new VariantOperationClient(token, clientConfiguration));
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
     * @return the token of the user logged in. Null if the user or password is incorrect.
     * @throws ClientException when it is not possible logging in.
     */
    public String login(String user, String password) throws ClientException {
        RestResponse<ObjectMap> login = getUserClient().login(user, new LoginParams().setPassword(password), null);
        updateTokenFromClients(login);
        setUserId(user);
        return login.firstResult().getString("token");
    }

    /**
     * Refresh the user token.
     *
     * @return the new token of the user. Null if the current session id is no longer valid.
     * @throws ClientException when it is not possible refreshing.
     */
    public String refresh() throws ClientException {
        RestResponse<ObjectMap> refresh = getUserClient().login(userId, new LoginParams(), null);
        updateTokenFromClients(refresh);
        return refresh.firstResult().getString("token");
    }

    public void updateTokenFromClients(RestResponse<ObjectMap> loginResponse) throws ClientException {
        String token = "";
        if (loginResponse.allResultsSize() == 1) {
            token = loginResponse.firstResult().getString("token");
            setToken(token);
        } else {
            for (Event event : loginResponse.getEvents()) {
                if (event.getType() == Event.Type.ERROR) {
                    throw new ClientException(event.getMessage());
                }
            }
        }
    }

    public void logout() {
        if (this.token != null) {
            // Remove token and userId for all clients
            setToken(null);
            setUserId(null);
        }
    }


    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;

        // Update token for all clients
        clients.values().stream()
                .filter(Objects::nonNull)
                .forEach(abstractParentClient -> {
                    abstractParentClient.setToken(this.token);
                });
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
