/*
 * Copyright 2015-2020 OpenCB
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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.clients.*;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.response.RestResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Created by imedina on 04/05/16.
 */
public class OpenCGAClient {

    private String userId;
    private String token;
    private String refreshToken;
    private final ClientConfiguration clientConfiguration;

    private final Map<String, AbstractParentClient> clients;
    private boolean throwExceptionOnError;

    public OpenCGAClient(ClientConfiguration clientConfiguration) {
        this(null, clientConfiguration);
    }

    public OpenCGAClient(String user, String password, ClientConfiguration clientConfiguration) throws ClientException {
        this(null, clientConfiguration);
        login(user, password);
    }

    public OpenCGAClient(AuthenticationResponse authenticationTokens, ClientConfiguration clientConfiguration) {
        this.clients = new HashMap<>(20);
        this.clientConfiguration = clientConfiguration;

        init(authenticationTokens);
    }

    private void init(AuthenticationResponse tokens) {
        if (tokens != null) {
            this.userId = getUserFromToken(tokens.getToken());
            setToken(tokens.getToken());
            setRefreshToken(tokens.getRefreshToken());
        }
    }

    protected static String getUserFromToken(String token) {
        // https://github.com/jwtk/jjwt/issues/280
        // https://github.com/jwtk/jjwt/issues/86
        // https://stackoverflow.com/questions/34998859/android-jwt-parsing-payload-claims-when-signed
        String withoutSignature = token.substring(0, token.lastIndexOf('.') + 1);
        Claims claims = (Claims)  Jwts.parser()
                .setAllowedClockSkewSeconds(TimeUnit.DAYS.toSeconds(3650))
                .parse(withoutSignature)
                .getBody();
        return claims.getSubject();
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

    public MetaClient getMetaClient() {
        return getClient(MetaClient.class, () -> new MetaClient(token, clientConfiguration));
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractParentClient> T getClient(Class<T> clazz, Supplier<T> constructor) {
        return (T) clients.computeIfAbsent(clazz.getName(), (k) -> {
            T t = constructor.get();
            t.setThrowExceptionOnError(throwExceptionOnError);
            return t;
        });
    }

    /**
     * Logs in the user.
     *
     * @param user userId.
     * @param password Password.
     * @return AuthenticationResponse object.
     * @throws ClientException when it is not possible logging in.
     */
    public AuthenticationResponse login(String user, String password) throws ClientException {
        RestResponse<AuthenticationResponse> login = getUserClient().login(new LoginParams(user, password), null);
        updateTokenFromClients(login);
        setUserId(user);
        return login.firstResult();
    }

    /**
     * Refresh the user token.
     *
     * @return the new AuthenticationResponse object.
     * @throws ClientException when it is not possible refreshing.
     */
    public AuthenticationResponse refresh() throws ClientException {
        if (StringUtils.isEmpty(refreshToken)) {
            throw new ClientException("Could not refresh token. 'refreshToken' not available.");
        }
        RestResponse<AuthenticationResponse> refresh = getUserClient().login(new LoginParams(refreshToken), null);
        updateTokenFromClients(refresh);
        return refresh.firstResult();
    }

    public void updateTokenFromClients(RestResponse<AuthenticationResponse> loginResponse) throws ClientException {
        if (loginResponse.allResultsSize() == 1) {
            setToken(loginResponse.firstResult().getToken());
            setRefreshToken(loginResponse.firstResult().getRefreshToken());
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

    public String getRefreshToken() {
        return refreshToken;
    }

    public OpenCGAClient setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }

    public OpenCGAClient setThrowExceptionOnError(boolean throwExceptionOnError) {
        this.throwExceptionOnError = throwExceptionOnError;
        clients.values().stream()
                .filter(Objects::nonNull)
                .forEach(abstractParentClient -> {
                    abstractParentClient.setThrowExceptionOnError(this.throwExceptionOnError);
                });
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
