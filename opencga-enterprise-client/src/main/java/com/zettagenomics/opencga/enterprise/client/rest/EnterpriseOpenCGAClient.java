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

package com.zettagenomics.opencga.enterprise.client.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zettagenomics.opencga.enterprise.client.rest.clients.*;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.response.RestResponse;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;


public class EnterpriseOpenCGAClient extends OpenCGAClient {

    private String userId;
    private String token;
    private String refreshToken;
    private ClientConfiguration clientConfiguration;
    private EnterpriseConfiguration enterpriseConfiguration;
    private Map<String, String> cookies;
    private Map<String, EnterpriseAbstractParentClient> clients;
    private boolean throwExceptionOnError;

    public EnterpriseOpenCGAClient(String user, String password, ClientConfiguration clientConfiguration) throws ClientException {
        super(user, password, clientConfiguration);
    }

    public EnterpriseOpenCGAClient(AuthenticationResponse authenticationTokens, ClientConfiguration clientConfiguration) {
        super(authenticationTokens, clientConfiguration);
    }

    public EnterpriseOpenCGAClient(ClientConfiguration clientConfiguration,
                                   EnterpriseConfiguration enterpriseConfiguration) {
        super(clientConfiguration);
        this.enterpriseConfiguration = enterpriseConfiguration;
    }

    public EnterpriseOpenCGAClient(String user, String password, ClientConfiguration clientConfiguration,
                                   EnterpriseConfiguration enterpriseConfiguration) throws ClientException {
        super(user, password, clientConfiguration);
        this.enterpriseConfiguration = enterpriseConfiguration;
    }

    public EnterpriseOpenCGAClient(AuthenticationResponse authenticationTokens, ClientConfiguration clientConfiguration,
                                   EnterpriseConfiguration enterpriseConfiguration) {
        super(authenticationTokens, clientConfiguration);
        this.enterpriseConfiguration = enterpriseConfiguration;
    }

    public UserClient getEnterpriseUserClient() {
        return this.getClient(UserClient.class, () -> new UserClient(this.token, this.clientConfiguration));
    }

    public ProjectClient getEnterpriseProjectClient() {
        return this.getClient(ProjectClient.class, () -> new ProjectClient(this.token, this.clientConfiguration));
    }

    public StudyClient getEnterpriseStudyClient() {
        return this.getClient(StudyClient.class, () -> new StudyClient(this.token, this.clientConfiguration));
    }

    public FileClient getEnterpriseFileClient() {
        return this.getClient(FileClient.class, () -> new FileClient(this.token, this.clientConfiguration));
    }

    public JobClient getEnterpriseJobClient() {
        return this.getClient(JobClient.class, () -> new JobClient(this.token, this.clientConfiguration));
    }

    public IndividualClient getEnterpriseIndividualClient() {
        return this.getClient(IndividualClient.class, () -> new IndividualClient(this.token, this.clientConfiguration));
    }

    public SampleClient getEnterpriseSampleClient() {
        return this.getClient(SampleClient.class, () -> new SampleClient(this.token, this.clientConfiguration));
    }

    public AdminClient getEnterpriseAdminClient() {
        return this.getClient(AdminClient.class, () -> new AdminClient(this.token, this.clientConfiguration));
    }

    public CohortClient getEnterpriseCohortClient() {
        return this.getClient(CohortClient.class, () -> new CohortClient(this.token, this.clientConfiguration));
    }

    public ClinicalAnalysisClient getEnterpriseClinicalAnalysisClient() {
        return this.getClient(ClinicalAnalysisClient.class,
                () -> new ClinicalAnalysisClient(this.token, this.clientConfiguration));
    }

    public DiseasePanelClient getEnterpriseDiseasePanelClient() {
        return this.getClient(DiseasePanelClient.class,
                () -> new DiseasePanelClient(this.token, this.clientConfiguration));
    }

    public FamilyClient getEnterpriseFamilyClient() {
        return this.getClient(FamilyClient.class, () -> new FamilyClient(this.token, this.clientConfiguration));
    }

    public AlignmentClient getEnterpriseAlignmentClient() {
        return this.getClient(AlignmentClient.class, () -> new AlignmentClient(this.token, this.clientConfiguration));
    }

    public VariantClient getEnterpriseVariantClient() {
        return this.getClient(VariantClient.class, () -> new VariantClient(this.token, this.clientConfiguration));
    }

    public VariantOperationClient getEnterpriseVariantOperationClient() {
        return this.getClient(VariantOperationClient.class,
                () -> new VariantOperationClient(this.token, this.clientConfiguration));
    }

    public MetaClient getEnterpriseMetaClient() {
        return this.getClient(MetaClient.class, () -> new MetaClient(this.token, this.clientConfiguration));
    }

    public CvaClient getCvaClient() {
        return getClient(CvaClient.class, () -> new CvaClient(token, clientConfiguration));
    }

    @SuppressWarnings("unchecked")
    private <T extends EnterpriseAbstractParentClient> T getClient(Class<T> clazz, Supplier<T> constructor) {
        return (T) clients.computeIfAbsent(clazz.getName(), (k) -> {
            T t = constructor.get();
            t.setEnterpriseConfiguration(enterpriseConfiguration);
            t.setThrowExceptionOnError(throwExceptionOnError);
            return t;
        });
    }

//    /**
//     * Refresh the user token.
//     *
//     * @return the new AuthenticationResponse object.
//     * @throws ClientException when it is not possible refreshing.
//     */
//    public AuthenticationResponse refresh() throws ClientException {
//        if (StringUtils.isEmpty(refreshToken)) {
//            throw new ClientException("Could not refresh token. 'refreshToken' not available.");
//        }
//        RestResponse<AuthenticationResponse> refresh = new UserClient(token, clientConfiguration).login(new LoginParams(refreshToken), null);
//        updateTokenFromClients(refresh);
//        return refresh.firstResult();
//    }
//
//    /**
//     * Logs in the user.
//     *
//     * @param user     userId.
//     * @param password Password.
//     * @return AuthenticationResponse object.
//     * @throws ClientException when it is not possible logging in.
//     */
//    public AuthenticationResponse login(String user, String password) throws ClientException {
//        if (this.enterpriseConfiguration.getSso() != null && this.enterpriseConfiguration.getSso().isActive()) {
//            return ssoLogin();
//        } else {
//            return nonSsoLogin(user, password);
//        }
//    }
//
//    public AuthenticationResponse nonSsoLogin(String user, String password) throws ClientException {
//        RestResponse<AuthenticationResponse> login = new UserClient(token, clientConfiguration)
//                .login(new LoginParams(user, password), null);
//        updateTokenFromClients(login);
//        this.userId = user;
//        return login.firstResult();
//    }
//
//    private AuthenticationResponse ssoLogin() throws ClientException {
//        // 1. Start server to get a valid SSO session for the user
//        ProcessBuilder processBuilder = new ProcessBuilder("python sso_login.py");
//        String processResponse;
//        Process p;
//        try {
//            p = processBuilder.start();
//            URI uri = new URI(getClientConfiguration().getCurrentHost().getUrl())
//                    .resolve("webservices")
//                    .resolve("rest")
//                    .resolve("v2")
//                    .resolve("meta")
//                    .resolve("sso")
//                    .resolve("?url=http://localhost:5000/secure");
//            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
//                Desktop.getDesktop().browse(uri);
//            } else {
//                System.out.println("Browser not detected. Please, open your browser and navigate to " + uri);
//            }
//
//            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
//            processResponse = input.readLine();
//            p.waitFor();
//        } catch (IOException | InterruptedException | URISyntaxException e) {
//            throw new ClientException("Error authenticating from SSO server: " + e.getMessage(), e);
//        }
//
//        // 2. Parse response into a map
//        ObjectMap ssoResponse;
//        try {
//            ssoResponse = JacksonUtils.getDefaultObjectMapper().readValue(processResponse, ObjectMap.class);
//        } catch (JsonProcessingException e) {
//            throw new ClientException("Error parsing SSO response: " + e.getMessage(), e);
//        }
//
//        // 3. Store cookies and token in current session file
//
//        // 4. Send cookies to all clients
//        updateCookiesFromClients(ssoResponse.getMap("cookies"));
//
//        // 5. Return token
//        return new AuthenticationResponse(ssoResponse.getString("token"));
//    }
//
//    private void updateCookiesFromClients(Map<String, Object> cookies) {
//        //clients.values().stream()
//        //        .filter(Objects::nonNull)
//        //        .forEach(enterpriseAbstractParentClient -> {
//        //            enterpriseAbstractParentClient.setSsoCookies(cookies);
//        //        });
//    }
//
//    /**
//     * Logs in the user.
//     *
//     * @param refreshToken userId.
//     * @return AuthenticationResponse object.
//     * @throws ClientException when it is not possible logging in.
//     */
//    public AuthenticationResponse refresh(String refreshToken) throws ClientException {
//        RestResponse<AuthenticationResponse> login = new UserClient(token, clientConfiguration).login(new LoginParams(refreshToken), null);
//        updateTokenFromClients(login);
//        return login.firstResult();
//    }
//
//    public void updateTokenFromClients(RestResponse<AuthenticationResponse> loginResponse) throws ClientException {
//        if (loginResponse.allResultsSize() == 1) {
//            setToken(loginResponse.firstResult().getToken());
//            setRefreshToken(loginResponse.firstResult().getRefreshToken());
//        } else {
//            for (Event event : loginResponse.getEvents()) {
//                if (event.getType() == Event.Type.ERROR) {
//                    throw new ClientException(event.getMessage());
//                }
//            }
//        }
//    }
//
//    public void logout() {
//        if (this.token != null) {
//            // Remove token and userId for all clients
//            setToken(null);
//            setUserId(null);
//        }
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpenCGAClient{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
        sb.append(", clientConfiguration=").append(clientConfiguration);
        sb.append('}');
        return sb.toString();
    }
//
//    public String getUserId() {
//        return userId;
//    }
//
//    public void setUserId(String userId) {
//        this.userId = userId;
//    }
//
//    public String getToken() {
//        return token;
//    }
//
//    public void setToken(String token) {
//        this.token = token;
//
//        // Update token for all clients
//        clients.values().stream()
//                .filter(Objects::nonNull)
//                .forEach(abstractParentClient -> {
//                    abstractParentClient.setToken(this.token);
//                });
//    }
//
//    public String getRefreshToken() {
//        return refreshToken;
//    }
//
//    public EnterpriseOpenCGAClient setRefreshToken(String refreshToken) {
//        this.refreshToken = refreshToken;
//        return this;
//    }
//
//    public ClientConfiguration getClientConfiguration() {
//        return clientConfiguration;
//    }
//
//    public EnterpriseOpenCGAClient setClientConfiguration(ClientConfiguration clientConfiguration) {
//        this.clientConfiguration = clientConfiguration;
//        return this;
//    }
//
//    public boolean isThrowExceptionOnError() {
//        return throwExceptionOnError;
//    }
//
//    public EnterpriseOpenCGAClient setThrowExceptionOnError(boolean throwExceptionOnError) {
//        this.throwExceptionOnError = throwExceptionOnError;
//        // We have to set the value to all existing clients
//        clients.values().stream()
//                .filter(Objects::nonNull)
//                .forEach(abstractParentClient -> {
//                    abstractParentClient.setThrowExceptionOnError(this.throwExceptionOnError);
//                });
//        return this;
//    }

}
