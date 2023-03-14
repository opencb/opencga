package com.zettagenomics.opencga.enterprise.client.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zettagenomics.opencga.enterprise.client.rest.clients.EnterpriseAbstractParentClient;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.user.AuthenticationResponse;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

public class PrivateOpenCGAClient extends OpenCGAClient {

    private EnterpriseConfiguration enterpriseConfiguration;
    private Map<String, EnterpriseAbstractParentClient> clients;

    public PrivateOpenCGAClient(ClientConfiguration clientConfiguration,
                                EnterpriseConfiguration enterpriseConfiguration) {
        super(clientConfiguration);
        this.enterpriseConfiguration = enterpriseConfiguration;
    }

    public PrivateOpenCGAClient(String user, String password, ClientConfiguration clientConfiguration,
                                EnterpriseConfiguration enterpriseConfiguration) throws ClientException {
        super(user, password, clientConfiguration);
        this.enterpriseConfiguration = enterpriseConfiguration;
    }

    public PrivateOpenCGAClient(AuthenticationResponse authenticationTokens, ClientConfiguration clientConfiguration,
                                EnterpriseConfiguration enterpriseConfiguration) {
        super(authenticationTokens, clientConfiguration);
        this.enterpriseConfiguration = enterpriseConfiguration;
    }

    @Override
    public AuthenticationResponse login(String user, String password) throws ClientException {
        if (this.enterpriseConfiguration.getSsoConfiguration() != null
                && this.enterpriseConfiguration.getSsoConfiguration().isActive()) {
            return loginFromSSO();
        } else {
            return super.login(user, password);
        }
    }

    private AuthenticationResponse loginFromSSO() throws ClientException {
        // 1. Start server to get a valid SSO session for the user
        ProcessBuilder processBuilder = new ProcessBuilder("python login_sso.py");
        String processResponse;
        Process p;
        try {
            p = processBuilder.start();
            URI uri = new URI(getClientConfiguration().getCurrentHost().getUrl())
                    .resolve("webservices")
                    .resolve("rest")
                    .resolve("v2")
                    .resolve("meta")
                    .resolve("sso")
                    .resolve("?url=http://localhost:5000/secure");
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                Desktop.getDesktop().browse(uri);
            } else {
                System.out.println("Browser not detected. Please, open your browser and navigate to " + uri);
            }

            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            processResponse = input.readLine();
            p.waitFor();
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw new ClientException("Error authenticating from SSO server: " + e.getMessage(), e);
        }

        // 2. Parse response into a map
        ObjectMap ssoResponse;
        try {
            ssoResponse = JacksonUtils.getDefaultObjectMapper().readValue(processResponse, ObjectMap.class);
        } catch (JsonProcessingException e) {
            throw new ClientException("Error parsing SSO response: " + e.getMessage(), e);
        }

        // 3. Store cookies and token in current session file

        // 4. Send cookies to all clients
        updateCookiesFromClients(ssoResponse.getMap("cookies"));

        // 5. Return token
        return new AuthenticationResponse(ssoResponse.getString("token"));
    }

    private void updateCookiesFromClients(Map<String, Object> cookies) {
        clients.values().stream()
                .filter(Objects::nonNull)
                .forEach(enterpriseAbstractParentClient -> {
                    enterpriseAbstractParentClient.setSsoCookies(cookies);
                });
    }
}
