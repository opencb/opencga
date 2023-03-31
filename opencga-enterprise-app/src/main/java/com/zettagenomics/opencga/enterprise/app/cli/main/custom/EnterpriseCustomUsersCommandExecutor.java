package com.zettagenomics.opencga.enterprise.app.cli.main.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.custom.CustomUsersCommandExecutor;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.opencb.commons.utils.PrintUtils.getKeyValueAsFormattedString;
import static org.opencb.commons.utils.PrintUtils.println;

public class EnterpriseCustomUsersCommandExecutor extends CustomUsersCommandExecutor {

    private static final String COOKIES = "cookies";

    private EnterpriseConfiguration enterpriseConfiguration;

    public EnterpriseCustomUsersCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                                SessionManager session, String appHome, Logger logger) {
        this(options, token, clientConfiguration, session, appHome, logger, null);
    }

    public EnterpriseCustomUsersCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                                SessionManager session, String appHome, Logger logger, OpenCGAClient openCGAClient) {
        super(options, token, clientConfiguration, session, appHome, logger, openCGAClient);

        this.init();
    }

    private void init() {
        if (enterpriseConfiguration == null) {
            logger.debug("Initialising EnterpriseConfiguration");
            try {
                // We load configuration file either from app home folder or from the JAR
                Path path = Paths.get(appHome).resolve("conf").resolve("enterprise-configuration.yml");
                if (Files.exists(path)) {
                    logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
                    this.enterpriseConfiguration = EnterpriseConfiguration
                            .load(Files.newInputStream(path.toFile().toPath()));
                } else {
                    logger.debug("Loading configuration from JAR file");
                    this.enterpriseConfiguration = EnterpriseConfiguration
                            .load(Configuration.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml"));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public RestResponse<AuthenticationResponse> login() throws Exception {
        if (this.enterpriseConfiguration.getSso() == null || !this.enterpriseConfiguration.getSso().isActive()) {
            return super.login();
        } else {
            Path pythonScriptPath = Paths.get(appHome)
                        .resolve("cloud")
                        .resolve("sso")
                        .resolve("python")
                        .resolve("sso_login.py");
            if (!Files.exists(pythonScriptPath)) {
                throw new RuntimeException("Could not find Python script to load temporal SSO server");
            }
            // 1. Start server to get a valid SSO session for the user
            String pythonScript = pythonScriptPath
                    .toAbsolutePath()
                    .toString();

            logger.debug("Running SSO server temporarily: 'python {}'", pythonScript);
            String pythonBin = enterpriseConfiguration.getSso().getPythonBin();
            if (StringUtils.isEmpty(pythonBin)) {
                pythonBin = "python3";
            }
            ProcessBuilder processBuilder = new ProcessBuilder(pythonBin, pythonScript);
            String processResponse;
            Process p;
            try {
                p = processBuilder.start();
                URI uri;
                if (getClientConfiguration().getCurrentHost().getUrl().endsWith("/")) {
                    uri = new URI(getClientConfiguration().getCurrentHost().getUrl()
                            + "webservices/rest/v2/meta/sso?url=http://localhost:5000/secure");
                } else {
                    uri = new URI(getClientConfiguration().getCurrentHost().getUrl()
                            + "/webservices/rest/v2/meta/sso?url=http://localhost:5000/secure");
                }
                if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                    logger.debug("Loading URL {}", uri);
                    Desktop.getDesktop().browse(uri);
                } else {
                    System.out.println("Browser not detected. Please, open your browser and navigate to " + uri);
                }

                p.waitFor();
                // We store the last line which should have a json format containing the credentials needed
                BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String previousLine = null;
                while ((processResponse = input.readLine()) != null) {
                    previousLine = processResponse;
                }
                processResponse = previousLine;
            } catch (IOException | InterruptedException e) {
                throw new ClientException("Error authenticating from SSO server: " + e.getMessage(), e);
            }

            // 2. Parse response into a map
            ObjectMap ssoResponse;
            try {
                logger.debug("Server response: {}", processResponse);
                // Change single quotes for double quotes
                processResponse = processResponse.replaceAll("'", "\"");
                ssoResponse = JacksonUtils.getDefaultObjectMapper().readValue(processResponse, ObjectMap.class);
            } catch (JsonProcessingException e) {
                throw new ClientException("Error parsing SSO response: " + e.getMessage(), e);
            }

            // 3. Store user, token and cookies in current session file
            String user = ssoResponse.getString("user");
            String token = ssoResponse.getString("token");
            Map<String, Object> cookies = new ObjectMap(COOKIES, ssoResponse.getMap(COOKIES));
            logger.debug("Login user ::: {}", user);
            logger.debug("Login token ::: {}", token);
            logger.debug("Login cookies ::: {}", cookies);
            // Update attributes from ClientConfiguration
            if (openCGAClient.getClientConfiguration().getAttributes() != null) {
                openCGAClient.getClientConfiguration().getAttributes().putAll(cookies);
            } else {
                openCGAClient.getClientConfiguration().setAttributes(cookies);
            }

            AuthenticationResponse response = new AuthenticationResponse(ssoResponse.getString("token"));
            RestResponse<AuthenticationResponse> res = session.saveSession(user, response, openCGAClient,
                    new ObjectMap(COOKIES, ssoResponse.getMap(COOKIES)));
            println(getKeyValueAsFormattedString(LOGIN_OK, user));

            return res;
        }
    }

}
