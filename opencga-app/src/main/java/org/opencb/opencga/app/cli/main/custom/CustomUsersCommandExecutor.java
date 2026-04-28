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
package org.opencb.opencga.app.cli.main.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.opencga.app.cli.main.options.UsersCommandOptions;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;

import java.awt.Desktop;
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

public class CustomUsersCommandExecutor extends CustomCommandExecutor {

    public static final String LOGIN_OK = "You have been logged in correctly: ";
    public static final String LOGIN_FAIL = "Incorrect username or password.";
    public static final String LOGIN_ERROR = "Not available login service now. Please contact the system administrator.";
    public static final String LOGOUT = "You've been logged out.";


    public CustomUsersCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                      SessionManager session, String appHome, Logger logger) {
        super(options, token, clientConfiguration, session, appHome, logger);
    }

    public CustomUsersCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                      SessionManager session, String appHome, Logger logger, OpenCGAClient openCGAClient) {
        super(options, token, clientConfiguration, session, appHome, logger, openCGAClient);
    }

    public RestResponse<AuthenticationResponse> login(CustomUsersCommandOptions.LoginCommandOptions commandOptions) throws Exception {
        logger.debug("Login");
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        try {
            String user = commandOptions.user;
            String password = commandOptions.password;

            if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
                AuthenticationResponse response = null;
                try {
                    response = openCGAClient.login(commandOptions.organization, user, password);
                } catch (Exception e) {
                    logger.debug("Login error", e);
                    Event event = new Event();
                    event.setMessage(e.getMessage());
                    event.setType(Event.Type.ERROR);
                    res.setType(QueryType.VOID);
                    res.getEvents().add(event);
                    return res;
                }
                logger.debug("Login token ::: " + session.getSession().getToken());
                res = session.saveSession(user, response, openCGAClient);
                println(getKeyValueAsFormattedString(LOGIN_OK, user));
            } else {
                String token = session.getSession().getToken();
                String errorMsg = "Missing password. ";
                if (StringUtils.isNotEmpty(token)) {
                    errorMsg += "Active token detected. Please logout first.";
                }
                CommandLineUtils.error(errorMsg);
            }
        } catch (Exception e) {
            CommandLineUtils.error(LOGIN_ERROR, e);
            logger.debug("Login error", e);
            Event event = new Event();
            event.setMessage(LOGIN_ERROR + e.getMessage());
            res.setType(QueryType.VOID);
            event.setType(Event.Type.ERROR);
            res.getEvents().add(event);
        }
        return res;
    }


    public RestResponse<AuthenticationResponse> logout(CustomUsersCommandOptions.LogoutCommandOptions commandOptions) throws IOException {
        logger.debug("Logout");
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        try {
            session.logoutSessionFile();
            openCGAClient.logout();
            Event event = new Event();
            event.setMessage(LOGOUT);
            event.setType(Event.Type.INFO);
            res.getEvents().add(event);
            res.setType(QueryType.VOID);
        } catch (Exception e) {
            CommandLineUtils.error("Logout fail", e);
            logger.debug("Logout error", e);
        }
        return res;
    }

    // TODO: update import to org.opencb.opencga.app.cli.main.options.UsersCommandOptions after autogeneration
    public RestResponse<AuthenticationResponse> loginSso(UsersCommandOptions.LoginSsoCommandOptions loginSsoCommandOptions) throws Exception {
        logger.debug("Executing loginSso in Users command line");

        Path pythonScriptPath = Paths.get(appHome)
                .resolve("cloud")
                .resolve("sso")
                .resolve("python")
                .resolve("sso_login.py");
        if (!Files.exists(pythonScriptPath)) {
            throw new RuntimeException("Could not find Python script to load temporal SSO server");
        }
        String pythonScript = pythonScriptPath.toAbsolutePath().toString();

        logger.debug("Running SSO server temporarily: 'python {}'", pythonScript);
        ProcessBuilder processBuilder = new ProcessBuilder("python3", pythonScript);
        String processResponse;
        Process p;
        try {
            p = processBuilder.start();
            URI uri;
            if (getClientConfiguration().getCurrentHost().getUrl().endsWith("/")) {
                uri = new URI(getClientConfiguration().getCurrentHost().getUrl()
                        + "webservices/rest/v2/meta/sso/login?url=http://localhost:5000/secure");
            } else {
                uri = new URI(getClientConfiguration().getCurrentHost().getUrl()
                        + "/webservices/rest/v2/meta/sso/login?url=http://localhost:5000/secure");
            }
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                logger.debug("Loading URL {}", uri);
                Desktop.getDesktop().browse(uri);
            } else {
                System.out.println("Browser not detected. Please, open your browser and navigate to " + uri);
            }

            p.waitFor();

            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String previousLine = null;
            while ((processResponse = input.readLine()) != null) {
                previousLine = processResponse;
            }
            processResponse = previousLine;
            if (processResponse == null) {
                throw new ClientException("Please, check the minimum Python3 requirements (Flask==2.2.5)");
            }
        } catch (IOException | InterruptedException e) {
            throw new ClientException("Error authenticating from SSO server: " + e.getMessage(), e);
        }

        ObjectMap ssoResponse;
        try {
            logger.debug("Server response: {}", processResponse);
            processResponse = processResponse.replaceAll("'", "\"");
            ssoResponse = JacksonUtils.getDefaultObjectMapper().readValue(processResponse, ObjectMap.class);
        } catch (JsonProcessingException e) {
            throw new ClientException("Error parsing SSO response: " + e.getMessage(), e);
        }

        String user = ssoResponse.getString("user");
        String token = ssoResponse.getString("token");
        Map<String, Object> cookies = new ObjectMap("cookies", ssoResponse.getMap("cookies"));
        logger.debug("Login user ::: {}", user);
        logger.debug("Login token ::: {}", token);
        logger.debug("Login cookies ::: {}", cookies);
        if (openCGAClient.getClientConfiguration().getAttributes() != null) {
            openCGAClient.getClientConfiguration().getAttributes().putAll(cookies);
        } else {
            openCGAClient.getClientConfiguration().setAttributes(cookies);
        }

        AuthenticationResponse response = new AuthenticationResponse(ssoResponse.getString("token"));
        RestResponse<AuthenticationResponse> res = session.saveSession(user, response, openCGAClient,
                new ObjectMap("cookies", ssoResponse.getMap("cookies")));
        println(getKeyValueAsFormattedString(LOGIN_OK, user));

        return res;
    }

    public RestResponse<AuthenticationResponse> logoutSso(UsersCommandOptions.LogoutSsoCommandOptions logoutSsoCommandOptions) throws Exception {
        logger.debug("Executing logout SSO in Users command line");
        return logout(null);
    }
}
