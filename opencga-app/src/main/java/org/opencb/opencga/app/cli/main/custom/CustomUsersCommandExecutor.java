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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;

import java.io.IOException;

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
}
