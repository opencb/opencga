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
package org.opencb.opencga.app.cli.main.parent;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.UsersCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.commons.utils.PrintUtils.getKeyValueAsFormattedString;
import static org.opencb.commons.utils.PrintUtils.println;

public abstract class ParentUsersCommandExecutor extends OpencgaCommandExecutor {

    public static final String LOGIN_OK = "You have been logged in correctly: ";
    public static final String LOGIN_FAIL = "Not available login service now. Please contact the system administrator.";
    public static final String LOGOUT = "You've been logged out.";
    private final UsersCommandOptions usersCommandOptions;

    public ParentUsersCommandExecutor(GeneralCliOptions.CommonCommandOptions options,
                                      UsersCommandOptions usersCommandOptions) throws CatalogAuthenticationException {
        super(options);
        this.usersCommandOptions = usersCommandOptions;
    }

    protected RestResponse<AuthenticationResponse> login() throws Exception {
        logger.debug("Login");
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        try {
            String user = usersCommandOptions.loginCommandOptions.user;
            String password = usersCommandOptions.loginCommandOptions.password;

            if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
                AuthenticationResponse response = openCGAClient.login(user, password);
                if (response != null) {
                    List<String> studies = new ArrayList<>();

                    RestResponse<Project> projects = openCGAClient.getProjectClient().search(
                            new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

                    if (projects.getResponses().get(0).getNumResults() == 0) {
                        // We try to fetch shared projects and studies instead when the user does not own any project or study
                        projects = openCGAClient.getProjectClient().search(new ObjectMap());
                    }
                    for (Project project : projects.getResponses().get(0).getResults()) {
                        for (Study study : project.getStudies()) {
                            studies.add(study.getFqn());
                        }
                    }
                    // write CLI session file

//                    CliSessionManager.getInstance().initUserSession(response.getToken(), user, response.getRefreshToken(), studies, this);
                    this.sessionManager.saveCliSession(user, response.getToken(), response.getRefreshToken(), studies, this.host);
                    res.setType(QueryType.VOID);
                    println(getKeyValueAsFormattedString(LOGIN_OK, user));
                } else {
                    Event event = new Event();
                    event.setMessage(LOGIN_FAIL);
                    event.setType(Event.Type.ERROR);
                    res.getEvents().add(event);
                }
            } else {
                String sessionId = usersCommandOptions.commonCommandOptions.token;
                String errorMsg = "Missing password. ";
                if (StringUtils.isNotEmpty(sessionId)) {
                    errorMsg += "Active token detected ";
                }
                CommandLineUtils.printError(errorMsg, new Exception());
            }
        } catch (Exception e) {
            Event event = new Event();
            event.setMessage(e.getMessage());
            event.setType(Event.Type.ERROR);
            res.getEvents().add(event);
            e.printStackTrace();
        }
        return res;
    }

    protected RestResponse<AuthenticationResponse> logout() throws IOException {
        logger.debug("Logout");
        RestResponse<AuthenticationResponse> res = new RestResponse();
        CommandLineUtils.printDebug("Logging out: " + LOGOUT);
        try {
            sessionManager.logoutCliSessionFile();
            Event event = new Event();
            event.setMessage(LOGOUT);
            event.setType(Event.Type.INFO);
            res.getEvents().add(event);
            res.setType(QueryType.VOID);
        } catch (Exception e) {
            CommandLineUtils.printError("Logout fail", e);
        }
        return res;
    }
}
