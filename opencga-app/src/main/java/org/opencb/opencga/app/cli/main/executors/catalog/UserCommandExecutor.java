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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.UserCommandOptions;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by imedina on 02/03/15.
 */
public class UserCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private UserCommandOptions usersCommandOptions;

    public UserCommandExecutor(UserCommandOptions usersCommandOptions) {

        super(usersCommandOptions.commonCommandOptions, getParsedSubCommand(usersCommandOptions.getjCommander()).startsWith("log"));
        this.usersCommandOptions = usersCommandOptions;
    }



    @Override
    public void execute() throws Exception {

        logger.debug("Executing users command line");
//        openCGAClient = new OpenCGAClient(clientConfiguration);

        String subCommandString = getParsedSubCommand(usersCommandOptions.getjCommander());
//        if (!subCommandString.equals("login") && !subCommandString.equals("logout")) {
//            checkSessionValid();
//        }
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "delete":
                delete();
                break;
            case "update":
                queryResponse = update();
                break;
            case "change-password":
                queryResponse = changePassword();
                break;
            case "projects":
                queryResponse = projects();
                break;
            case "login":
                login();
                break;
            case "logout":
                logout();
                break;
            case "reset-password":
                resetPasword();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private void login() throws CatalogException, IOException {
        logger.debug("Login");

        String user = usersCommandOptions.loginCommandOptions.user;
        String password = usersCommandOptions.loginCommandOptions.password;

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            String sessionId = openCGAClient.login(user, password);
            if (StringUtils.isNotEmpty(sessionId)) {
                Map<String, List<String>> studies = new HashMap<>();
                QueryResponse<Project> projects = openCGAClient.getUserClient().getProjects(QueryOptions.empty());

                if (projects.getResponse().get(0).getNumResults() == 0) {
                    // We try to fetch shared projects and studies instead when the user does not owe any project or study
                    projects = openCGAClient.getUserClient().getProjects(new QueryOptions("shared", true));
                }
                for (Project project : projects.getResponse().get(0).getResult()) {
                    if (!studies.containsKey(project.getAlias())) {
                        studies.put(project.getAlias(), new ArrayList<>());
                    }

                    for (Study study : project.getStudies()) {
                        studies.get(project.getAlias()).add(study.getAlias());
                    }
                }
                // write CLI session file
                saveCliSessionFile(user, sessionId, studies);
                System.out.println("You have been logged correctly. This is your new session id " + sessionId);
            }
        } else {
            String sessionId = usersCommandOptions.commonCommandOptions.sessionId;
            String errorMsg = "Missing password. ";
            if (StringUtils.isNotEmpty(sessionId)) {
                errorMsg += "Active session id detected " + sessionId;
            }
            System.err.println(errorMsg);
        }
    }

    private void logout() throws IOException {
        logger.debug("Logout");
        openCGAClient.logout();
        logoutCliSessionFile();
    }

    private QueryResponse<User> create() throws CatalogException, IOException {
        logger.debug("Creating user...");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.NAME.key(), usersCommandOptions.createCommandOptions.name);
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.EMAIL.key(), usersCommandOptions.createCommandOptions.email);
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.PASSWORD.key(), usersCommandOptions.createCommandOptions.password);
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.ORGANIZATION.key(), usersCommandOptions.createCommandOptions.organization);

        return openCGAClient.getUserClient().create(usersCommandOptions.createCommandOptions.user,
                usersCommandOptions.createCommandOptions.password, params);
    }

    private QueryResponse<User> info() throws CatalogException, IOException {
        logger.debug("User info");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(usersCommandOptions.infoCommandOptions.userParam.user)) {
            queryOptions.putIfNotEmpty("userId", usersCommandOptions.infoCommandOptions.userParam.user);
        } else if (StringUtils.isNotEmpty(cliSession.getUserId())) {
            queryOptions.putIfNotEmpty("userId", cliSession.getUserId());
        }

        queryOptions.putIfNotEmpty(UserDBAdaptor.QueryParams.LAST_MODIFIED.key(), usersCommandOptions.infoCommandOptions.lastModified);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, usersCommandOptions.infoCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, usersCommandOptions.infoCommandOptions.dataModelOptions.exclude);

        QueryResponse<User> userQueryResponse = openCGAClient.getUserClient().get(queryOptions);
        if (userQueryResponse.getResponse().size() == 1 && userQueryResponse.getResponse().get(0).getNumResults() == 1) {
            queryOptions.put("shared", true);
            QueryResponse<Project> sharedProjects = openCGAClient.getUserClient().getProjects(queryOptions);
            if (sharedProjects.getResponse().size() > 0 && sharedProjects.getResponse().get(0).getNumResults() > 0) {
                QueryResult<User> userQueryResult = userQueryResponse.getResponse().get(0);
                List<Project> newProjectList = Stream
                        .concat(userQueryResult.first().getProjects().stream(), sharedProjects.first().getResult().stream())
                        .collect(Collectors.toList());
                userQueryResult.first().setProjects(newProjectList);
            }
        }

        return userQueryResponse;
    }

    private QueryResponse<Project> projects() throws CatalogException, IOException {
        logger.debug("List all projects and studies of user");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, usersCommandOptions.projectsCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, usersCommandOptions.projectsCommandOptions.dataModelOptions.exclude);
        queryOptions.put(QueryOptions.LIMIT, usersCommandOptions.projectsCommandOptions.numericOptions.limit);
        queryOptions.put(QueryOptions.SKIP, usersCommandOptions.projectsCommandOptions.numericOptions.skip);

        if (StringUtils.isNotEmpty(usersCommandOptions.projectsCommandOptions.userParam.user)) {
            queryOptions.putIfNotEmpty("userId", usersCommandOptions.projectsCommandOptions.userParam.user);
        } else {
            queryOptions.putIfNotEmpty("userId", cliSession.getUserId());
        }

        return openCGAClient.getUserClient().getProjects(queryOptions);
    }

    private void resetPasword() throws CatalogException, IOException {
        logger.debug("Resetting the user password and sending a new one to the e-mail stored in catalog.");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("userId", usersCommandOptions.resetPasswordCommandOptions.user);
        openCGAClient.getUserClient().resetPassword(params);
    }

    private void delete() throws CatalogException, IOException {
        System.out.println("Pending functionality");
        logger.debug("Deleting user");

//        openCGAClient.getUserClient().delete(usersCommandOptions.deleteCommandOptions.user, new ObjectMap());
    }

    private QueryResponse<User> update() throws CatalogException, IOException {
        logger.debug("Updating user");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.NAME.key(), usersCommandOptions.updateCommandOptions.name);
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.EMAIL.key(), usersCommandOptions.updateCommandOptions.email);
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.ORGANIZATION.key(), usersCommandOptions.updateCommandOptions.organization);
        params.putIfNotEmpty(UserDBAdaptor.QueryParams.ATTRIBUTES.key(), usersCommandOptions.updateCommandOptions.attributes);
        return openCGAClient.getUserClient().update(usersCommandOptions.updateCommandOptions.user, null, params);
    }

    private QueryResponse<User> changePassword () throws CatalogException, IOException {
        return openCGAClient.getUserClient().changePassword(usersCommandOptions.changePasswordCommandOptions.password,
                usersCommandOptions.changePasswordCommandOptions.npassword, new ObjectMap());
    }

}
