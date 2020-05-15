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
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.UserCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.template.TemplateManager;
import org.opencb.opencga.client.template.config.TemplateConfiguration;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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

        String subCommandString = getParsedSubCommand(usersCommandOptions.getjCommander());
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "update":
                queryResponse = update();
                break;
            case "password":
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
            case "template":
                loadTemplate();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private void login() throws ClientException, IOException {
        logger.debug("Login");

        String user = usersCommandOptions.loginCommandOptions.user;
        String password = usersCommandOptions.loginCommandOptions.password;

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            AuthenticationResponse response = openCGAClient.login(user, password);
            if (response != null) {
                List<String> studies = new ArrayList<>();

                RestResponse<Project> projects = openCGAClient.getProjectClient().search(
                        new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

                if (projects.getResponses().get(0).getNumResults() == 0) {
                    // We try to fetch shared projects and studies instead when the user does not owe any project or study
                    projects = openCGAClient.getProjectClient().search(new ObjectMap());
                }
                for (Project project : projects.getResponses().get(0).getResults()) {
                    for (Study study : project.getStudies()) {
                        studies.add(study.getFqn());
                    }
                }
                // write CLI session file
                saveCliSessionFile(user, response.getToken(), response.getRefreshToken(), studies);
                System.out.println("You have been logged in correctly. This is your new token " + response.getToken());
            }
        } else {
            String sessionId = usersCommandOptions.commonCommandOptions.token;
            String errorMsg = "Missing password. ";
            if (StringUtils.isNotEmpty(sessionId)) {
                errorMsg += "Active token detected " + sessionId;
            }
            System.err.println(errorMsg);
        }
    }

    private void logout() throws IOException {
        logger.debug("Logout");
        openCGAClient.logout();
        logoutCliSessionFile();
    }

    private RestResponse<User> create() throws ClientException {
        logger.debug("Creating user...");

        UserCommandOptions.CreateCommandOptions c = usersCommandOptions.createCommandOptions;

        UserCreateParams createParams = new UserCreateParams()
                .setId(c.user)
                .setName(c.name)
                .setEmail(c.email)
                .setOrganization(c.organization)
                .setPassword(c.password);

        return openCGAClient.getUserClient().create(createParams);
    }

    private RestResponse<User> info() throws ClientException {
        logger.debug("User info");

        UserCommandOptions.InfoCommandOptions c = usersCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        String userId;
        if (StringUtils.isNotEmpty(c.userParam.user)) {
            userId = c.userParam.user;
        } else if (cliSession != null && StringUtils.isNotEmpty(cliSession.getUser())) {
            userId = cliSession.getUser();
        } else {
            throw new ClientException("Missing user parameter");
        }

        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);

        RestResponse<User> userQueryResponse = openCGAClient.getUserClient().info(userId, params);
        if (userQueryResponse.getResponses().size() == 1 && userQueryResponse.getResponses().get(0).getNumResults() == 1) {
            params.put("shared", true);
            RestResponse<Project> sharedProjects = openCGAClient.getUserClient().projects(userId, params);
            if (sharedProjects.getResponses().size() > 0 && sharedProjects.getResponses().get(0).getNumResults() > 0) {
                OpenCGAResult<User> userQueryResult = userQueryResponse.getResponses().get(0);
                List<Project> newProjectList = Stream
                        .concat(userQueryResult.first().getProjects().stream(), sharedProjects.first().getResults().stream())
                        .collect(Collectors.toList());
                userQueryResult.first().setProjects(newProjectList);
            }
        }

        return userQueryResponse;
    }

    private RestResponse<Project> projects() throws ClientException {
        logger.debug("List all projects and studies of user");

        UserCommandOptions.ProjectsCommandOptions c = usersCommandOptions.projectsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, c.numericOptions.limit);
        params.put(QueryOptions.SKIP, c.numericOptions.skip);

        String userId;
        if (StringUtils.isNotEmpty(c.userParam.user)) {
            userId = c.userParam.user;
        } else if (cliSession != null) {
            userId = cliSession.getUser();
        } else {
            throw new ClientException("Missing user parameter");
        }

        return openCGAClient.getUserClient().projects(userId, params);
    }

    private RestResponse<User> update() throws ClientException, CatalogException {
        logger.debug("Updating user");

        UserCommandOptions.UpdateCommandOptions options = usersCommandOptions.updateCommandOptions;
        UserUpdateParams params = new UserUpdateParams(options.name, options.email, options.organization, null);
        return openCGAClient.getUserClient().update(usersCommandOptions.updateCommandOptions.user, params);
    }

    private RestResponse<User> changePassword () throws ClientException, IOException {
        UserCommandOptions.ChangePasswordCommandOptions c = usersCommandOptions.changePasswordCommandOptions;

        PasswordChangeParams changeParams = new PasswordChangeParams(c.user, c.password, c.npassword);
        return openCGAClient.getUserClient().password(changeParams);
    }

    private void loadTemplate() throws IOException, ClientException {
        UserCommandOptions.TemplateCommandOptions options = usersCommandOptions.templateCommandOptions;

        TemplateConfiguration template = TemplateConfiguration.load(Paths.get(options.file));
        TemplateManager templateManager = new TemplateManager(clientConfiguration, options.resume, cliSession.getToken());
        if (options.validate) {
            templateManager.validate(template);
        } else {
            templateManager.execute(template);
        }
    }

}
