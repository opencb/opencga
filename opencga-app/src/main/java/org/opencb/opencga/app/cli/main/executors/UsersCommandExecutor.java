/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli.main.executors;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.UserCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by imedina on 02/03/15.
 */
public class UsersCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private UserCommandOptions usersCommandOptions;

    public UsersCommandExecutor(UserCommandOptions usersCommandOptions) {

        super(usersCommandOptions.commonCommandOptions, getParsedSubCommand(usersCommandOptions.getjCommander()).startsWith("log"));
        this.usersCommandOptions = usersCommandOptions;
    }

    public UsersCommandExecutor(OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions, UserCommandOptions usersCommandOptions) {

        super(commonOptions, getParsedSubCommand(usersCommandOptions.getjCommander()).startsWith("log"));
        this.usersCommandOptions = usersCommandOptions;
    }

    @Override
    public void execute() throws Exception {

        logger.debug("Executing variant command line");
//        openCGAClient = new OpenCGAClient(clientConfiguration);

        String subCommandString = getParsedSubCommand(usersCommandOptions.getjCommander());
        if (!subCommandString.equals("login") && !subCommandString.equals("logout")) {
            checkSessionValid();
        }

        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "projects":
                projects();
                break;
            case "login":
                login();
                break;
            case "logout":
                logout();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating user...");

        ObjectMap params = new ObjectMap()
                .append("name", usersCommandOptions.createCommandOptions.userName)
                .append("email", usersCommandOptions.createCommandOptions.userEmail)
                .append("password", usersCommandOptions.createCommandOptions.userPassword);

        if (usersCommandOptions.createCommandOptions.userOrganization != null) {
            params.append("organization", usersCommandOptions.createCommandOptions.userOrganization);
        }

        QueryResponse<User> userQueryResponse = openCGAClient.getUserClient().create(usersCommandOptions.createCommandOptions.user,
                usersCommandOptions.createCommandOptions.userPassword, params);

        if (userQueryResponse != null && userQueryResponse.first().getNumResults() == 1) {
            logger.info("User {} successfully created", usersCommandOptions.createCommandOptions.user);
        } else {
            logger.error("User {} could not be created due to ", usersCommandOptions.createCommandOptions.user,
                    userQueryResponse.getError());
            return;
        }

        openCGAClient.login(usersCommandOptions.createCommandOptions.user, usersCommandOptions.createCommandOptions.userPassword);

        logger.info("Creating project...");

        params = new ObjectMap();

        String alias = usersCommandOptions.createCommandOptions.projectAlias != null
                ? usersCommandOptions.createCommandOptions.projectAlias : "default";
        String name = usersCommandOptions.createCommandOptions.projectName != null
                ? usersCommandOptions.createCommandOptions.projectName : "Default";

        if (usersCommandOptions.createCommandOptions.projectDescription != null) {
            params.append("description", usersCommandOptions.createCommandOptions.projectDescription);
        }

        if (usersCommandOptions.createCommandOptions.projectOrganization != null) {
            params.append("description", usersCommandOptions.createCommandOptions.projectOrganization);
        }

        QueryResponse<Project> projectQueryResponse = openCGAClient.getProjectClient().create(name, alias, params);

        openCGAClient.logout();

        if (projectQueryResponse != null && projectQueryResponse.first().getNumResults() == 1) {
            logger.info("Project {} has been created successfully", name);
        } else {
            logger.error("Project {} could not be created due to ", name, projectQueryResponse.getError());
        }
    }

    private void info() throws CatalogException, IOException {
        logger.debug("User info");
        QueryResponse<User> user = openCGAClient.getUserClient().get(null);
        System.out.println("user = " + user);
    }

    private void projects() throws CatalogException, IOException {
        logger.debug("List all projects and studies of user");
        QueryResponse<Project> projects = openCGAClient.getUserClient().getProjects(null);
        System.out.println("projects = " + projects);
    }

    private void login() throws CatalogException, IOException {
        logger.debug("Login");

        String user = usersCommandOptions.loginCommandOptions.user;
        String password = usersCommandOptions.loginCommandOptions.password;

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            //  "hgva", "hgva_cafeina", clientConfiguration
            String session = openCGAClient.login(user, password);
            // write session file
            saveSessionFile(user, session);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        } else {
            String sessionId = usersCommandOptions.loginCommandOptions.sessionId;
            if (StringUtils.isNotEmpty(sessionId)) {
                openCGAClient.setSessionId(sessionId);
            } else {
                // load user session file

//                openCGAClient.setSessionId(sessionId);
            }
        }


    }

    private void logout() throws IOException {
        logger.debug("Logout");
        openCGAClient.logout();
        logoutSessionFile();
//        logoutSession();
    }

}
