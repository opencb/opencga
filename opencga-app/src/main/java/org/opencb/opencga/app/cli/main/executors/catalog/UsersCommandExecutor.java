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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.UserCommandOptions;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;

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
                create();
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

    private void create() throws CatalogException, IOException {
        logger.debug("Creating user...");

        ObjectMap params = new ObjectMap()
                .append(UserDBAdaptor.QueryParams.NAME.key(), usersCommandOptions.createCommandOptions.userName)
                .append(UserDBAdaptor.QueryParams.EMAIL.key(), usersCommandOptions.createCommandOptions.userEmail)
                .append(UserDBAdaptor.QueryParams.PASSWORD.key(), usersCommandOptions.createCommandOptions.userPassword);

        if (usersCommandOptions.createCommandOptions.userOrganization != null) {
            params.append(UserDBAdaptor.QueryParams.ORGANIZATION.key(), usersCommandOptions.createCommandOptions.userOrganization);
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
            params.append("organization", usersCommandOptions.createCommandOptions.projectOrganization);
        }

        QueryResponse<Project> projectQueryResponse = openCGAClient.getProjectClient().create(name, alias, params);

        openCGAClient.logout();

        if (projectQueryResponse != null && projectQueryResponse.first().getNumResults() == 1) {
            logger.info("Project {} has been created successfully", name);
        } else {
            logger.error("Project {} could not be created due to ", name, projectQueryResponse.getError());
        }
    }

    private QueryResponse<User> info() throws CatalogException, IOException {
        logger.debug("User info");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, usersCommandOptions.infoCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, usersCommandOptions.infoCommandOptions.exclude);
        queryOptions.putIfNotEmpty(UserDBAdaptor.QueryParams.LAST_MODIFIED.key(), usersCommandOptions.infoCommandOptions.lastModified);

        return openCGAClient.getUserClient().get(queryOptions);
    }

    private QueryResponse<Project> projects() throws CatalogException, IOException {
        logger.debug("List all projects and studies of user");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, usersCommandOptions.projectsCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, usersCommandOptions.projectsCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, usersCommandOptions.projectsCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, usersCommandOptions.projectsCommandOptions.skip);

        return openCGAClient.getUserClient().getProjects(queryOptions);
    }

    private void login() throws CatalogException, IOException {
        logger.debug("Login");

        String user = usersCommandOptions.loginCommandOptions.user;
        String password = usersCommandOptions.loginCommandOptions.password;

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            //  "hgva", "hgva_cafeina", clientConfiguration
            String session = openCGAClient.login(user, password);
            saveSessionFile(user, session);
            System.out.println("You have been logged correctly. This is your new session id " + session + ".");
            // write session file

//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

        } else {
            String sessionId = usersCommandOptions.loginCommandOptions.sessionId;
            if (StringUtils.isNotEmpty(sessionId)) {
                openCGAClient.setSessionId(sessionId);
                System.out.println("You have been logged correctly. This is your new session id " + sessionId + ".");
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

    private void resetPasword() throws CatalogException, IOException {
        logger.debug("Resetting the user password and sending a new one to the e-mail stored in catalog.");
        openCGAClient.getUserClient().resetPassword(new ObjectMap());
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting user");
        openCGAClient.getUserClient().delete(usersCommandOptions.deleteCommandOptions.user, new ObjectMap());
    }

    private QueryResponse<User> update() throws CatalogException, IOException {
        logger.debug("Updating user");

        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(UserDBAdaptor.QueryParams.NAME.key(), usersCommandOptions.updateCommandOptions.name);
        objectMap.putIfNotEmpty(UserDBAdaptor.QueryParams.EMAIL.key(), usersCommandOptions.updateCommandOptions.email);
        objectMap.putIfNotEmpty(UserDBAdaptor.QueryParams.ORGANIZATION.key(), usersCommandOptions.updateCommandOptions.organization);
        objectMap.putIfNotEmpty(UserDBAdaptor.QueryParams.ATTRIBUTES.key(), usersCommandOptions.updateCommandOptions.attributes);
        objectMap.putIfNotEmpty("configs", usersCommandOptions.updateCommandOptions.configs);

        return openCGAClient.getUserClient().update(usersCommandOptions.updateCommandOptions.user, objectMap);
    }

    private QueryResponse<User> changePassword () throws CatalogException, IOException {
        return openCGAClient.getUserClient().changePassword(usersCommandOptions.changePaswordCommandOptions.password,
                usersCommandOptions.changePaswordCommandOptions.npassword, new ObjectMap());
    }

}
