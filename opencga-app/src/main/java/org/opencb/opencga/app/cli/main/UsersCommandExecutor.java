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

package org.opencb.opencga.app.cli.main;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;

/**
 * Created by imedina on 02/03/15.
 */
public class UsersCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.UsersCommandOptions usersCommandOptions;

    public UsersCommandExecutor(OpencgaCliOptionsParser.UsersCommandOptions usersCommandOptions) {
        super(usersCommandOptions.commonOptions, usersCommandOptions.getParsedSubCommand().startsWith("log"));
        this.usersCommandOptions = usersCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");
//        openCGAClient = new OpenCGAClient(clientConfiguration);


        String subCommandString = usersCommandOptions.getParsedSubCommand();
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
            case "list":
                list();
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
        logger.debug("Creating user");

    }

    private void info() throws CatalogException, IOException {
        logger.debug("User info");
        QueryResponse<User> user = openCGAClient.getUserClient().get(usersCommandOptions.infoCommand.up.user, null);
        System.out.println("user = " + user);
    }

    private void list() throws CatalogException {
        logger.debug("List all projects and studies of user");
    }
    private void login() throws CatalogException, IOException {
        logger.debug("Login");

        String user = usersCommandOptions.loginCommand.up.user;
        String password = usersCommandOptions.loginCommand.up.password;

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
            String sessionId = usersCommandOptions.loginCommand.up.sessionId;
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
        logoutSession();
    }

}
