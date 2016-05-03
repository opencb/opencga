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

package org.opencb.opencga.app.cli.admin;


import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.User;

import java.util.Collections;

/**
 * Created by imedina on 02/03/15.
 */
public class UsersCommandExecutor extends CommandExecutor {

    private CliOptionsParser.UsersCommandOptions usersCommandOptions;

    public UsersCommandExecutor(CliOptionsParser.UsersCommandOptions usersCommandOptions) {
        super(usersCommandOptions.commonOptions);
        this.usersCommandOptions = usersCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = usersCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "delete":
                delete();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException {
        if (usersCommandOptions.createUserCommandOptions.databaseUser != null) {
            configuration.getDatabase().setUser(usersCommandOptions.createUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.createUserCommandOptions.databasePassword != null) {
            configuration.getDatabase().setPassword(usersCommandOptions.createUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.createUserCommandOptions.database != null) {
            configuration.getDatabase().setDatabase(usersCommandOptions.createUserCommandOptions.database);
        }
        if (usersCommandOptions.createUserCommandOptions.hosts != null) {
            configuration.getDatabase().setHosts(Collections.singletonList(usersCommandOptions.createUserCommandOptions.hosts));
        }
        if (usersCommandOptions.commonOptions.password != null) {
            configuration.getAdmin().setPassword(usersCommandOptions.commonOptions.password);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        long userDiskQuota;
        if (usersCommandOptions.createUserCommandOptions.userDiskQuota != null) {
            userDiskQuota = usersCommandOptions.createUserCommandOptions.userDiskQuota;
        } else {
            userDiskQuota = configuration.getUserDefaultDiskQuota();
        }

        CatalogManager catalogManager = new CatalogManager(configuration);
        User user = catalogManager.createUser(usersCommandOptions.createUserCommandOptions.userId,
                usersCommandOptions.createUserCommandOptions.userName, usersCommandOptions.createUserCommandOptions.userEmail,
                usersCommandOptions.createUserCommandOptions.userPassword, usersCommandOptions.createUserCommandOptions.userOrganization,
                userDiskQuota, null).first();
        System.out.println("The user has been successfully created: " + user.toString());

    }

    private void delete() throws CatalogException {
        if (usersCommandOptions.deleteUserCommandOptions.databaseUser != null) {
            configuration.getDatabase().setUser(usersCommandOptions.deleteUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.deleteUserCommandOptions.databasePassword != null) {
            configuration.getDatabase().setPassword(usersCommandOptions.deleteUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.deleteUserCommandOptions.database != null) {
            configuration.getDatabase().setDatabase(usersCommandOptions.deleteUserCommandOptions.database);
        }
        if (usersCommandOptions.deleteUserCommandOptions.hosts != null) {
            configuration.getDatabase().setHosts(Collections.singletonList(usersCommandOptions.deleteUserCommandOptions.hosts));
        }
        if (usersCommandOptions.commonOptions.password != null) {
            configuration.getAdmin().setPassword(usersCommandOptions.commonOptions.password);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        CatalogManager catalogManager = new CatalogManager(configuration);
        User user = catalogManager.deleteUser(usersCommandOptions.deleteUserCommandOptions.userId,
                new QueryOptions("force", true), null).first();
        System.out.println("The user has been successfully deleted from the database: " + user.toString());
    }

}
