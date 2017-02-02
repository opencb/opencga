/*
 * Copyright 2015-2016 OpenCB
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


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.User;

import javax.naming.NamingException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 02/03/15.
 */
public class UsersCommandExecutor extends AdminCommandExecutor {

    private AdminCliOptionsParser.UsersCommandOptions usersCommandOptions;

    public UsersCommandExecutor(AdminCliOptionsParser.UsersCommandOptions usersCommandOptions) {
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
            case "import":
                importUsers();
                break;
            case "delete":
                delete();
                break;
            case "quota":
                setQuota();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void importUsers() throws CatalogException, NamingException {
        AdminCliOptionsParser.ImportUserCommandOptions executor = usersCommandOptions.importUserCommandOptions;
        if (executor.databaseUser != null) {
            configuration.getCatalog().getDatabase().setUser(executor.databaseUser);
        }
        if (executor.databasePassword != null) {
            configuration.getCatalog().getDatabase().setPassword(executor.databasePassword);
        }
        if (executor.database != null) {
            configuration.getCatalog().getDatabase().setDatabase(executor.database);
        }
        if (executor.databaseHost != null) {
            configuration.getCatalog().getDatabase().setHosts(Collections.singletonList(executor.databaseHost));
        }
        if (executor.commonOptions.adminPassword != null) {
            configuration.getAdmin().setPassword(executor.commonOptions.adminPassword);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert the OpenCGA admin password.");
        }

        if (executor.groups != null) {
            throw new CatalogException("Groups option is pending. Use users instead.");
        }

        if (executor.users == null) {
            throw new CatalogException("At least, users or groups should be provided to start importing.");
        }

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            ObjectMap params = new ObjectMap();
            params.putIfNotNull("users", executor.users);
            params.putIfNotNull("groups", executor.groups);
            params.putIfNotNull("expirationDate", executor.expDate);
            params.putIfNotNull("studies", executor.studies);
            List<QueryResult<User>> resultList = catalogManager.getUserManager().importFromExternalAuthOrigin(executor.authOrigin,
                    executor.type, params, configuration.getAdmin().getPassword());


            System.out.println("\n" + resultList.size() + " users have been imported");
            // Print the user names if less than 10 users have been imported.
            if (resultList.size() <= 10) {
                for (QueryResult<User> userQueryResult : resultList) {
                    if (userQueryResult.getNumResults() == 0) {
                        System.out.println(userQueryResult.getErrorMsg());
                    } else {
                        System.out.println(userQueryResult.first().getName() + " user account created.");
                    }
                }
            }
        }
    }

    private void create() throws CatalogException, IOException {
        if (usersCommandOptions.createUserCommandOptions.databaseUser != null) {
            configuration.getCatalog().getDatabase().setUser(usersCommandOptions.createUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.createUserCommandOptions.databasePassword != null) {
            configuration.getCatalog().getDatabase().setPassword(usersCommandOptions.createUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.createUserCommandOptions.database != null) {
            configuration.getCatalog().getDatabase().setDatabase(usersCommandOptions.createUserCommandOptions.database);
        }
        if (usersCommandOptions.createUserCommandOptions.databaseHost != null) {
            configuration.getCatalog().getDatabase()
                    .setHosts(Collections.singletonList(usersCommandOptions.createUserCommandOptions.databaseHost));
        }
        if (usersCommandOptions.commonOptions.adminPassword != null) {
            configuration.getAdmin().setPassword(usersCommandOptions.commonOptions.adminPassword);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert the OpenCGA admin password.");
        }

        long userQuota;
        if (usersCommandOptions.createUserCommandOptions.userQuota != null) {
            userQuota = usersCommandOptions.createUserCommandOptions.userQuota;
        } else {
            userQuota = configuration.getUserDefaultQuota();
        }

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            catalogManager.getUserManager().validatePassword("admin", configuration.getAdmin().getPassword(), true);

            User user = catalogManager.getUserManager().create(usersCommandOptions.createUserCommandOptions.userId,
                    usersCommandOptions.createUserCommandOptions.userName, usersCommandOptions.createUserCommandOptions.userEmail,
                    usersCommandOptions.createUserCommandOptions.userPassword,
                    usersCommandOptions.createUserCommandOptions.userOrganization, userQuota,
                    usersCommandOptions.createUserCommandOptions.type, null).first();

            System.out.println("The user has been successfully created: " + user.toString() + "\n");
        }
    }

    private void delete() throws CatalogException, IOException {
        if (usersCommandOptions.deleteUserCommandOptions.databaseUser != null) {
            configuration.getCatalog().getDatabase().setUser(usersCommandOptions.deleteUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.deleteUserCommandOptions.databasePassword != null) {
            configuration.getCatalog().getDatabase().setPassword(usersCommandOptions.deleteUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.deleteUserCommandOptions.database != null) {
            configuration.getCatalog().getDatabase().setDatabase(usersCommandOptions.deleteUserCommandOptions.database);
        }
        if (usersCommandOptions.deleteUserCommandOptions.databaseHost != null) {
            configuration.getCatalog().getDatabase()
                    .setHosts(Collections.singletonList(usersCommandOptions.deleteUserCommandOptions.databaseHost));
        }
        if (usersCommandOptions.commonOptions.adminPassword != null) {
            configuration.getAdmin().setPassword(usersCommandOptions.commonOptions.adminPassword);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            catalogManager.getUserManager().validatePassword("admin", configuration.getAdmin().getPassword(), true);

            List<QueryResult<User>> deletedUsers = catalogManager.getUserManager()
                    .delete(usersCommandOptions.deleteUserCommandOptions.userId, new QueryOptions("force", true), null);
            for (QueryResult<User> deletedUser : deletedUsers) {
                User user = deletedUser.first();
                if (user != null) {
                    System.out.println("The user has been successfully deleted from the database: " + user.toString());
                } else {
                    System.out.println(deletedUser.getErrorMsg());
                }
            }
        }
    }

    private void setQuota() throws CatalogException {
        if (usersCommandOptions.QuotaUserCommandOptions.databaseUser != null) {
            configuration.getCatalog().getDatabase().setUser(usersCommandOptions.QuotaUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.QuotaUserCommandOptions.databasePassword != null) {
            configuration.getCatalog().getDatabase().setPassword(usersCommandOptions.QuotaUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.QuotaUserCommandOptions.database != null) {
            configuration.getCatalog().getDatabase().setDatabase(usersCommandOptions.QuotaUserCommandOptions.database);
        }
        if (usersCommandOptions.QuotaUserCommandOptions.databaseHost != null) {
            configuration.getCatalog().getDatabase()
                    .setHosts(Collections.singletonList(usersCommandOptions.QuotaUserCommandOptions.databaseHost));
        }
        if (usersCommandOptions.commonOptions.adminPassword != null) {
            configuration.getAdmin().setPassword(usersCommandOptions.commonOptions.adminPassword);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            catalogManager.getUserManager().validatePassword("admin", configuration.getAdmin().getPassword(), true);

            User user = catalogManager.modifyUser(usersCommandOptions.QuotaUserCommandOptions.userId,
                    new ObjectMap(UserDBAdaptor.QueryParams.QUOTA.key(), usersCommandOptions.QuotaUserCommandOptions.quota * 1073741824),
                    null).first();

            System.out.println("The disk quota has been properly updated: " + user.toString());
        }
    }

}
