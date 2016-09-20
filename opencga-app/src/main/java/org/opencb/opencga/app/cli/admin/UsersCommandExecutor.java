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


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
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
            case "disk-quota":
                setDiskQuota();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void importUsers() throws CatalogException, NamingException {
        AdminCliOptionsParser.ImportUserCommandOptions executor = usersCommandOptions.importUserCommandOptions;
        if (executor.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(executor.databaseUser);
        }
        if (executor.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(executor.databasePassword);
        }
        if (executor.database != null) {
            catalogConfiguration.getDatabase().setDatabase(executor.database);
        }
        if (executor.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(executor.databaseHost));
        }
        if (executor.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(executor.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert the OpenCGA admin password.");
        }

        if (executor.groups != null) {
            throw new CatalogException("Groups option is pending. Use users instead.");
        }

        if (executor.users == null) {
            throw new CatalogException("At least, users or groups should be provided to start importing.");
        }

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("users", executor.users);
        params.putIfNotNull("groups", executor.groups);
        params.putIfNotNull("expirationDate", executor.expDate);
        params.putIfNotNull("studies", executor.studies);
        List<QueryResult<User>> resultList = catalogManager.getUserManager().importFromExternalAuthOrigin(executor.authOrigin,
                executor.type, params, catalogConfiguration.getAdmin().getPassword());

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

    private void create() throws CatalogException, IOException {
        if (usersCommandOptions.createUserCommandOptions.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(usersCommandOptions.createUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.createUserCommandOptions.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(usersCommandOptions.createUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.createUserCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(usersCommandOptions.createUserCommandOptions.database);
        }
        if (usersCommandOptions.createUserCommandOptions.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(usersCommandOptions.createUserCommandOptions.databaseHost));
        }
        if (usersCommandOptions.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(usersCommandOptions.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert the OpenCGA admin password.");
        }

        long userDiskQuota;
        if (usersCommandOptions.createUserCommandOptions.userDiskQuota != null) {
            userDiskQuota = usersCommandOptions.createUserCommandOptions.userDiskQuota;
        } else {
            userDiskQuota = catalogConfiguration.getUserDefaultDiskQuota();
        }

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.getUserManager().validatePassword("admin", catalogConfiguration.getAdmin().getPassword(), true);

        User user = catalogManager.createUser(usersCommandOptions.createUserCommandOptions.userId,
                usersCommandOptions.createUserCommandOptions.userName, usersCommandOptions.createUserCommandOptions.userEmail,
                usersCommandOptions.createUserCommandOptions.userPassword, usersCommandOptions.createUserCommandOptions.userOrganization,
                userDiskQuota, null).first();
        System.out.println("The user has been successfully created: " + user.toString() + "\n");

        // Login the user
        ObjectMap login = catalogManager.login(usersCommandOptions.createUserCommandOptions.userId,
                usersCommandOptions.createUserCommandOptions.userPassword, "localhost").first();

        String projectName = "Default";
        if (usersCommandOptions.createUserCommandOptions.projectName != null
                && !usersCommandOptions.createUserCommandOptions.projectName.isEmpty()) {
            projectName = usersCommandOptions.createUserCommandOptions.projectName;
        }

        String projectAlias = "default";
        if (usersCommandOptions.createUserCommandOptions.projectAlias != null
                && !usersCommandOptions.createUserCommandOptions.projectAlias.isEmpty()) {
            projectAlias = usersCommandOptions.createUserCommandOptions.projectAlias;
        }

        String projectDescription = "";
        if (usersCommandOptions.createUserCommandOptions.projectDescription != null
                && !usersCommandOptions.createUserCommandOptions.projectDescription.isEmpty()) {
            projectDescription = usersCommandOptions.createUserCommandOptions.projectDescription;
        }

        String projectOrganization = "";
        if (usersCommandOptions.createUserCommandOptions.projectOrganization != null
                && !usersCommandOptions.createUserCommandOptions.projectOrganization.isEmpty()) {
            projectOrganization = usersCommandOptions.createUserCommandOptions.projectOrganization;
        }

        Project project = catalogManager.createProject(projectName, projectAlias,
                projectDescription, projectOrganization, null, login.getString("sessionId")).first();
        System.out.println("A default project has been created for the user: " + project.toString() + "\n");

        catalogManager.logout(usersCommandOptions.createUserCommandOptions.userId, login.getString("sessionId"));
    }

    private void delete() throws CatalogException, IOException {
        if (usersCommandOptions.deleteUserCommandOptions.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(usersCommandOptions.deleteUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.deleteUserCommandOptions.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(usersCommandOptions.deleteUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.deleteUserCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(usersCommandOptions.deleteUserCommandOptions.database);
        }
        if (usersCommandOptions.deleteUserCommandOptions.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(usersCommandOptions.deleteUserCommandOptions.databaseHost));
        }
        if (usersCommandOptions.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(usersCommandOptions.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.getUserManager().validatePassword("admin", catalogConfiguration.getAdmin().getPassword(), true);

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

    private void setDiskQuota() throws CatalogException {
        if (usersCommandOptions.diskQuotaUserCommandOptions.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(usersCommandOptions.diskQuotaUserCommandOptions.databaseUser);
        }
        if (usersCommandOptions.diskQuotaUserCommandOptions.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(usersCommandOptions.diskQuotaUserCommandOptions.databasePassword);
        }
        if (usersCommandOptions.diskQuotaUserCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(usersCommandOptions.diskQuotaUserCommandOptions.database);
        }
        if (usersCommandOptions.diskQuotaUserCommandOptions.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(usersCommandOptions.diskQuotaUserCommandOptions.databaseHost));
        }
        if (usersCommandOptions.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(usersCommandOptions.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.getUserManager().validatePassword("admin", catalogConfiguration.getAdmin().getPassword(), true);
        
        User user = catalogManager.modifyUser(usersCommandOptions.diskQuotaUserCommandOptions.userId,
                new ObjectMap(UserDBAdaptor.QueryParams.DISK_QUOTA.key(),
                        usersCommandOptions.diskQuotaUserCommandOptions.diskQuota *  1073741824), null).first();
        System.out.println("The disk quota has been properly updated: " + user.toString());
    }

}
