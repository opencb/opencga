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

package org.opencb.opencga.app.cli.admin;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.GroupParams;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.core.results.LdapImportResult;

import javax.naming.NamingException;
import java.io.IOException;
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
                importUsersAndGroups();
                break;
            case "sync":
                syncGroups();
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

    private void syncGroups() throws CatalogException, NamingException, IOException {
        AdminCliOptionsParser.SyncCommandOptions executor = usersCommandOptions.syncCommandOptions;

        setCatalogDatabaseCredentials(executor.databaseHost, executor.prefix, executor.databaseUser, executor.databasePassword,
                executor.commonOptions.adminPassword);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            QueryResult<Session> login = catalogManager.login("admin", configuration.getAdmin().getPassword(), "localhost");
            String sessionId = login.first().getId();

            if (executor.syncAll) {
                QueryResult<Group> allGroups = catalogManager.getStudyManager().getGroup(executor.study, null, sessionId);

                boolean foundAny = false;
                for (Group group : allGroups.getResult()) {
                    if (group.getSyncedFrom() != null && group.getSyncedFrom().getAuthOrigin().equals(executor.authOrigin)) {
                        foundAny = true;
                        logger.info("Synchronising users from {} to {}", group.getSyncedFrom().getRemoteGroup(), group.getName());

                        // Sync
                        GroupParams groupParams = new GroupParams(StringUtils.join(group.getUserIds(), ","), GroupParams.Action.REMOVE);
                        QueryResult<Group> deleteUsers = catalogManager.getStudyManager()
                                .updateGroup(executor.study, group.getName(), groupParams, sessionId);
                        if (deleteUsers.first().getUserIds().size() > 0) {
                            logger.error("Could not sync. An internal error happened. {} users could not be removed from {}.",
                                    deleteUsers.first().getUserIds().size(), deleteUsers.first().getName());
                            return;
                        }

                        ObjectMap params = new ObjectMap();
                        params.putIfNotNull("group", group.getSyncedFrom().getRemoteGroup());
                        params.putIfNotNull("study-group", group.getName());
                        params.putIfNotNull("study", executor.study);
                        params.putIfNotNull("expirationDate", executor.expDate);
                        LdapImportResult ldapImportResult = catalogManager.getUserManager().importFromExternalAuthOrigin(executor.authOrigin,
                                executor.type, params, configuration.getAdmin().getPassword());

                        printImportReport(ldapImportResult);
                    }
                }
                if (!foundAny) {
                    logger.info("No groups to sync found under study {}", executor.study);
                }
            } else {
                try {
                    QueryResult<Group> group = catalogManager.getStudyManager().getGroup(executor.study, executor.to, sessionId);
                    if (group.first().getSyncedFrom() != null && (!group.first().getSyncedFrom().getRemoteGroup().equals(executor.from)
                            || !group.first().getSyncedFrom().getAuthOrigin().equals(executor.authOrigin))) {
                        // Sync with different group or different authentication origin
                        logger.error("Cannot synchronise with group {}. The group is already synchronised with the group {} from the "
                                + "authentication origin {}", executor.to, group.first().getSyncedFrom().getRemoteGroup(),
                                group.first().getSyncedFrom().getAuthOrigin());
                        return;
                    } else if (group.first().getSyncedFrom() == null && !executor.force) {
                        logger.error("Cannot synchronise with group {}. The group already exists. You can use --force to force the "
                                + "synchronisation with this group.", executor.to);
                        return;
                    }

                    // Remove all users from the group
                    GroupParams groupParams = new GroupParams(StringUtils.join(group.first().getUserIds(), ","), GroupParams.Action.REMOVE);
                    QueryResult<Group> deleteUsers = catalogManager.getStudyManager().updateGroup(executor.study, executor.to, groupParams,
                            sessionId);
                    if (deleteUsers.first().getUserIds().size() > 0) {
                        logger.error("Could not sync. An internal error happened. {} users could not be removed from {}.",
                                deleteUsers.first().getUserIds().size(), deleteUsers.first().getName());
                        return;
                    }

                } catch (CatalogException e) {
                    logger.info("{} group does not exist.", executor.to, e.getMessage());
                }

                ObjectMap params = new ObjectMap();
                params.putIfNotNull("group", executor.from);
                params.putIfNotNull("study-group", executor.to);
                params.putIfNotNull("study", executor.study);
                params.putIfNotNull("expirationDate", executor.expDate);
                LdapImportResult ldapImportResult = catalogManager.getUserManager().importFromExternalAuthOrigin(executor.authOrigin,
                        executor.type, params, configuration.getAdmin().getPassword());

                printImportReport(ldapImportResult);

                if (StringUtils.isEmpty(ldapImportResult.getErrorMsg()) && StringUtils.isEmpty(ldapImportResult.getWarningMsg())) {
                    Group.Sync sync = new Group.Sync(executor.authOrigin, executor.from);
                    catalogManager.getStudyManager().syncGroupWith(executor.study, executor.to, sync, sessionId);

                    logger.info("{} synchronised with {}", executor.from, executor.to);
                }
            }
        }
    }

    private void importUsersAndGroups() throws CatalogException, NamingException {
        AdminCliOptionsParser.ImportCommandOptions executor = usersCommandOptions.importCommandOptions;

        setCatalogDatabaseCredentials(executor.databaseHost, executor.prefix, executor.databaseUser, executor.databasePassword,
                executor.commonOptions.adminPassword);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            ObjectMap params = new ObjectMap();
            params.putIfNotNull("users", executor.user);
            params.putIfNotNull("group", executor.group);
            params.putIfNotNull("study", executor.study);
            params.putIfNotNull("study-group", executor.studyGroup);
            params.putIfNotNull("expirationDate", executor.expDate);
            LdapImportResult ldapImportResult = catalogManager.getUserManager()
                    .importFromExternalAuthOrigin(executor.authOrigin, executor.type, params, configuration.getAdmin().getPassword());

            printImportReport(ldapImportResult);
        }
    }

    private void printImportReport(LdapImportResult ldapImportResult) {
        if (ldapImportResult.getResult() != null) {
            LdapImportResult.SummaryResult userSummary = ldapImportResult.getResult().getUserSummary();
            if (userSummary != null) {
                if (userSummary.getNewUsers().size() > 0) {
                    System.out.println("New users registered: " + StringUtils.join(userSummary.getNewUsers(), ", "));
                }
                if (userSummary.getExistingUsers().size() > 0) {
                    System.out.println("Users already registered: " + StringUtils.join(userSummary.getExistingUsers(), ", "));
                }
                if (userSummary.getNonExistingUsers().size() > 0) {
                    System.out.println("Users not found in origin: " + StringUtils.join(userSummary.getNonExistingUsers(), ", "));
                }
            }
            List<String> usersInGroup = ldapImportResult.getResult().getUsersInGroup();
            if (usersInGroup != null) {
                System.out.println("Users registered in group " + ldapImportResult.getInput().getStudyGroup() + " from "
                        + ldapImportResult.getInput().getStudy() + ": " + StringUtils.join(usersInGroup, ", "));
            }
        }

        if (StringUtils.isNotEmpty(ldapImportResult.getWarningMsg())) {
            System.out.println("WARNING: " + ldapImportResult.getWarningMsg());
        }

        if (StringUtils.isNotEmpty(ldapImportResult.getErrorMsg())) {
            System.out.println("ERROR: " + ldapImportResult.getErrorMsg());
        }
    }

    private void create() throws CatalogException, IOException {
        setCatalogDatabaseCredentials(usersCommandOptions.createUserCommandOptions.databaseHost,
                usersCommandOptions.createUserCommandOptions.prefix, usersCommandOptions.createUserCommandOptions.databaseUser,
                usersCommandOptions.createUserCommandOptions.databasePassword,
                usersCommandOptions.createUserCommandOptions.commonOptions.adminPassword);

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
        setCatalogDatabaseCredentials(usersCommandOptions.deleteUserCommandOptions.databaseHost,
                usersCommandOptions.deleteUserCommandOptions.prefix, usersCommandOptions.deleteUserCommandOptions.databaseUser,
                usersCommandOptions.deleteUserCommandOptions.databasePassword,
                usersCommandOptions.deleteUserCommandOptions.commonOptions.adminPassword);

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
        setCatalogDatabaseCredentials(usersCommandOptions.QuotaUserCommandOptions.databaseHost,
                usersCommandOptions.QuotaUserCommandOptions.prefix, usersCommandOptions.QuotaUserCommandOptions.databaseUser,
                usersCommandOptions.QuotaUserCommandOptions.databasePassword,
                usersCommandOptions.QuotaUserCommandOptions.commonOptions.adminPassword);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            catalogManager.getUserManager().validatePassword("admin", configuration.getAdmin().getPassword(), true);

            User user = catalogManager.modifyUser(usersCommandOptions.QuotaUserCommandOptions.userId,
                    new ObjectMap(UserDBAdaptor.QueryParams.QUOTA.key(), usersCommandOptions.QuotaUserCommandOptions.quota * 1073741824),
                    null).first();

            System.out.println("The disk quota has been properly updated: " + user.toString());
        }
    }

    private AuthenticationOrigin getAuthenticationOrigin(String authOrigin) {
        if (configuration.getAuthentication().getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : configuration.getAuthentication().getAuthenticationOrigins()) {
                if (authOrigin.equals(authenticationOrigin.getId())) {
                    return authenticationOrigin;
                }
            }
        }
        return null;
    }

}
