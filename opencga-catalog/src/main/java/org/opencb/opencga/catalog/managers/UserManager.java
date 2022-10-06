/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.AzureADAuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.LDAPAuthenticationManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.ParamUtils.checkEmail;
import static org.opencb.opencga.core.models.common.Enums.Resource.STUDY;
import static org.opencb.opencga.core.models.common.Enums.Resource.USER;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserManager extends AbstractManager {

    static final QueryOptions INCLUDE_ACCOUNT = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            UserDBAdaptor.QueryParams.ID.key(), UserDBAdaptor.QueryParams.ACCOUNT.key()));
    protected static Logger logger = LoggerFactory.getLogger(UserManager.class);
    private final CatalogIOManager catalogIOManager;
    private final String INTERNAL_AUTHORIZATION = CatalogAuthenticationManager.INTERNAL;
    private final Map<String, AuthenticationManager> authenticationManagerMap;

    UserManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration)
            throws CatalogException {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.catalogIOManager = catalogIOManager;

        String secretKey = configuration.getAdmin().getSecretKey();
        long expiration = configuration.getAuthentication().getExpiration();

        authenticationManagerMap = new LinkedHashMap<>();
        if (configuration.getAuthentication().getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : configuration.getAuthentication().getAuthenticationOrigins()) {
                if (authenticationOrigin.getId() != null) {
                    switch (authenticationOrigin.getType()) {
                        case LDAP:
                            authenticationManagerMap.put(authenticationOrigin.getId(),
                                    new LDAPAuthenticationManager(authenticationOrigin, secretKey, expiration));
                            break;
                        case AzureAD:
                            authenticationManagerMap.put(authenticationOrigin.getId(),
                                    new AzureADAuthenticationManager(authenticationOrigin));
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        // Even if internal authentication is not present in the configuration file, create it
        authenticationManagerMap.putIfAbsent(INTERNAL_AUTHORIZATION,
                new CatalogAuthenticationManager(catalogDBAdaptorFactory, configuration.getEmail(), secretKey, expiration));
        AuthenticationOrigin authenticationOrigin = new AuthenticationOrigin();
        if (configuration.getAuthentication().getAuthenticationOrigins() == null) {
            configuration.getAuthentication().setAuthenticationOrigins(Collections.singletonList(authenticationOrigin));
        } else {
            // Check if OPENCGA authentication is already present in catalog configuration
            boolean catalogPresent = false;
            for (AuthenticationOrigin origin : configuration.getAuthentication().getAuthenticationOrigins()) {
                if (AuthenticationOrigin.AuthenticationType.OPENCGA == origin.getType()) {
                    catalogPresent = true;
                    break;
                }
            }
            if (!catalogPresent) {
                List<AuthenticationOrigin> linkedList = new LinkedList<>();
                linkedList.addAll(configuration.getAuthentication().getAuthenticationOrigins());
                linkedList.add(authenticationOrigin);
                configuration.getAuthentication().setAuthenticationOrigins(linkedList);
            }
        }
    }

    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("oldPassword", "**********")
                .append("newPassword", "**********");
        run(auditParams, Enums.Action.CHANGE_USER_PASSWORD, USER, "", "", null, (study, userId1, rp, queryOptions) -> {
            rp.setId(userId);

            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(oldPassword, "oldPassword");
            ParamUtils.checkParameter(newPassword, "newPassword");

            if (oldPassword.equals(newPassword)) {
                throw new CatalogException("New password is the same as the old password.");
            }

            userDBAdaptor.checkId(userId);
            String authOrigin = getAuthenticationOriginId(userId);
            authenticationManagerMap.get(authOrigin).changePassword(userId, oldPassword, newPassword);
            return null;
        });
    }

    public OpenCGAResult<User> create(User user, String password, @Nullable String token) throws CatalogException {
        // Check if the users can be registered publicly or just the admin.
        ObjectMap auditParams = new ObjectMap()
                .append("user", user)
                .append("password", "********")
                .append("token", token);
        return run(auditParams, Enums.Action.CREATE, USER, null, token, null, (study, userId, rp, queryOptions) -> {
            // Initialise fields
            ParamUtils.checkObj(user, "User");
            ParamUtils.checkValidUserId(user.getId());
            ParamUtils.checkParameter(user.getName(), "name");
            user.setEmail(ParamUtils.defaultString(user.getEmail(), ""));
            if (StringUtils.isNotEmpty(user.getEmail())) {
                checkEmail(user.getEmail());
            }
            user.setOrganization(ParamUtils.defaultObject(user.getOrganization(), ""));
            ParamUtils.checkObj(user.getAccount(), "account");
            user.getAccount().setType(ParamUtils.defaultObject(user.getAccount().getType(), Account.AccountType.GUEST));
            user.getAccount().setCreationDate(TimeUtils.getTime());
            user.getAccount().setExpirationDate(ParamUtils.defaultString(user.getAccount().getExpirationDate(), ""));
            user.setInternal(new UserInternal(new UserStatus(UserStatus.READY)));
            user.setQuota(ParamUtils.defaultObject(user.getQuota(), UserQuota::new));
            user.setProjects(ParamUtils.defaultObject(user.getProjects(), Collections::emptyList));
            user.setSharedProjects(ParamUtils.defaultObject(user.getSharedProjects(), Collections::emptyList));
            user.setConfigs(ParamUtils.defaultObject(user.getConfigs(), HashMap::new));
            user.setFilters(ParamUtils.defaultObject(user.getFilters(), LinkedList::new));
            user.setAttributes(ParamUtils.defaultObject(user.getAttributes(), Collections::emptyMap));

            if (StringUtils.isEmpty(password)) {
                // The authentication origin must be different than internal
                Set<String> authOrigins = configuration.getAuthentication().getAuthenticationOrigins()
                        .stream()
                        .map(AuthenticationOrigin::getId)
                        .collect(Collectors.toSet());
                if (!authOrigins.contains(user.getAccount().getAuthentication().getId())) {
                    throw new CatalogException("Unknown authentication origin id '" + user.getAccount().getAuthentication() + "'");
                }
            } else {
                user.getAccount().setAuthentication(new Account.AuthenticationOrigin(INTERNAL_AUTHORIZATION, false));
            }

            userId = user.getId();
            // We add a condition to check if the registration is private + user (or system) is not trying to create the ADMINISTRATOR user
            if (!authorizationManager.isPublicRegistration() && !OPENCGA.equals(user.getId())) {
                userId = authenticationManagerMap.get(INTERNAL_AUTHORIZATION).getUserId(token);
                if (!OPENCGA.equals(userId)) {
                    String errorMsg = "The registration is closed to the public: Please talk to your administrator.";
                    throw new CatalogException(errorMsg);
                }
            }

            checkUserExists(user.getId());

            try {
                if (StringUtils.isNotEmpty(password) && !PasswordUtils.isStrongPassword(password)) {
                    throw new CatalogException("Invalid password. Check password strength for user " + user.getId());
                }
                if (user.getProjects() != null && !user.getProjects().isEmpty()) {
                    throw new CatalogException("Creating user and projects in a single transaction is forbidden");
                }

                catalogIOManager.createUser(user.getId());
                userDBAdaptor.insert(user, password, QueryOptions.empty());

                return userDBAdaptor.get(user.getId(), QueryOptions.empty());
            } catch (CatalogIOException | CatalogDBException e) {
                if (userDBAdaptor.exists(user.getId())) {
                    logger.error("ERROR! DELETING USER! " + user.getId());
                    catalogIOManager.deleteUser(user.getId());
                }

                throw e;
            }
        });
    }

    /**
     * Create a new user.
     *
     * @param id           User id
     * @param name         Name
     * @param email        Email
     * @param password     Encrypted Password
     * @param organization Optional organization
     * @param quota        Maximum user disk quota
     * @param type         User account type. Full or guest.
     * @param token        JWT token.
     * @return The created user
     * @throws CatalogException If user already exists, or unable to create a new user.
     */
    public OpenCGAResult<User> create(String id, String name, String email, String password, String organization, Long quota,
                                      Account.AccountType type, String token) throws CatalogException {
        User user = new User(id, name, email, organization, new UserInternal(new UserStatus()))
                .setAccount(new Account(type, "", "", null))
                .setQuota(new UserQuota().setMaxDisk(quota != null ? quota : -1));
        return create(user, password, token);
    }

    public void syncAllUsersOfExternalGroup(String studyStr, String authOrigin, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("authOrigin", authOrigin)
                .append("token", token);
        run(auditParams, Enums.Action.SYNC_EXTERNAL_GROUP_OF_USERS, STUDY, studyStr, token, null,
                Collections.singletonList(StudyDBAdaptor.QueryParams.GROUPS.key()), (study, userId, rp, queryOptions) -> {
                    rp.setId(study.getId());
                    rp.setUuid(study.getUuid());

                    if (!OPENCGA.equals(authenticationManagerMap.get(INTERNAL_AUTHORIZATION).getUserId(token))) {
                        throw new CatalogAuthorizationException("Only the root user can perform this action");
                    }

                    List<Group> allGroups = study.getGroups();

                    boolean foundAny = false;
                    for (Group group : allGroups) {
                        if (group.getSyncedFrom() != null && group.getSyncedFrom().getAuthOrigin().equals(authOrigin)) {
                            logger.info("Fetching users of group '{}' from authentication origin '{}'",
                                    group.getSyncedFrom().getRemoteGroup(), group.getSyncedFrom().getAuthOrigin());
                            foundAny = true;

                            List<User> userList;
                            try {
                                userList = authenticationManagerMap.get(group.getSyncedFrom().getAuthOrigin())
                                        .getUsersFromRemoteGroup(group.getSyncedFrom().getRemoteGroup());
                            } catch (CatalogException e) {
                                // There was some kind of issue for which we could not retrieve the group information.
                                logger.info("Removing all users from group '{}' belonging to group '{}' in the external authentication "
                                                + "origin", group.getId(), group.getSyncedFrom().getAuthOrigin());
                                logger.info("Please, manually remove group '{}' if external group '{}' was removed from the authentication"
                                                + " origin", group.getId(), group.getSyncedFrom().getAuthOrigin());
                                catalogManager.getStudyManager().updateGroup(studyStr, group.getId(), ParamUtils.BasicUpdateAction.SET,
                                        new GroupUpdateParams(Collections.emptyList()), token);
                                continue;
                            }
                            Iterator<User> iterator = userList.iterator();
                            while (iterator.hasNext()) {
                                User user = iterator.next();
                                try {
                                    create(user, null, token);
                                    logger.info("User '{}' ({}) successfully created", user.getId(), user.getName());
                                } catch (CatalogParameterException e) {
                                    logger.warn("Could not create user '{}' ({}). {}", user.getId(), user.getName(), e.getMessage());
                                    iterator.remove();
                                } catch (CatalogException e) {
                                    if (!e.getMessage().contains("already exists")) {
                                        logger.warn("Could not create user '{}' ({}). {}", user.getId(), user.getName(), e.getMessage());
                                        iterator.remove();
                                    }
                                }
                            }

                            GroupUpdateParams updateParams;
                            if (ListUtils.isEmpty(userList)) {
                                logger.info("No members associated to the external group");
                                updateParams = new GroupUpdateParams(Collections.emptyList());
                            } else {
                                logger.info("Associating members to the internal OpenCGA group");
                                updateParams = new GroupUpdateParams(new ArrayList<>(userList.stream().map(User::getId)
                                        .collect(Collectors.toSet())));
                            }
                            catalogManager.getStudyManager().updateGroup(studyStr, group.getId(), ParamUtils.BasicUpdateAction.SET,
                                    updateParams, token);
                        }
                    }
                    if (!foundAny) {
                        logger.info("No synced groups found in study '{}' from authentication origin '{}'", study, authOrigin);
                    }
                    return null;
                });
    }

    /**
     * Register all the users belonging to a remote group. If internalGroup and study are not null, it will also associate the remote group
     * to the internalGroup defined.
     *
     * @param authOrigin    Authentication origin.
     * @param remoteGroup   Group name of the remote authentication origin.
     * @param internalGroup Group name in Catalog that will be associated to the remote group.
     * @param study         Study where the internal group will be associated.
     * @param sync          Boolean indicating whether the remote group will be synced with the internal group or not.
     * @param token         JWT token. The token should belong to the root user.
     * @throws CatalogException If any of the parameters is wrong or there is any internal error.
     */
    public void importRemoteGroupOfUsers(String authOrigin, String remoteGroup, @Nullable String internalGroup, @Nullable String study,
                                         boolean sync, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("authOrigin", authOrigin)
                .append("remoteGroup", remoteGroup)
                .append("internalGroup", internalGroup)
                .append("study", study)
                .append("sync", sync)
                .append("token", token);

        runBatch(auditParams, Enums.Action.IMPORT_EXTERNAL_GROUP_OF_USERS, USER, study, token, null, (s, userId, qo, operationUuid) -> {
            if (!OPENCGA.equals(authenticationManagerMap.get(INTERNAL_AUTHORIZATION).getUserId(token))) {
                throw new CatalogAuthorizationException("Only the root user can perform this action");
            }

            ParamUtils.checkParameter(authOrigin, "Authentication origin");
            ParamUtils.checkParameter(remoteGroup, "Remote group");

            if (!authenticationManagerMap.containsKey(authOrigin)) {
                throw new CatalogException("Unknown authentication origin");
            }

            List<User> userList;
            if (sync) {
                // We don't create any user as they will be automatically populated during login
                userList = Collections.emptyList();
            } else {
                logger.info("Fetching users from authentication origin '{}'", authOrigin);

                // Register the users
                userList = authenticationManagerMap.get(authOrigin).getUsersFromRemoteGroup(remoteGroup);
                for (User user : userList) {
                    try {
                        run(auditParams, Enums.Action.IMPORT_EXTERNAL_GROUP_OF_USERS, USER, operationUuid, s, userId, null, (s2, u, rp,
                                                                                                                             qo2) -> {
                            create(user, null, token);
                            logger.info("User '{}' successfully created", user.getId());
                            return null;
                        });
                    } catch (CatalogException e) {
                        logger.warn("{}", e.getMessage());
                    }
                }
            }

            if (StringUtils.isNotEmpty(internalGroup) && StringUtils.isNotEmpty(study)) {
                // Check if the group already exists
                OpenCGAResult<Group> groupResult = catalogManager.getStudyManager().getGroup(study, internalGroup, token);
                if (groupResult.getNumResults() == 1) {
                    logger.error("Cannot synchronise with group {}. The group already exists and is already in use.", internalGroup);
                    throw new CatalogException("Cannot synchronise with group " + internalGroup
                            + ". The group already exists and is already in use.");
                }

                // Create new group associating it to the remote group
                try {
                    logger.info("Attempting to register group '{}' in study '{}'", internalGroup, study);
                    Group.Sync groupSync = null;
                    if (sync) {
                        groupSync = new Group.Sync(authOrigin, remoteGroup);
                    }
                    Group group = new Group(internalGroup, userList.stream().map(User::getId).collect(Collectors.toList()))
                            .setSyncedFrom(groupSync);
                    catalogManager.getStudyManager().createGroup(study, group, token);
                    logger.info("Group '{}' created and synchronised with external group", internalGroup);
                } catch (CatalogException e) {
                    logger.error("Could not register group '{}' in study '{}'\n{}", internalGroup, study, e.getMessage(), e);
                    throw new CatalogException("Could not register group '" + internalGroup + "' in study '" + study + "': "
                            + e.getMessage(), e);
                }
            }
            return null;
        });
    }

    /**
     * Register all the ids. If internalGroup and study are not null, it will also associate the users to the internalGroup defined.
     *
     * @param authOrigin    Authentication origin.
     * @param idList        List of entity ids existing in the authentication origin.
     * @param isApplication boolean indicating whether the id list belong to external applications or users.
     * @param internalGroup Group name in Catalog that will be associated to the remote group.
     * @param studyStr         Study where the internal group will be associated.
     * @param token         JWT token. The token should belong to the root user.
     * @throws CatalogException If any of the parameters is wrong or there is any internal error.
     */
    public void importRemoteEntities(String authOrigin, List<String> idList, boolean isApplication, @Nullable String internalGroup,
                                     @Nullable String studyStr, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("authOrigin", authOrigin)
                .append("idList", idList)
                .append("isApplication", isApplication)
                .append("internalGroup", internalGroup)
                .append("study", studyStr)
                .append("token", token);

        runBatch(auditParams, Enums.Action.IMPORT_EXTERNAL_USERS, USER, studyStr, token, null, (study, userId, qo, operationUuid) -> {
            if (!OPENCGA.equals(userId)) {
                throw new CatalogAuthorizationException("Only the root user can perform this action");
            }

            ParamUtils.checkParameter(authOrigin, "Authentication origin");
            ParamUtils.checkObj(idList, "ids");

            if (!authenticationManagerMap.containsKey(authOrigin)) {
                throw new CatalogException("Unknown authentication origin");
            }

            if (!isApplication) {
                logger.info("Fetching user information from authentication origin '{}'", authOrigin);
                List<User> parsedUserList = authenticationManagerMap.get(authOrigin).getRemoteUserInformation(idList);
                for (User user : parsedUserList) {
                    create(user, null, token);
                    logger.info("User '{}' successfully created", user.getId());
                }
            } else {
                for (String applicationId : idList) {
                    User application = new User(applicationId, new Account()
                            .setType(Account.AccountType.GUEST)
                            .setAuthentication(new Account.AuthenticationOrigin(authOrigin, true)))
                            .setEmail("mail@mail.co.uk");
                    create(application, null, token);
                    logger.info("User (application) '{}' successfully created", application.getId());
                }
            }

            if (StringUtils.isNotEmpty(internalGroup) && StringUtils.isNotEmpty(studyStr)) {
                // Check if the group already exists
                try {
                    OpenCGAResult<Group> group = catalogManager.getStudyManager().getGroup(studyStr, internalGroup, token);
                    if (group.getNumResults() == 1) {
                        // We will add those users to the existing group
                        catalogManager.getStudyManager().updateGroup(studyStr, internalGroup, ParamUtils.BasicUpdateAction.ADD,
                                new GroupUpdateParams(idList), token);
                        return null;
                    }
                } catch (CatalogException e) {
                    logger.warn("The group '{}' did not exist.", internalGroup);
                }

                // Create new group associating it to the remote group
                try {
                    logger.info("Attempting to register group '{}' in study '{}'", internalGroup, studyStr);
                    Group group = new Group(internalGroup, idList);
                    catalogManager.getStudyManager().createGroup(studyStr, group, token);
                } catch (CatalogException e) {
                    logger.error("Could not register group '{}' in study '{}'\n{}", internalGroup, studyStr, e.getMessage());
                }
            }

            return null;
        });
    }

    /**
     * Gets the user information.
     *
     * @param userId  User id
     * @param options QueryOptions
     * @param token   SessionId of the user performing this operation.
     * @return The requested user
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<User> get(String userId, QueryOptions options, String token) throws CatalogException {
        return get(Collections.singletonList(userId), options, token);
//        ParamUtils.checkParameter(userId, "userId");
//        ParamUtils.checkParameter(token, "sessionId");
//        options = ParamUtils.defaultObject(options, QueryOptions::new);
//
//        ObjectMap auditParams = new ObjectMap()
//                .append("userId", userId)
//                .append("options", options)
//                .append("token", token);
//        try {
//            userId = getCatalogUserId(userId, token);
//            OpenCGAResult<User> userDataResult = userDBAdaptor.get(userId, options);
//
//            // Remove some unnecessary and prohibited parameters
//            for (User user : userDataResult.getResults()) {
//                if (user.getProjects() != null) {
//                    for (Project project : user.getProjects()) {
//                        if (project.getStudies() != null) {
//                            for (Study study : project.getStudies()) {
//                                study.setVariableSets(null);
//                            }
//                        }
//                    }
//                }
//            }
//
//            auditManager.auditInfo(userId, Enums.Resource.USER, userId, "", "", "", auditParams,
//                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
//            return userDataResult;
//        } catch (CatalogException e) {
//            auditManager.auditInfo(userId, Enums.Resource.USER, userId, "", "", "", auditParams,
//                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
//            throw e;
//        }
    }

    /**
     * Gets the user information.
     *
     * @param userIdList List of user id
     * @param options    QueryOptions
     * @param token      Token belonging to the user itself or administrator of any study shared with the user list requested.
     * @return The requested users
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<User> get(List<String> userIdList, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userIdList", userIdList)
                .append("options", options)
                .append("token", token);
        return runBatch(auditParams, Enums.Action.INFO, USER, null, token, options, (s, userId, queryOptions, operationUuid) -> {
            ParamUtils.checkNotEmptyArray(userIdList, "userId");
            ParamUtils.checkParameter(token, "token");

            OpenCGAResult<User> userDataResult;

            if (userIdList.size() == 1 && userId.equals(userIdList.get(0))) {
                return run(auditParams, Enums.Action.INFO, USER, operationUuid, null, userId, null, (study, userId1, rp, queryOptions1) -> {
                    rp.setId(userId);
                    return userDBAdaptor.get(userId, queryOptions);
                });
            }

            // We will obtain the users this user is administrating
            QueryOptions adminOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    UserDBAdaptor.QueryParams.PROJECTS.key() + "." + ProjectDBAdaptor.QueryParams.STUDIES.key() + "."
                            + StudyDBAdaptor.QueryParams.GROUPS.key(), UserDBAdaptor.QueryParams.SHARED_PROJECTS.key() + "."
                            + ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.GROUPS.key()));
            userDataResult = userDBAdaptor.get(userId, adminOptions);
            User admin = userDataResult.first();

            Set<String> users = new HashSet<>();
            boolean isAdmin = false;

            if (admin.getProjects() != null) {
                for (Project project : admin.getProjects()) {
                    if (project.getStudies() != null) {
                        for (Study study : project.getStudies()) {
                            isAdmin = true;
                            for (Group group : study.getGroups()) {
                                if (StudyManager.MEMBERS.equals(group.getId())) {
                                    users.addAll(group.getUserIds());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if (admin.getSharedProjects() != null) {
                for (Project project : admin.getSharedProjects()) {
                    if (project.getStudies() != null) {
                        for (Study study : project.getStudies()) {
                            boolean isAdminInStudy = false;
                            Set<String> usersInStudy = new HashSet<>();
                            for (Group group : study.getGroups()) {
                                if (StudyManager.ADMINS.equals(group.getId()) && group.getUserIds().contains(userId)) {
                                    isAdminInStudy = true;
                                }
                                if (StudyManager.MEMBERS.equals(group.getId())) {
                                    usersInStudy.addAll(group.getUserIds());
                                }
                            }
                            if (isAdminInStudy) {
                                isAdmin = true;
                                users.addAll(usersInStudy);
                            }
                        }
                    }
                }
            }
            if (!isAdmin) {
                throw new CatalogAuthorizationException("Only owners or administrators can see other user information");
            }

            // Filter only the users the userId can get information for
            List<String> auxUserList = userIdList.stream().filter(users::contains).collect(Collectors.toList());

            Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), auxUserList);
            OpenCGAResult<User> result = userDBAdaptor.get(query, queryOptions);
            Map<String, User> userMap = new HashMap<>();
            for (User user : result.getResults()) {
                userMap.put(user.getId(), user);
            }

            // Ensure order and audit
            List<User> finalUserList = new ArrayList<>(userIdList.size());
            List<Event> eventList = new ArrayList<>(userIdList.size());
            for (String tmpUserId : userIdList) {
                try {
                    run(auditParams, Enums.Action.INFO, USER, operationUuid, null, userId, null, (study, userId1, rp, queryOptions1) -> {
                        rp.setId(tmpUserId);
                        if (userMap.containsKey(tmpUserId)) {
                            finalUserList.add(userMap.get(tmpUserId));
                        } else {
                            throw new CatalogException("'" + userId + "' is not administrating a study of user '" + tmpUserId
                                    + "' or user does not exist.");
                        }
                        return null;
                    });
                } catch (CatalogException e) {
                    finalUserList.add(new User().setId(tmpUserId));
                    eventList.add(new Event(Event.Type.ERROR, e.getMessage()));
                }
            }

            result.setResults(finalUserList);
            result.setEvents(eventList);

            return result;
        });
    }

    public OpenCGAResult<User> update(String userId, ObjectMap parameters, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("updateParams", parameters)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.UPDATE, USER, null, token, options, (study, loggedUser, rp, queryOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkObj(parameters, "parameters");
            ParamUtils.checkParameter(token, "token");

            String realUserId = getCatalogUserId(userId, token);
            rp.setId(realUserId);
            for (String s : parameters.keySet()) {
                if (!s.matches("name|email|organization|attributes")) {
                    throw new CatalogDBException("Parameter '" + s + "' can't be changed");
                }
            }

            if (parameters.containsKey("email")) {
                checkEmail(parameters.getString("email"));
            }
            OpenCGAResult<User> updateResult = userDBAdaptor.update(realUserId, parameters);

            if (queryOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated user
                OpenCGAResult<User> result = userDBAdaptor.get(realUserId, queryOptions);
                updateResult.setResults(result.getResults());
            }

            return updateResult;
        });
    }

    /**
     * Delete entries from Catalog.
     *
     * @param userIdList Comma separated list of ids corresponding to the objects to delete
     * @param options    Deleting options.
     * @param token      Token
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<User> delete(String userIdList, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userIdList", userIdList)
                .append("options", options)
                .append("token", token);
        return runBatch(auditParams, Enums.Action.DELETE, USER, null, token, options, (s, tokenUser, qOptions, operationUuid) -> {
            ParamUtils.checkParameter(userIdList, "userIdList");
            ParamUtils.checkParameter(token, "token");

            String[] userIds = userIdList.split(",");
            OpenCGAResult<User> deletedUsers = OpenCGAResult.empty();
            for (String userId : userIds) {
                // Only if the user asking the deletion is the ADMINISTRATOR or the user to be deleted itself...
                if (OPENCGA.equals(tokenUser) || userId.equals(tokenUser)) {
                    OpenCGAResult<User> deletedUser = run(auditParams, Enums.Action.DELETE, USER, operationUuid, null, userId, null,
                            (study, userId1, rp, qo) -> {
                                rp.setId(userId);
                                userDBAdaptor.delete(userId, options);
                                Query query = new Query()
                                        .append(UserDBAdaptor.QueryParams.ID.key(), userId)
                                        .append(UserDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.DELETED);
                                return userDBAdaptor.get(query, QueryOptions.empty());
                            });
                    deletedUsers.append(deletedUser);
                }
            }
            return deletedUsers;
        });
    }

    public OpenCGAResult resetPassword(String userId, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("token", token);
        return run(auditParams, Enums.Action.RESET_USER_PASSWORD, USER, null, token, null, (s, loggedUser, rp, queryOptions) -> {
            rp.setId(userId);
            authorizationManager.checkIsInstallationAdministrator(loggedUser);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "token");
            String authOrigin = getAuthenticationOriginId(userId);
            return authenticationManagerMap.get(authOrigin).resetPassword(userId);
        });
    }

    public AuthenticationResponse loginAsAdmin(String password) throws CatalogException {
        return login(OPENCGA, password);
    }

    public AuthenticationResponse login(String username, String password) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("username", username)
                .append("password", "********");
        return run(auditParams, Enums.Action.LOGIN, USER, null, null, null, (s, u, rp, queryOptions) -> {
            rp.setId(username);
            ParamUtils.checkParameter(username, "userId");
            ParamUtils.checkParameter(password, "password");

            String authId = null;
            AuthenticationResponse response = null;

            OpenCGAResult<User> userOpenCGAResult = userDBAdaptor.get(username, INCLUDE_ACCOUNT);
            if (userOpenCGAResult.getNumResults() == 1) {
                authId = userOpenCGAResult.first().getAccount().getAuthentication().getId();
                if (!authenticationManagerMap.containsKey(authId)) {
                    throw new CatalogException("Could not authenticate user '" + username + "'. The authentication origin '" + authId
                            + "' could not be found.");
                }
                return authenticationManagerMap.get(authId).authenticate(username, password);
            } else {
                // We attempt to login the user with the different authentication managers
                for (Map.Entry<String, AuthenticationManager> entry : authenticationManagerMap.entrySet()) {
                    AuthenticationManager authenticationManager = entry.getValue();
                    try {
                        response = authenticationManager.authenticate(username, password);
                        authId = entry.getKey();
                        break;
                    } catch (CatalogAuthenticationException e) {
                        logger.debug("Attempted authentication failed with {} for user '{}'\n{}", entry.getKey(), username, e.getMessage(),
                                e);
                    }
                }
            }

            if (response == null) {
                throw CatalogAuthenticationException.incorrectUserOrPassword();
            }

            String userId = authenticationManagerMap.get(authId).getUserId(response.getToken());
            if (!INTERNAL_AUTHORIZATION.equals(authId)) {
                // External authorization
                try {
                    // If the user is not registered, an exception will be raised
                    userDBAdaptor.checkId(userId);
                } catch (CatalogDBException e) {
                    // The user does not exist so we register it
                    User user = authenticationManagerMap.get(authId).getRemoteUserInformation(Collections.singletonList(userId)).get(0);
                    // Generate a root token to be able to create the user even if the installation is private
                    String rootToken = authenticationManagerMap.get(INTERNAL_AUTHORIZATION).createToken(OPENCGA);
                    create(user, null, rootToken);
                }

                try {
                    List<String> remoteGroups = authenticationManagerMap.get(authId).getRemoteGroups(response.getToken());

                    // Resync synced groups of user in OpenCGA
                    studyDBAdaptor.resyncUserWithSyncedGroups(userId, remoteGroups, authId);
                } catch (CatalogException e) {
                    logger.error("Could not update synced groups for user '" + userId + "'\n" + e.getMessage(), e);
                }
            }

            return response;
        });
    }

    /**
     * Create a new token if the token provided corresponds to the user and it is not expired yet.
     *
     * @param token active token.
     * @return a new AuthenticationResponse object.
     * @throws CatalogException if the token does not correspond to the user or the token is expired.
     */
    public AuthenticationResponse refreshToken(String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("token", token);
        return run(auditParams, Enums.Action.REFRESH_TOKEN, USER, null, token, null, (study, userId, rp, queryOptions) -> {
            rp.setId(userId);
            AuthenticationResponse response = null;
            CatalogAuthenticationException exception = null;
            // We attempt to renew the token with the different authentication managers
            for (Map.Entry<String, AuthenticationManager> entry : authenticationManagerMap.entrySet()) {
                AuthenticationManager authenticationManager = entry.getValue();
                try {
                    response = authenticationManager.refreshToken(token);
                    break;
                } catch (CatalogAuthenticationException e) {
                    logger.debug("Could not refresh token with '{}' provider: {}", entry.getKey(), e.getMessage(), e);
                    if (INTERNAL_AUTHORIZATION.equals(entry.getKey())) {
                        exception = e;
                    }
                }
            }
            if (response == null && exception != null) {
                throw exception;
            }
            return response;
        });
    }

    /**
     * This method will be only callable by the system. It generates a new session id for the user.
     *
     * @param userId user id for which a session will be generated.
     * @param token  Password or active session of the OpenCGA admin.
     * @return an objectMap containing the new sessionId
     * @throws CatalogException if the password is not correct or the userId does not exist.
     */
    public String getNonExpiringToken(String userId, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("token", token);
        return run(auditParams, Enums.Action.FETCH_NON_EXPIRING_TOKEN, USER, null, token, null, (study, userId1, rp, queryOptions) -> {
            rp.setId(userId);
            if (OPENCGA.equals(getUserId(token))) {
                return authenticationManagerMap.get(INTERNAL_AUTHORIZATION).createNonExpiringToken(userId);
            } else {
                throw new CatalogException("Only user '" + OPENCGA + "' is allowed to create non expiring tokens");
            }
        });
    }

    public String getAdminNonExpiringToken(String token) throws CatalogException {
        return getNonExpiringToken(OPENCGA, token);
    }

    /**
     * Add a new filter to the user account.
     * <p>
     *
     * @param userId       user id to whom the filter will be associated.
     * @param id           Filter id.
     * @param description  Filter description.
     * @param resource     Resource where the filter should be applied.
     * @param query        Query object.
     * @param queryOptions Query options object.
     * @param token        session id of the user asking to store the filter.
     * @return the created filter.
     * @throws CatalogException if there already exists a filter with that same name for the user or if the user corresponding to the
     *                          session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> addFilter(String userId, String id, String description, Enums.Resource resource, Query query,
                                               QueryOptions queryOptions, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("id", id)
                .append("description", description)
                .append("resource", resource)
                .append("query", query)
                .append("queryOptions", queryOptions)
                .append("token", token);

        return run(auditParams, Enums.Action.CHANGE_USER_CONFIG, USER, null, token, queryOptions, (study, u, rp, qOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "sessionId");
            ParamUtils.checkParameter(id, "id");
            ParamUtils.checkObj(resource, "resource");
            ParamUtils.checkObj(query, "Query");
            ParamUtils.checkObj(queryOptions, "QueryOptions");
            String finalDescription = ParamUtils.defaultString(description, "");

            String finalUserId = getCatalogUserId(userId, token);
            rp.setId(finalUserId);
            userDBAdaptor.checkId(finalUserId);

            Query queryExists = new Query()
                    .append(UserDBAdaptor.QueryParams.ID.key(), finalUserId)
                    .append(UserDBAdaptor.QueryParams.FILTERS_ID.key(), id);
            if (userDBAdaptor.count(queryExists).getNumMatches() > 0) {
                throw new CatalogException("There already exists a filter called " + id + " for user " + finalUserId);
            }

            UserFilter filter = new UserFilter(id, finalDescription, resource, query, queryOptions);
            OpenCGAResult<?> result = userDBAdaptor.addFilter(finalUserId, filter);
            return new OpenCGAResult<>(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(filter), 1);
        });
    }

    /**
     * Update the filter information.
     * <p>
     *
     * @param userId user id to whom the filter should be updated.
     * @param name   Filter name.
     * @param params Map containing the parameters to be updated.
     * @param token  session id of the user asking to update the filter.
     * @return the updated filter.
     * @throws CatalogException if the filter could not be updated because the filter name is not correct or if the user corresponding to
     *                          the session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> updateFilter(String userId, String name, ObjectMap params, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("params", params)
                .append("token", token);
        return run(auditParams, Enums.Action.CHANGE_USER_CONFIG, USER, null, token, null, (study, u, rp, queryOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "token");
            ParamUtils.checkParameter(name, "name");

            String finalUserId = getCatalogUserId(userId, token);
            rp.setId(finalUserId);
            userDBAdaptor.checkId(finalUserId);

            Query queryExists = new Query()
                    .append(UserDBAdaptor.QueryParams.ID.key(), finalUserId)
                    .append(UserDBAdaptor.QueryParams.FILTERS_ID.key(), name);
            if (userDBAdaptor.count(queryExists).getNumMatches() == 0) {
                throw new CatalogException("There is no filter called " + name + " for user " + finalUserId);
            }

            OpenCGAResult<?> result = userDBAdaptor.updateFilter(finalUserId, name, params);
            UserFilter filter = getFilter(finalUserId, name);
            if (filter == null) {
                throw new CatalogException("Internal error: The filter " + name + " could not be found.");
            }
            return new OpenCGAResult<>(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(filter), 1);
        });
    }

    /**
     * Delete the filter.
     * <p>
     *
     * @param userId user id to whom the filter should be deleted.
     * @param name   filter name to be deleted.
     * @param token  session id of the user asking to delete the filter.
     * @return the deleted filter.
     * @throws CatalogException when the filter cannot be removed or the name is not correct or if the user corresponding to the session id
     *                          is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> deleteFilter(String userId, String name, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);

        return run(auditParams, Enums.Action.CHANGE_USER_CONFIG, USER, null, token, null, (study, u, rp, qOptions) -> {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "token");
            ParamUtils.checkParameter(name, "name");

            String finalUserId = getCatalogUserId(userId, token);
            userDBAdaptor.checkId(finalUserId);

            UserFilter filter = getFilter(finalUserId, name);
            if (filter == null) {
                throw new CatalogException("There is no filter called " + name + " for user " + finalUserId);
            }

            OpenCGAResult<?> result = userDBAdaptor.deleteFilter(finalUserId, name);
            return new OpenCGAResult<>(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(filter), 1);
        });
    }

    /**
     * Retrieves a filter.
     * <p>
     *
     * @param userId user id having the filter stored.
     * @param name   Filter name to be fetched.
     * @param token  session id of the user fetching the filter.
     * @return the filter.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> getFilter(String userId, String name, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);

        return run(auditParams, Enums.Action.FETCH_USER_CONFIG, USER, null, token, null, (study, u, rp, queryOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "sessionId");
            ParamUtils.checkParameter(name, "name");

            String finalUserId = getCatalogUserId(userId, token);
            rp.setId(finalUserId);
            userDBAdaptor.checkId(finalUserId);

            UserFilter filter = getFilter(finalUserId, name);
            if (filter == null) {
                throw new CatalogException("Filter " + name + " not found.");
            } else {
                return new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(filter), 1);
            }
        });
    }

    /**
     * Retrieves all the user filters.
     *
     * @param userId user id having the filters.
     * @param token  session id of the user fetching the filters.
     * @return the filters.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> getAllFilters(String userId, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("token", token);

        return run(auditParams, Enums.Action.FETCH_USER_CONFIG, USER, null, token, null, (study, userId1, rp, qOptions) -> {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "sessionId");

            String finalUserId = getCatalogUserId(userId, token);
            userDBAdaptor.checkId(finalUserId);

            Query query = new Query()
                    .append(UserDBAdaptor.QueryParams.ID.key(), finalUserId);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.FILTERS.key());
            OpenCGAResult<User> userDataResult = userDBAdaptor.get(query, queryOptions);

            if (userDataResult.getNumResults() != 1) {
                throw new CatalogException("Internal error: User " + finalUserId + " not found.");
            }

            List<UserFilter> filters = userDataResult.first().getFilters();
            return new OpenCGAResult<>(0, Collections.emptyList(), filters.size(), filters, filters.size());
        });
    }

    /**
     * Creates or updates a configuration.
     * <p>
     *
     * @param userId user id to whom the config will be associated.
     * @param name   Name of the configuration (normally, name of the application).
     * @param config Configuration to be stored.
     * @param token  session id of the user asking to store the config.
     * @return the set configuration.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    public OpenCGAResult setConfig(String userId, String name, Map<String, Object> config, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("config", config)
                .append("token", token);

        return run(auditParams, Enums.Action.CHANGE_USER_CONFIG, USER, null, token, null, (study, userId1, rp, queryOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "sessionId");
            ParamUtils.checkParameter(name, "name");
            ParamUtils.checkObj(config, "ObjectMap");

            String finalUserId = getCatalogUserId(userId, token);
            rp.setId(finalUserId);
            userDBAdaptor.checkId(finalUserId);

            OpenCGAResult<?> result = userDBAdaptor.setConfig(finalUserId, name, config);
            return new OpenCGAResult(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(config), 1);
        });
    }

    /**
     * Deletes a configuration.
     * <p>
     *
     * @param userId user id to whom the configuration should be deleted.
     * @param name   Name of the configuration to be deleted (normally, name of the application).
     * @param token  session id of the user asking to delete the configuration.
     * @return the deleted configuration.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id or the configuration did
     *                          not exist.
     */
    public OpenCGAResult deleteConfig(String userId, String name, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);

        return run(auditParams, Enums.Action.CHANGE_USER_CONFIG, USER, null, token, null, (study, userId1, rp, queryOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "token");
            ParamUtils.checkParameter(name, "name");

            String finalUserId = getCatalogUserId(userId, token);
            rp.setId(finalUserId);
            userDBAdaptor.checkId(finalUserId);

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.CONFIGS.key());
            OpenCGAResult<User> userDataResult = userDBAdaptor.get(finalUserId, options);
            if (userDataResult.getNumResults() == 0) {
                throw new CatalogException("Internal error: Could not get user " + finalUserId);
            }

            Map<String, ObjectMap> configs = userDataResult.first().getConfigs();
            if (configs == null) {
                throw new CatalogException("Internal error: Configuration object is null.");
            }

            if (!configs.containsKey(name)) {
                throw new CatalogException("Error: Cannot delete configuration with name " + name + ". Configuration name not found.");
            }

            OpenCGAResult<?> result = userDBAdaptor.deleteConfig(finalUserId, name);
            return new OpenCGAResult(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(configs.get(name)), 1);
        });
    }

    /**
     * Retrieves a configuration.
     * <p>
     *
     * @param userId user id having the configuration stored.
     * @param name   Name of the configuration to be fetched (normally, name of the application).
     * @param token  session id of the user attempting to fetch the configuration.
     * @return the configuration.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id or the configuration
     *                          does not exist.
     */
    public OpenCGAResult getConfig(String userId, String name, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);

        return run(auditParams, Enums.Action.FETCH_USER_CONFIG, USER, null, token, null, (study, u, rp, queryOptions) -> {
            rp.setId(userId);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "sessionId");

            String finalUserId = getCatalogUserId(userId, token);
            rp.setId(finalUserId);
            userDBAdaptor.checkId(finalUserId);

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.CONFIGS.key());
            OpenCGAResult<User> userDataResult = userDBAdaptor.get(finalUserId, options);
            if (userDataResult.getNumResults() == 0) {
                throw new CatalogException("Internal error: Could not get user " + finalUserId);
            }

            Map<String, ObjectMap> configs = userDataResult.first().getConfigs();
            if (configs == null) {
                throw new CatalogException("Internal error: Configuration object is null.");
            }

            if (StringUtils.isNotEmpty(name) && !configs.containsKey(name)) {
                throw new CatalogException("Error: Cannot fetch configuration with name " + name + ". Configuration name not found.");
            }

            return new OpenCGAResult(userDataResult.getTime(), userDataResult.getEvents(), 1, Collections.singletonList(configs.get(name)),
                    1);
        });
    }

    private UserFilter getFilter(String userId, String name) throws CatalogException {
        Query query = new Query()
                .append(UserDBAdaptor.QueryParams.ID.key(), userId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.FILTERS.key());
        OpenCGAResult<User> userDataResult = userDBAdaptor.get(query, queryOptions);

        if (userDataResult.getNumResults() != 1) {
            throw new CatalogException("Internal error: User " + userId + " not found.");
        }

        for (UserFilter filter : userDataResult.first().getFilters()) {
            if (name.equals(filter.getId())) {
                return filter;
            }
        }

        return null;
    }

    private void checkUserExists(String userId) throws CatalogException {
        if (userId.equalsIgnoreCase(ANONYMOUS)) {
            throw new CatalogException("Permission denied: Cannot create users with special treatments in catalog.");
        }

        if (userDBAdaptor.exists(userId)) {
            throw new CatalogException("The user already exists in our database.");
        }
    }

    private String getAuthenticationOriginId(String userId) throws CatalogException {
        OpenCGAResult<User> user = userDBAdaptor.get(userId, new QueryOptions());
        if (user == null || user.getNumResults() == 0) {
            throw new CatalogException(userId + " user not found");
        }
        return user.first().getAccount().getAuthentication().getId();
    }

    /**
     * Extracts the user id from the token. If it doesn't match the userId provided and the userId provided is actually an email, it will
     * fetch the user of the token from Catalog and check whether the email matches. If it matches, it will return the corresponding user
     * id.
     *
     * @param userId User id provided by the user.
     * @param token  Token.
     * @return A valid user id for the user and token provided.
     * @throws CatalogException if the user cannot be retrieved for whatever reason.
     */
    private String getCatalogUserId(String userId, String token) throws CatalogException {
        ParamUtils.checkParameter(userId, "User id");

        String userFromToken = getUserId(token);

        if (userFromToken.equals(userId)) {
            return userId;
        }

        // User might be using the email as an id
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.EMAIL.key());
        OpenCGAResult<User> userOpenCGAResult = userDBAdaptor.get(userFromToken, options);
        if (userOpenCGAResult.getNumResults() == 0) {
            throw new CatalogException("User '" + userFromToken + "' not found. Please, call to login first or talk to your administrator");
        }

        if (userId.equalsIgnoreCase(userOpenCGAResult.first().getEmail())) {
            return userFromToken;
        } else {
            throw new CatalogAuthorizationException("User '" + userFromToken + "' cannot operate on behalf of user '" + userId + "'.");
        }
    }

    /**
     * Get the userId from the sessionId.
     *
     * @param token Token
     * @return UserId owner of the sessionId. Empty string if SessionId does not match.
     * @throws CatalogException when the session id does not correspond to any user or the token has expired.
     */
    public String getUserId(String token) throws CatalogException {
        for (Map.Entry<String, AuthenticationManager> entry : authenticationManagerMap.entrySet()) {
            AuthenticationManager authenticationManager = entry.getValue();
            try {
                String userId = authenticationManager.getUserId(token);
                userDBAdaptor.checkId(userId);
                return userId;
            } catch (Exception e) {
                logger.debug("Could not get user from token using {} authentication manager. {}", entry.getKey(), e.getMessage(), e);
            }
        }
        // We make this call again to get the original exception
        return authenticationManagerMap.get(INTERNAL_AUTHORIZATION).getUserId(token);
    }

    public Date getExpirationDate(String token) {
        for (Map.Entry<String, AuthenticationManager> entry : authenticationManagerMap.entrySet()) {
            AuthenticationManager authenticationManager = entry.getValue();
            try {
                return authenticationManager.getExpirationDate(token);
            } catch (Exception e) {
                logger.debug("Could not get expiration date from token using {} authentication manager. {}", entry.getKey(),
                        e.getMessage(), e);
            }
        }
        return null;
    }
}
