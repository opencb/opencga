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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.azure.AuthenticationFactory;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.ParamUtils.checkEmail;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserManager extends AbstractManager {

    static final QueryOptions INCLUDE_ACCOUNT_AND_INTERNAL = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            UserDBAdaptor.QueryParams.ID.key(), UserDBAdaptor.QueryParams.ACCOUNT.key(), UserDBAdaptor.QueryParams.INTERNAL.key()));
    protected static Logger logger = LoggerFactory.getLogger(UserManager.class);
    private final CatalogIOManager catalogIOManager;
    private final AuthenticationFactory authenticationFactory;

    UserManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager,
                AuthenticationFactory authenticationFactory, Configuration configuration)
            throws CatalogException {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.catalogIOManager = catalogIOManager;
        this.authenticationFactory = authenticationFactory;
    }

    public void changePassword(String organizationId, String userId, String oldPassword, String newPassword) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(oldPassword, "oldPassword");
        ParamUtils.checkParameter(newPassword, "newPassword");
        try {
            if (oldPassword.equals(newPassword)) {
                throw new CatalogException("New password is the same as the old password.");
            }

            getUserDBAdaptor(organizationId).checkId(userId);
            String authOrigin = getAuthenticationOriginId(organizationId, userId);
            authenticationFactory.changePassword(organizationId, authOrigin, userId, oldPassword, newPassword);
            auditManager.auditUser(organizationId, userId, Enums.Action.CHANGE_USER_PASSWORD, userId,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, userId, Enums.Action.CHANGE_USER_PASSWORD, userId,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<User> create(User user, String password, String token) throws CatalogException {
        if (StringUtils.isEmpty(user.getOrganization())) {
            if (StringUtils.isEmpty(token)) {
                throw CatalogParameterException.isNull("user.organization");
            }
            JwtPayload payload = new JwtPayload(token);
            if (ParamConstants.ADMIN_ORGANIZATION.equals(payload.getOrganization())) {
                // Administrators always need to provide the user organization
                throw CatalogParameterException.isNull("user.organization");
            }
            logger.warn("User organization was missing. Setting it with the token's organization id '{}'.", payload.getOrganization());
            user.setOrganization(payload.getOrganization());
        }
        String organizationId = user.getOrganization();

        if (token == null && (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId) || !OPENCGA.equals(user.getId()))) {
            throw new CatalogException("Missing token parameter");
        }
        if (OPENCGA.equals(user.getId()) && !ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
            throw new CatalogException("Creating '" + OPENCGA + "' user is forbidden in any organization.");
        }

        ObjectMap auditParams = new ObjectMap("user", user);

        // Initialise fields
        ParamUtils.checkObj(user, "User");
        ParamUtils.checkValidUserId(user.getId());
        ParamUtils.checkParameter(user.getName(), "name");
        user.setEmail(ParamUtils.defaultString(user.getEmail(), ""));
        if (StringUtils.isNotEmpty(user.getEmail())) {
            checkEmail(user.getEmail());
        }
        user.setAccount(ParamUtils.defaultObject(user.getAccount(), Account::new));
        user.getAccount().setCreationDate(TimeUtils.getTime());
        if (StringUtils.isEmpty(user.getAccount().getExpirationDate())) {
            // By default, user accounts will be valid for 1 year when they are created.
            Date date = TimeUtils.add1YeartoDate(new Date());
            user.getAccount().setExpirationDate(TimeUtils.getTime(date));
        } else {
            // Validate expiration date is not over
            ParamUtils.checkDateIsNotExpired(user.getAccount().getExpirationDate(), "account.expirationDate");
        }
        user.setInternal(new UserInternal(new UserStatus(UserStatus.READY)));
        user.setQuota(ParamUtils.defaultObject(user.getQuota(), UserQuota::new));
        user.setProjects(ParamUtils.defaultObject(user.getProjects(), Collections::emptyList));
        user.setConfigs(ParamUtils.defaultObject(user.getConfigs(), HashMap::new));
        user.setFilters(ParamUtils.defaultObject(user.getFilters(), LinkedList::new));
        user.setAttributes(ParamUtils.defaultObject(user.getAttributes(), Collections::emptyMap));

        if (StringUtils.isEmpty(password)) {
            Map<String, AuthenticationManager> authOriginMap = authenticationFactory.getOrganizationAuthenticationManagers(organizationId);
            if (!authOriginMap.containsKey(user.getAccount().getAuthentication().getId())) {
                throw new CatalogException("Unknown authentication origin id '" + user.getAccount().getAuthentication() + "'");
            }
        } else {
            user.getAccount().setAuthentication(new Account.AuthenticationOrigin(CatalogAuthenticationManager.INTERNAL, false));
        }

        if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId) || !OPENCGA.equals(user.getId())) {
            JwtPayload jwtPayload = validateToken(token);
            // If it's not one of the SUPERADMIN users or the owner or one of the admins of the organisation, we should not allow it
            if (!authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, jwtPayload.getUserId(organizationId))) {
                String errorMsg = "Please ask your administrator to create your account.";
                auditManager.auditCreate(organizationId, user.getId(), Enums.Resource.USER, user.getId(), "", "", "", auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", errorMsg)));
                throw new CatalogException(errorMsg);
            }
        }

        checkUserExists(organizationId, user.getId());

        try {
            if (StringUtils.isNotEmpty(password) && !PasswordUtils.isStrongPassword(password)) {
                throw new CatalogException("Invalid password. Check password strength for user " + user.getId());
            }
            if (user.getProjects() != null && !user.getProjects().isEmpty()) {
                throw new CatalogException("Creating user and projects in a single transaction is forbidden");
            }

            getUserDBAdaptor(organizationId).insert(user, password, QueryOptions.empty());

            auditManager.auditCreate(organizationId, user.getId(), Enums.Resource.USER, user.getId(), "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return getUserDBAdaptor(organizationId).get(user.getId(), QueryOptions.empty());
        } catch (CatalogIOException | CatalogDBException e) {
            auditManager.auditCreate(organizationId, user.getId(), Enums.Resource.USER, user.getId(), "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));

            throw e;
        }
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
     * @param token        JWT token.
     * @return The created user
     * @throws CatalogException If user already exists, or unable to create a new user.
     */
    public OpenCGAResult<User> create(String id, String name, String email, String password, String organization, Long quota, String token)
            throws CatalogException {
        User user = new User(id, name, email, organization, new UserInternal(new UserStatus()))
                .setAccount(new Account("", "", null))
                .setQuota(new UserQuota().setMaxDisk(quota != null ? quota : -1));
        return create(user, password, token);
    }

    public JwtPayload validateToken(String token) throws CatalogException {
        JwtPayload jwtPayload = new JwtPayload(token);
        ParamUtils.checkParameter(jwtPayload.getUserId(), "jwt user");
        ParamUtils.checkParameter(jwtPayload.getOrganization(), "jwt organization");

        String authOrigin;
        if (ParamConstants.ANONYMOUS_USER_ID.equals(jwtPayload.getUserId())) {
            authOrigin = CatalogAuthenticationManager.INTERNAL;
        } else {
            OpenCGAResult<User> userResult = getUserDBAdaptor(jwtPayload.getOrganization()).get(jwtPayload.getUserId(), INCLUDE_ACCOUNT_AND_INTERNAL);
            if (userResult.getNumResults() == 0) {
                throw new CatalogException("User '" + jwtPayload.getUserId() + "' could not be found.");
            }
            authOrigin = userResult.first().getAccount().getAuthentication().getId();
        }

        authenticationFactory.validateToken(jwtPayload.getOrganization(), authOrigin, token);
        return jwtPayload;
    }

    public void syncAllUsersOfExternalGroup(String organizationId, String study, String authOrigin, String token) throws CatalogException {
        if (!OPENCGA.equals(authenticationFactory.getUserId(organizationId, authOrigin, token))) {
            throw new CatalogAuthorizationException("Only the root user can perform this action");
        }

        OpenCGAResult<Group> allGroups = catalogManager.getStudyManager().getGroup(study, null, token);

        boolean foundAny = false;
        for (Group group : allGroups.getResults()) {
            if (group.getSyncedFrom() != null && group.getSyncedFrom().getAuthOrigin().equals(authOrigin)) {
                logger.info("Fetching users of group '{}' from authentication origin '{}'", group.getSyncedFrom().getRemoteGroup(),
                        group.getSyncedFrom().getAuthOrigin());
                foundAny = true;

                List<User> userList;
                try {
                    userList = authenticationFactory.getUsersFromRemoteGroup(organizationId, group.getSyncedFrom().getAuthOrigin(),
                            group.getSyncedFrom().getRemoteGroup());
                } catch (CatalogException e) {
                    // There was some kind of issue for which we could not retrieve the group information.
                    logger.info("Removing all users from group '{}' belonging to group '{}' in the external authentication origin",
                            group.getId(), group.getSyncedFrom().getAuthOrigin());
                    logger.info("Please, manually remove group '{}' if external group '{}' was removed from the authentication origin",
                            group.getId(), group.getSyncedFrom().getAuthOrigin());
                    catalogManager.getStudyManager().updateGroup(study, group.getId(), ParamUtils.BasicUpdateAction.SET,
                            new GroupUpdateParams(Collections.emptyList()), token);
                    continue;
                }
                Iterator<User> iterator = userList.iterator();
                while (iterator.hasNext()) {
                    User user = iterator.next();
                    user.setOrganization(organizationId);
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
                    updateParams = new GroupUpdateParams(new ArrayList<>(userList.stream().map(User::getId).collect(Collectors.toSet())));
                }
                catalogManager.getStudyManager().updateGroup(study, group.getId(), ParamUtils.BasicUpdateAction.SET,
                        updateParams, token);
            }
        }
        if (!foundAny) {
            logger.info("No synced groups found in study '{}' from authentication origin '{}'", study, authOrigin);
        }
    }

    /**
     * Register all the users belonging to a remote group. If internalGroup and study are not null, it will also associate the remote group
     * to the internalGroup defined.
     *
     * @param organizationId Organization id.
     * @param authOrigin     Authentication origin.
     * @param remoteGroup    Group name of the remote authentication origin.
     * @param internalGroup  Group name in Catalog that will be associated to the remote group.
     * @param study          Study where the internal group will be associated.
     * @param sync           Boolean indicating whether the remote group will be synced with the internal group or not.
     * @param token          JWT token. The token should belong to the root user.
     * @throws CatalogException If any of the parameters is wrong or there is any internal error.
     */
    public void importRemoteGroupOfUsers(String organizationId, String authOrigin, String remoteGroup, @Nullable String internalGroup,
                                         @Nullable String study, boolean sync, String token) throws CatalogException {
        JwtPayload payload = validateToken(token);
        String userId = payload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("authOrigin", authOrigin)
                .append("remoteGroup", remoteGroup)
                .append("internalGroup", internalGroup)
                .append("study", study)
                .append("sync", sync)
                .append("token", token);
        try {
            if (!OPENCGA.equals(authenticationFactory.getUserId(organizationId, authOrigin, token))) {
                throw new CatalogAuthorizationException("Only the root user can perform this action");
            }

            ParamUtils.checkParameter(authOrigin, "Authentication origin");
            ParamUtils.checkParameter(remoteGroup, "Remote group");

            List<User> userList;
            if (sync) {
                // We don't create any user as they will be automatically populated during login
                userList = Collections.emptyList();
            } else {
                logger.info("Fetching users from authentication origin '{}'", authOrigin);

                // Register the users
                userList = authenticationFactory.getUsersFromRemoteGroup(organizationId, authOrigin, remoteGroup);
                for (User user : userList) {
                    user.setOrganization(organizationId);
                    try {
                        create(user, null, token);
                        logger.info("User '{}' successfully created", user.getId());
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
                    auditManager.audit(organizationId, userId, Enums.Action.IMPORT_EXTERNAL_GROUP_OF_USERS, Enums.Resource.USER,
                            group.getId(), "", study, "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                } catch (CatalogException e) {
                    logger.error("Could not register group '{}' in study '{}'\n{}", internalGroup, study, e.getMessage(), e);
                    throw new CatalogException("Could not register group '" + internalGroup + "' in study '" + study + "': "
                            + e.getMessage(), e);
                }
            }
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.IMPORT_EXTERNAL_GROUP_OF_USERS, Enums.Resource.USER, "", "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Register all the ids. If internalGroup and study are not null, it will also associate the users to the internalGroup defined.
     *
     * @param organizationId Organization id.
     * @param authOrigin     Authentication origin.
     * @param idList         List of entity ids existing in the authentication origin.
     * @param isApplication  boolean indicating whether the id list belong to external applications or users.
     * @param internalGroup  Group name in Catalog that will be associated to the remote group.
     * @param study          Study where the internal group will be associated.
     * @param token          JWT token. The token should belong to the root user.
     * @throws CatalogException If any of the parameters is wrong or there is any internal error.
     */
    public void importRemoteEntities(String organizationId, String authOrigin, List<String> idList, boolean isApplication,
                                     @Nullable String internalGroup, @Nullable String study, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("authOrigin", authOrigin)
                .append("idList", idList)
                .append("isApplication", isApplication)
                .append("internalGroup", internalGroup)
                .append("study", study)
                .append("token", token);

        JwtPayload payload = validateToken(token);
        String userId = payload.getUserId(organizationId);

        try {
            if (!OPENCGA.equals(userId)) {
                throw new CatalogAuthorizationException("Only the root user can perform this action");
            }

            ParamUtils.checkParameter(authOrigin, "Authentication origin");
            ParamUtils.checkObj(idList, "ids");

            if (!isApplication) {
                logger.info("Fetching user information from authentication origin '{}'", authOrigin);
                List<User> parsedUserList = authenticationFactory.getRemoteUserInformation(organizationId, authOrigin, idList);
                for (User user : parsedUserList) {
                    user.setOrganization(organizationId);
                    create(user, null, token);
                    auditManager.audit(organizationId, userId, Enums.Action.IMPORT_EXTERNAL_USERS, Enums.Resource.USER, user.getId(), "",
                            "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                    logger.info("User '{}' successfully created", user.getId());
                }
            } else {
                for (String applicationId : idList) {
                    User application = new User(applicationId, new Account()
                            .setAuthentication(new Account.AuthenticationOrigin(authOrigin, true)))
                            .setEmail("mail@mail.co.uk");
                    application.setOrganization(organizationId);
                    create(application, null, token);
                    auditManager.audit(organizationId, userId, Enums.Action.IMPORT_EXTERNAL_USERS, Enums.Resource.USER, application.getId(),
                            "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                    logger.info("User (application) '{}' successfully created", application.getId());
                }
            }

            if (StringUtils.isNotEmpty(internalGroup) && StringUtils.isNotEmpty(study)) {
                // Check if the group already exists
                try {
                    OpenCGAResult<Group> group = catalogManager.getStudyManager().getGroup(study, internalGroup, token);
                    if (group.getNumResults() == 1) {
                        // We will add those users to the existing group
                        catalogManager.getStudyManager().updateGroup(study, internalGroup, ParamUtils.BasicUpdateAction.ADD,
                                new GroupUpdateParams(idList), token);
                        return;
                    }
                } catch (CatalogException e) {
                    logger.warn("The group '{}' did not exist.", internalGroup);
                }

                // Create new group associating it to the remote group
                try {
                    logger.info("Attempting to register group '{}' in study '{}'", internalGroup, study);
                    Group group = new Group(internalGroup, idList);
                    catalogManager.getStudyManager().createGroup(study, group, token);
                } catch (CatalogException e) {
                    logger.error("Could not register group '{}' in study '{}'\n{}", internalGroup, study, e.getMessage());
                }
            }
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.IMPORT_EXTERNAL_USERS, Enums.Resource.USER, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Gets the user information.
     *
     * @param organizationId Organization id.
     * @param userId         User id
     * @param options        QueryOptions
     * @param token          SessionId of the user performing this operation.
     * @return The requested user
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<User> get(String organizationId, String userId, QueryOptions options, String token) throws CatalogException {
        return get(organizationId, Collections.singletonList(userId), options, token);
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
//            OpenCGAResult<User> userDataResult = getUserDBAdaptor(organizationId).get(userId, options);
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
     * @param organizationId Organization id.
     * @param userIdList     List of user id
     * @param options        QueryOptions
     * @param token          Token belonging to the user itself or administrator of any study shared with the user list requested.
     * @return The requested users
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<User> get(String organizationId, List<String> userIdList, QueryOptions options, String token)
            throws CatalogException {
        ParamUtils.checkNotEmptyArray(userIdList, "userId");
        ParamUtils.checkParameter(token, "token");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("userIdList", userIdList)
                .append("options", options)
                .append("token", token);
        JwtPayload jwtPayload = validateToken(token);

        if (userIdList.size() == 1 && jwtPayload.getUserId().equals(userIdList.get(0)) && StringUtils.isEmpty(organizationId)) {
            organizationId = jwtPayload.getOrganization();
        }
        String userId = jwtPayload.getUserId(organizationId);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        auditManager.initAuditBatch(operationUuid);
        try {
            // 1. If the user is an opencga administrator or the organization owner or admin
            // 2. Or the user is requesting its own data
            //    - return info
            if (authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, userId)
                    || (userIdList.size() == 1 && userId.equals(userIdList.get(0)))) {
                Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userIdList);
                OpenCGAResult<User> userDataResult = getUserDBAdaptor(organizationId).get(query, options);
                if (userDataResult.getNumResults() < userIdList.size()) {
                    Set<String> returnedUsers = userDataResult.getResults().stream().map(User::getId).collect(Collectors.toSet());
                    String missingUsers = userIdList.stream().filter(u -> !returnedUsers.contains(u)).collect(Collectors.joining(", "));
                    throw new CatalogException("Some users were not found: " + missingUsers);
                }
                for (String tmpUserId : userIdList) {
                    auditManager.auditInfo(organizationId, operationUuid, userId, Enums.Resource.USER, tmpUserId, "", "", "", auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                }
                return userDataResult;
            } else {
                throw CatalogAuthorizationException.notOrganizationOwnerOrAdmin("retrieve user's information.");
            }
        } catch (CatalogException e) {
            for (String tmpUserId : userIdList) {
                auditManager.auditInfo(organizationId, operationUuid, userId, Enums.Resource.USER, tmpUserId, "", "", "", auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationUuid);
        }
    }

    public OpenCGAResult<User> update(String userId, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload payload = validateToken(token);
        String organizationId = payload.getOrganization();
        String loggedUser = payload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("updateParams", parameters)
                .append("options", options)
                .append("token", token);
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkObj(parameters, "parameters");
            ParamUtils.checkParameter(token, "token");

            userId = getValidUserId(userId, payload);
            for (String s : parameters.keySet()) {
                if (!s.matches("name|email")) {
                    throw new CatalogDBException("Parameter '" + s + "' can't be changed");
                }
            }

            if (parameters.containsKey("email")) {
                checkEmail(parameters.getString("email"));
            }
            OpenCGAResult<User> updateResult = getUserDBAdaptor(organizationId).update(userId, parameters);
            auditManager.auditUpdate(organizationId, loggedUser, Enums.Resource.USER, userId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated user
                OpenCGAResult<User> result = getUserDBAdaptor(organizationId).get(userId, options);
                updateResult.setResults(result.getResults());
            }

            return updateResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, loggedUser, Enums.Resource.USER, userId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Delete entries from Catalog.
     *
     * @param organizationId Organization id.
     * @param userIdList     Comma separated list of ids corresponding to the objects to delete
     * @param options        Deleting options.
     * @param token          Token
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<User> delete(String organizationId, String userIdList, QueryOptions options, String token)
            throws CatalogException {
        ParamUtils.checkParameter(userIdList, "userIdList");
        ParamUtils.checkParameter(token, "token");

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("userIdList", userIdList)
                .append("options", options)
                .append("token", token);

        JwtPayload payload = validateToken(token);
        String tokenUser = payload.getUserId(organizationId);

        List<String> userIds = Arrays.asList(userIdList.split(","));
        OpenCGAResult<User> deletedUsers = OpenCGAResult.empty();
        for (String userId : userIds) {
            // Only if the user asking the deletion is the ADMINISTRATOR or the user to be deleted itself...
            if (OPENCGA.equals(tokenUser) || userId.equals(tokenUser)) {
                try {
                    OpenCGAResult result = getUserDBAdaptor(organizationId).delete(userId, options);

                    auditManager.auditDelete(organizationId, operationUuid, tokenUser, Enums.Resource.USER, userId, "", "", "", auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                    Query query = new Query()
                            .append(UserDBAdaptor.QueryParams.ID.key(), userId)
                            .append(UserDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.DELETED);
                    OpenCGAResult<User> deletedUser = getUserDBAdaptor(organizationId).get(query, QueryOptions.empty());
                    deletedUser.setTime(deletedUser.getTime() + result.getTime());

                    deletedUsers.append(deletedUser);
                } catch (CatalogException e) {
                    auditManager.auditDelete(organizationId, operationUuid, tokenUser, Enums.Resource.USER, userId, "", "", "", auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
                }
            }
        }
        return deletedUsers;
    }

    /**
     * Delete the entries satisfying the query.
     *
     * @param organizationId Organization id.
     * @param query          Query of the objects to be deleted.
     * @param options        Deleting options.
     * @param sessionId      sessionId.
     * @return A list with the deleted objects.
     * @throws CatalogException CatalogException
     * @throws IOException      IOException.
     */
    public OpenCGAResult<User> delete(String organizationId, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.ID.key());
        OpenCGAResult<User> userDataResult = getUserDBAdaptor(organizationId).get(query, queryOptions);
        List<String> userIds = userDataResult.getResults().stream().map(User::getId).collect(Collectors.toList());
        String userIdStr = StringUtils.join(userIds, ",");
        return delete(organizationId, userIdStr, options, sessionId);
    }

    public OpenCGAResult<User> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    public OpenCGAResult resetPassword(String userId, String token) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(token, "token");
        JwtPayload jwtPayload = validateToken(token);
        String organizationId = jwtPayload.getOrganization();
        try {
            authorizationManager.checkIsOpencgaAdministrator(jwtPayload, "reset password");
            String authOrigin = getAuthenticationOriginId(organizationId, userId);
            OpenCGAResult writeResult = authenticationFactory.resetPassword(organizationId, authOrigin, userId);

            auditManager.auditUser(organizationId, jwtPayload.getUserId(organizationId), Enums.Action.RESET_USER_PASSWORD, userId,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return writeResult;
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, jwtPayload.getUserId(organizationId), Enums.Action.RESET_USER_PASSWORD, userId,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public AuthenticationResponse loginAsAdmin(String password) throws CatalogException {
        return login(ParamConstants.ADMIN_ORGANIZATION, OPENCGA, password);
    }

    public AuthenticationResponse login(String organizationId, String username, String password) throws CatalogException {
        ParamUtils.checkParameter(username, "userId");
        ParamUtils.checkParameter(password, "password");

        String authId = null;
        AuthenticationResponse response = null;

        if (StringUtils.isEmpty(organizationId)) {
            // Try to automatically set the organization id
            if (OPENCGA.equals(username)) {
                organizationId = ParamConstants.ADMIN_ORGANIZATION;
            } else {
                List<String> organizationIds = catalogDBAdaptorFactory.getOrganizationIds();
                if (organizationIds.size() == 2) {
                    organizationId = organizationIds.stream().filter(s -> !ParamConstants.ADMIN_ORGANIZATION.equals(s)).findFirst().get();
                } else {
                    throw CatalogParameterException.isNull("organization");
                }
            }
        }

        OpenCGAResult<User> userOpenCGAResult = getUserDBAdaptor(organizationId).get(username, INCLUDE_ACCOUNT_AND_INTERNAL);
        if (userOpenCGAResult.getNumResults() == 1) {
            User user = userOpenCGAResult.first();
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                // Check user is not banned or account is expired
                if (UserStatus.BANNED.equals(user.getInternal().getStatus().getId())) {
                    throw CatalogAuthenticationException.userIsBanned(username);
                }
                Date date = TimeUtils.toDate(user.getAccount().getExpirationDate());
                if (date == null) {
                    throw new CatalogException("Unexpected null expiration date for user '" + username + "'.");
                }
                if (date.before(new Date())) {
                    throw CatalogAuthenticationException.accountIsExpired(username, user.getAccount().getExpirationDate());
                }
            }
            authId = userOpenCGAResult.first().getAccount().getAuthentication().getId();
            try {
                response = authenticationFactory.authenticate(organizationId, authId, username, password);
            } catch (CatalogAuthenticationException e) {
                if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                    // We can only lock the account if it is not the root user
                    int failedAttempts = userOpenCGAResult.first().getInternal().getFailedAttempts();
                    ObjectMap updateParams = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_FAILED_ATTEMPTS.key(), failedAttempts + 1);
                    if (failedAttempts >= 4) {
                        // Ban the account
                        updateParams.append(UserDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.BANNED);
                    }
                    getUserDBAdaptor(organizationId).update(username, updateParams);
                }

                auditManager.auditUser(organizationId, username, Enums.Action.LOGIN, username,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
                throw e;
            }
        } else {
            // We attempt to login the user with the different authentication managers
            for (Map.Entry<String, AuthenticationManager> entry
                    : authenticationFactory.getOrganizationAuthenticationManagers(organizationId).entrySet()) {
                AuthenticationManager authenticationManager = entry.getValue();
                try {
                    response = authenticationManager.authenticate(organizationId, username, password);
                    authId = entry.getKey();
                    break;
                } catch (CatalogAuthenticationException e) {
                    logger.debug("Attempted authentication failed with {} for user '{}'\n{}", entry.getKey(), username, e.getMessage(), e);
                }
            }
        }

        if (response == null) {
            auditManager.auditUser(organizationId, username, Enums.Action.LOGIN, username,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", "Incorrect user or password.")));
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        }

        // Reset login failed attempts counter
        ObjectMap updateParams = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_FAILED_ATTEMPTS.key(), 0);
        getUserDBAdaptor(organizationId).update(username, updateParams);

        auditManager.auditUser(organizationId, username, Enums.Action.LOGIN, username,
                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        String userId = authenticationFactory.getUserId(organizationId, authId, response.getToken());
        if (!CatalogAuthenticationManager.INTERNAL.equals(authId)) {
            // External authorization
            try {
                // If the user is not registered, an exception will be raised
                getUserDBAdaptor(organizationId).checkId(userId);
            } catch (CatalogDBException e) {
                // The user does not exist so we register it
                User user = authenticationFactory.getRemoteUserInformation(organizationId, authId, Collections.singletonList(userId))
                        .get(0);
                user.setOrganization(organizationId);
                // Generate a root token to be able to create the user even if the installation is private
                String rootToken = authenticationFactory.createToken(organizationId, CatalogAuthenticationManager.INTERNAL, OPENCGA);
                create(user, null, rootToken);
            }

            try {
                List<String> remoteGroups = authenticationFactory.getRemoteGroups(organizationId, authId, response.getToken());

                // Resync synced groups of user in OpenCGA
                getStudyDBAdaptor(organizationId).resyncUserWithSyncedGroups(userId, remoteGroups, authId);
            } catch (CatalogException e) {
                logger.error("Could not update synced groups for user '" + userId + "'\n" + e.getMessage(), e);
            }
        }

        return response;
    }

    public AuthenticationResponse loginAnonymous(String organizationId) throws CatalogException {
        ParamUtils.checkParameter(organizationId, "organization id");

        // Check user anonymous has access to any study within the organization
        Query query = new Query(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), ParamConstants.ANONYMOUS_USER_ID);
        OpenCGAResult<Long> count = getStudyDBAdaptor(organizationId).count(query);
        if (count.getNumMatches() == 0) {
            throw CatalogAuthenticationException.userNotFound(organizationId, ParamConstants.ANONYMOUS_USER_ID);
        }

        String token = authenticationFactory.createToken(organizationId, CatalogAuthenticationManager.INTERNAL,
                ParamConstants.ANONYMOUS_USER_ID);
        return new AuthenticationResponse(token);
    }

    /**
     * Create a new token if the token provided corresponds to the user and it is not expired yet.
     *
     * @param token          active token.
     * @return a new AuthenticationResponse object.
     * @throws CatalogException if the token does not correspond to the user or the token is expired.
     */
    public AuthenticationResponse refreshToken(String token) throws CatalogException {
        JwtPayload payload = new JwtPayload(token);
        String organizationId = payload.getOrganization();
        AuthenticationResponse response = null;
        CatalogAuthenticationException exception = null;
        String userId = "";
        // We attempt to renew the token with the different authentication managers
        for (Map.Entry<String, AuthenticationManager> entry
                : authenticationFactory.getOrganizationAuthenticationManagers(organizationId).entrySet()) {
            AuthenticationManager authenticationManager = entry.getValue();
            try {
                response = authenticationManager.refreshToken(token);
                userId = authenticationManager.getUserId(token);
                break;
            } catch (CatalogAuthenticationException e) {
                logger.debug("Could not refresh token with '{}' provider: {}", entry.getKey(), e.getMessage(), e);
                if (CatalogAuthenticationManager.INTERNAL.equals(entry.getKey())) {
                    exception = e;
                }
            }
        }

        if (response == null && exception != null) {
            auditManager.auditUser(organizationId, userId, Enums.Action.REFRESH_TOKEN, userId,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, exception.getError()));
            throw exception;
        }

        auditManager.auditUser(organizationId, userId, Enums.Action.REFRESH_TOKEN, userId,
                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        return response;
    }

    public OpenCGAResult<User> changeStatus(String organizationId, String userId, String status, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = validateToken(token);
        String userIdOrganization = StringUtils.isNotEmpty(organizationId) ? organizationId : tokenPayload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("userId", userId)
                .append("status", status)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(userIdOrganization, tokenPayload.getUserId(userIdOrganization));
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            // Validate user exists
            getUserDBAdaptor(userIdOrganization).checkId(userId);

            // Validate status is valid
            if (!UserStatus.isValid(status)) {
                throw new CatalogParameterException("Invalid status '" + status + "'. Valid values are: " + UserStatus.STATUS_LIST);
            }

            if (UserStatus.BANNED.equals(status)) {
                // Get organization information
                Set<String> ownerAndAdmins = catalogManager.getOrganizationManager().getOrganizationOwnerAndAdmins(userIdOrganization);
                if (ownerAndAdmins.contains(userId)) {
                    if (tokenPayload.getUserId().equals(userId)) {
                        // The user is trying to ban himself
                        throw new CatalogAuthorizationException("You can't ban your own account.");
                    }
                    if (!authorizationManager.isAtLeastOrganizationOwner(userIdOrganization, tokenPayload.getUserId(userIdOrganization))) {
                        // One of the admins is trying to ban the owner or one of the admins
                        throw new CatalogAuthorizationException("Only the owner of the organization can ban administrators.");
                    }
                }
            }

            // Update user status and reset failed attempts to 0
            ObjectMap updateParams = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), status);
            if (!UserStatus.BANNED.equals(status)) {
                updateParams.put(UserDBAdaptor.QueryParams.INTERNAL_FAILED_ATTEMPTS.key(), 0);
            }
            OpenCGAResult<User> result = getUserDBAdaptor(userIdOrganization).update(userId, updateParams);

            auditManager.auditUpdate(organizationId, tokenPayload.getUserId(userIdOrganization), Enums.Resource.USER, userId, "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated user
                OpenCGAResult<User> tmpResult = getUserDBAdaptor(userIdOrganization).get(userId, options);
                result.setResults(tmpResult.getResults());
            }

            return result;
        } catch (Exception e) {
            auditManager.auditUpdate(organizationId, tokenPayload.getUserId(userIdOrganization), Enums.Resource.USER, userId, "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(-1, "Could not update user status",
                            e.getMessage())));
            throw e;
        }
    }

    /**
     * This method will be only callable by the system. It generates a new session id for the user.
     *
     * @param organizationId Organization id.
     * @param userId         user id for which a session will be generated.
     * @param attributes     attributes to be put as part of the claims section in the JWT.
     * @param expiration     Expiration time in seconds. If null, default expiration time will be used.
     * @param token          Password or active session of the OpenCGA admin.
     * @return an objectMap containing the new sessionId
     * @throws CatalogException if the password is not correct or the userId does not exist.
     */
    public String getToken(String organizationId, String userId, Map<String, Object> attributes, Long expiration, String token)
            throws CatalogException {
        JwtPayload jwtPayload = validateToken(token);
        authorizationManager.checkIsOpencgaAdministrator(jwtPayload, "create tokens");

        if (expiration != null && expiration <= 0) {
            throw new CatalogException("Expiration time must be higher than 0");
        }
        AuthenticationManager authManager = getAuthenticationManagerForUser(organizationId, userId);
        return expiration != null
                ? authManager.createToken(organizationId, userId, attributes, expiration)
                : authManager.createToken(organizationId, userId, attributes);
    }

    /**
     * This method will be only callable by the system. It generates a new session id for the user.
     *
     * @param organizationId Organization id.
     * @param userId         user id for which a session will be generated.
     * @param attributes     attributes to be put as part of the claims section in the JWT.
     * @param token          Password or active session of the OpenCGA admin.
     * @return an objectMap containing the new sessionId
     * @throws CatalogException if the password is not correct or the userId does not exist.
     */
    public String getNonExpiringToken(String organizationId, String userId, Map<String, Object> attributes, String token)
            throws CatalogException {
        JwtPayload payload = validateToken(token);
        authorizationManager.checkIsOpencgaAdministrator(payload, "create tokens");

        AuthenticationManager authManager = getAuthenticationManagerForUser(organizationId, userId);
        return authManager.createNonExpiringToken(organizationId, userId, attributes);
    }

    private AuthenticationManager getAuthenticationManagerForUser(String organizationId, String user) throws CatalogException {
        OpenCGAResult<User> userOpenCGAResult = getUserDBAdaptor(organizationId).get(user, INCLUDE_ACCOUNT_AND_INTERNAL);
        if (userOpenCGAResult.getNumResults() == 1) {
            String authId = userOpenCGAResult.first().getAccount().getAuthentication().getId();
            return authenticationFactory.getOrganizationAuthenticationManager(organizationId, authId);
        } else {
            throw new CatalogException("User '" + user + "' not found.");
        }
    }

    /**
     * Add a new filter to the user account.
     * <p>
     *
     * @param userId         user id to whom the filter will be associated.
     * @param id             Filter id.
     * @param description    Filter description.
     * @param resource       Resource where the filter should be applied.
     * @param query          Query object.
     * @param queryOptions   Query options object.
     * @param token          session id of the user asking to store the filter.
     * @return the created filter.
     * @throws CatalogException if there already exists a filter with that same name for the user or if the user corresponding to the
     *                          session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> addFilter(String userId, String id, String description, Enums.Resource resource, Query query,
                                               QueryOptions queryOptions, String token) throws CatalogException {
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(token, "token");
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkObj(resource, "resource");
        ParamUtils.checkObj(query, "Query");
        ParamUtils.checkObj(queryOptions, "QueryOptions");
        if (description == null) {
            description = "";
        }

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("id", id)
                .append("description", description)
                .append("resource", resource)
                .append("query", query)
                .append("queryOptions", queryOptions)
                .append("token", token);
        try {
            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            Query queryExists = new Query()
                    .append(UserDBAdaptor.QueryParams.ID.key(), userId)
                    .append(UserDBAdaptor.QueryParams.FILTERS_ID.key(), id);
            if (getUserDBAdaptor(organizationId).count(queryExists).getNumMatches() > 0) {
                throw new CatalogException("There already exists a filter called " + id + " for user " + userId);
            }

            UserFilter filter = new UserFilter(id, description, resource, query, queryOptions);
            OpenCGAResult result = getUserDBAdaptor(organizationId).addFilter(userId, filter);
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(filter), 1);
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Update the filter information.
     * <p>
     *
     * @param userId         user id to whom the filter should be updated.
     * @param name           Filter name.
     * @param params         Map containing the parameters to be updated.
     * @param token          session id of the user asking to update the filter.
     * @return the updated filter.
     * @throws CatalogException if the filter could not be updated because the filter name is not correct or if the user corresponding to
     *                          the session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> updateFilter(String userId, String name, ObjectMap params, String token) throws CatalogException {
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("params", params)
                .append("token", token);
        try {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "token");
            ParamUtils.checkParameter(name, "name");

            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            Query queryExists = new Query()
                    .append(UserDBAdaptor.QueryParams.ID.key(), userId)
                    .append(UserDBAdaptor.QueryParams.FILTERS_ID.key(), name);
            if (getUserDBAdaptor(organizationId).count(queryExists).getNumMatches() == 0) {
                throw new CatalogException("There is no filter called " + name + " for user " + userId);
            }

            OpenCGAResult<?> result = getUserDBAdaptor(organizationId).updateFilter(userId, name, params);
            UserFilter filter = getFilter(getUserDBAdaptor(organizationId), userId, name);
            if (filter == null) {
                throw new CatalogException("Internal error: The filter " + name + " could not be found.");
            }
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(filter), 1);
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
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
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);
        try {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(token, "token");
            ParamUtils.checkParameter(name, "name");

            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            UserFilter filter = getFilter(getUserDBAdaptor(organizationId), userId, name);
            if (filter == null) {
                throw new CatalogException("There is no filter called " + name + " for user " + userId);
            }

            OpenCGAResult result = getUserDBAdaptor(organizationId).deleteFilter(userId, name);
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(filter), 1);
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
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
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);
        try {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(name, "name");

            userId = getValidUserId(userId, payload);

            UserFilter filter = getFilter(getUserDBAdaptor(organizationId), userId, name);
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.FETCH_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (filter == null) {
                throw new CatalogException("Filter " + name + " not found.");
            } else {
                return new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(filter), 1);
            }
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.FETCH_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private UserFilter getFilter(UserDBAdaptor userDBAdaptor, String userId, String name) throws CatalogException {
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

    /**
     * Retrieves all the user filters.
     *
     * @param userId user id having the filters.
     * @param token  session id of the user fetching the filters.
     * @return the filters.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    public OpenCGAResult<UserFilter> getAllFilters(String userId, String token) throws CatalogException {
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("token", token);
        try {
            ParamUtils.checkParameter(userId, "userId");

            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            Query query = new Query()
                    .append(UserDBAdaptor.QueryParams.ID.key(), userId);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.FILTERS.key());
            OpenCGAResult<User> userDataResult = getUserDBAdaptor(organizationId).get(query, queryOptions);

            if (userDataResult.getNumResults() != 1) {
                throw new CatalogException("Internal error: User " + userId + " not found.");
            }

            List<UserFilter> filters = userDataResult.first().getFilters();
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.FETCH_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(0, Collections.emptyList(), filters.size(), filters, filters.size());
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.FETCH_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
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
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("config", config)
                .append("token", token);

        try {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(name, "name");
            ParamUtils.checkObj(config, "ObjectMap");

            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            OpenCGAResult result = getUserDBAdaptor(organizationId).setConfig(userId, name, config);
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(config), 1);
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
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
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);
        try {
            ParamUtils.checkParameter(userId, "userId");
            ParamUtils.checkParameter(name, "name");

            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.CONFIGS.key());
            OpenCGAResult<User> userDataResult = getUserDBAdaptor(organizationId).get(userId, options);
            if (userDataResult.getNumResults() == 0) {
                throw new CatalogException("Internal error: Could not get user " + userId);
            }

            Map<String, ObjectMap> configs = userDataResult.first().getConfigs();
            if (configs == null) {
                throw new CatalogException("Internal error: Configuration object is null.");
            }

            if (!configs.containsKey(name)) {
                throw new CatalogException("Error: Cannot delete configuration with name " + name + ". Configuration name not found.");
            }

            OpenCGAResult result = getUserDBAdaptor(organizationId).deleteConfig(userId, name);
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult(result.getTime(), Collections.emptyList(), 1, Collections.singletonList(configs.get(name)), 1);
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.CHANGE_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
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
        JwtPayload payload = validateToken(token);
        String authenticatedUserId = payload.getUserId();
        String organizationId = payload.getOrganization();

        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("name", name)
                .append("token", token);
        try {
            ParamUtils.checkParameter(userId, "userId");

            userId = getValidUserId(userId, payload);
            getUserDBAdaptor(organizationId).checkId(userId);

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.CONFIGS.key());
            OpenCGAResult<User> userDataResult = getUserDBAdaptor(organizationId).get(userId, options);
            if (userDataResult.getNumResults() == 0) {
                throw new CatalogException("Internal error: Could not get user " + userId);
            }

            Map<String, ObjectMap> configs = userDataResult.first().getConfigs();
            if (configs == null) {
                throw new CatalogException("Internal error: Configuration object is null.");
            }

            if (StringUtils.isNotEmpty(name) && !configs.containsKey(name)) {
                throw new CatalogException("Error: Cannot fetch configuration with name " + name + ". Configuration name not found.");
            }

            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.FETCH_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult(userDataResult.getTime(), userDataResult.getEvents(), 1, Collections.singletonList(configs.get(name)),
                    1);
        } catch (CatalogException e) {
            auditManager.auditUser(organizationId, authenticatedUserId, Enums.Action.FETCH_USER_CONFIG, userId, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void checkUserExists(String organizationId, String userId) throws CatalogException {
        if (userId.equalsIgnoreCase(ANONYMOUS)) {
            throw new CatalogException("Permission denied: Cannot create users with special treatments in catalog.");
        }

        if (getUserDBAdaptor(organizationId).exists(userId)) {
            throw new CatalogException("The user already exists in our database.");
        }
    }

    private String getAuthenticationOriginId(String organizationId, String userId) throws CatalogException {
        OpenCGAResult<User> user = getUserDBAdaptor(organizationId).get(userId, new QueryOptions());
        if (user == null || user.getNumResults() == 0) {
            throw new CatalogException(userId + " user not found");
        }
        return user.first().getAccount().getAuthentication().getId();
    }

    /**
     * Compares the userId provided with the one from the token payload. If it doesn't match and the userId provided is actually an email,
     * it will fetch the user from the token from Catalog and check whether the email matches. If it matches, it will return the
     * corresponding user id or fail otherwise.
     *
     * @param userId   User id provided by the user.
     * @param payload  Token payload.
     * @return A valid user id for the user and token provided.
     * @throws CatalogException if the user cannot be retrieved for whatever reason.
     */
    private String getValidUserId(String userId, JwtPayload payload) throws CatalogException {
        if (payload.getUserId().equals(userId)) {
            return userId;
        }
        String organizationId = payload.getOrganization();
        String userFromToken = payload.getUserId();

        // User might be using the email as an id
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.EMAIL.key());
        OpenCGAResult<User> userOpenCGAResult = getUserDBAdaptor(organizationId).get(userFromToken, options);
        if (userOpenCGAResult.getNumResults() == 0) {
            throw new CatalogException("User '" + userFromToken + "' not found. Please, call to login first or talk to your administrator");
        }

        if (userId.equalsIgnoreCase(userOpenCGAResult.first().getEmail())) {
            return userFromToken;
        } else {
            throw new CatalogAuthorizationException("User '" + userFromToken + "' cannot operate on behalf of user '" + userId + "'.");
        }
    }

}
