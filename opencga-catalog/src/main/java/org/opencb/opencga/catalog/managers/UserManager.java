package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.AuthenticationOrigin;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogMetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.Account;
import org.opencb.opencga.catalog.models.QueryFilter;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserManager extends AbstractManager implements IUserManager {

    protected static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    //    private final SessionManager sessionManager;
    protected static final Pattern EMAILPATTERN = Pattern.compile(EMAIL_PATTERN);
    protected static Logger logger = LoggerFactory.getLogger(UserManager.class);
//    protected final Policies.UserCreation creationUserPolicy;

    @Deprecated
    public UserManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                       AuditManager auditManager,
                       CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
//        creationUserPolicy = Policies.UserCreation.ALWAYS;
        //creationUserPolicy = catalogProperties.getProperty(CatalogManager.CATALOG_MANAGER_POLICY_CREATION_USER,
        // Policies.UserCreation.ALWAYS);
//        sessionManager = new CatalogSessionManager(userDBAdaptor, authenticationManager);
    }

    public UserManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                       AuditManager auditManager, CatalogManager catalogManager, CatalogDBAdaptorFactory catalogDBAdaptorFactory,
                       CatalogIOManagerFactory ioManagerFactory, CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                catalogConfiguration);

//        creationUserPolicy = catalogConfiguration.getPolicies().getUserCreation();
//        sessionManager = new CatalogSessionManager(userDBAdaptor, authenticationManager);
    }

    static void checkEmail(String email) throws CatalogException {
        if (email == null || !EMAILPATTERN.matcher(email).matches()) {
            throw new CatalogException("email not valid");
        }
    }

    @Override
    public String getUserId(String sessionId) throws CatalogException {
        return authenticationManager.getUserId(sessionId);
//        if (sessionId == null || sessionId.isEmpty() || sessionId.equalsIgnoreCase("anonymous")) {
//            return "anonymous";
//        }
//        return userDBAdaptor.getUserIdBySessionId(sessionId);
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword)
            throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(oldPassword, "oldPassword");
        ParamUtils.checkParameter(newPassword, "newPassword");
//        checkSessionId(userId, sessionId);  //Only the user can change his own password
        userDBAdaptor.updateUserLastModified(userId);
        authenticationManager.changePassword(userId, oldPassword, newPassword);
    }

    @Deprecated
    @Override
    public QueryResult<User> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        return create(
                objectMap.getString("id"),
                objectMap.getString("name"),
                objectMap.getString("email"),
                objectMap.getString("password"),
                objectMap.getString("organization"), objectMap.getLong("diskQuota"), options, sessionId);
    }

    @Override
    public QueryResult<User> create(String id, String name, String email, String password, String organization, Long diskQuota,
                                    QueryOptions options, String adminPassword) throws CatalogException {

        // Check if the users can be registered publicly or just the admin.
        if (!catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().isRegisterOpen()) {
            if (adminPassword != null && !adminPassword.isEmpty()) {
                authenticationManager.authenticate("admin", adminPassword, true);
            } else {
                throw new CatalogException("The registration is closed to the public: Please talk to your administrator.");
            }
        }

        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(password, "password");
        ParamUtils.checkParameter(name, "name");
        checkEmail(email);
        organization = organization != null ? organization : "";
        checkUserExists(id);

        User user = new User(id, name, email, "", organization, User.UserStatus.READY);

        if (diskQuota != null && diskQuota > 0L) {
            user.setDiskQuota(diskQuota);
        }

        // TODO: If the registration is closed, we have to check the sessionId to see if it corresponds with the admin in order to continue.
        String userId = id;

//        switch (creationUserPolicy) {
//            case ONLY_ADMIN: {
//                userId = getUserId(sessionId);
//                if (!userId.isEmpty() && authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
//                    user.getAttributes().put("creatorUserId", userId);
//                } else {
//                    throw new CatalogException("CreateUser Fail. Required Admin role");
//                }
//                break;
//            }
//            case ANY_LOGGED_USER: {
//                ParamUtils.checkParameter(sessionId, "sessionId");
//                userId = getUserId(sessionId);
//                if (userId.isEmpty()) {
//                    throw new CatalogException("CreateUser Fail. Required existing account");
//                }
//                user.getAttributes().put("creatorUserId", userId);
//                break;
//            }
//            case ALWAYS:
//            default:
//                userId = id;
//                break;
//        }


        try {
            catalogIOManagerFactory.getDefault().createUser(user.getId());
            QueryResult<User> queryResult = userDBAdaptor.insertUser(user, options);
//            auditManager.recordCreation(AuditRecord.Resource.user, user.getId(), userId, queryResult.first(), null, null);
            auditManager.recordAction(AuditRecord.Resource.user, AuditRecord.Action.create, AuditRecord.Magnitude.low, user.getId(), userId,
                    null, queryResult.first(), null, null);
            authenticationManager.newPassword(user.getId(), password);
            return queryResult;
        } catch (CatalogIOException | CatalogDBException e) {
            if (!userDBAdaptor.userExists(user.getId())) {
                logger.error("ERROR! DELETING USER! " + user.getId());
                catalogIOManagerFactory.getDefault().deleteUser(user.getId());
            }
            throw e;
        }
    }

    @Override
    public List<QueryResult<User>> importFromExternalAuthOrigin(String authOrigin, String accountType, ObjectMap params,
                                                                String adminPassword) throws CatalogException {
        // Validate the admin password.
        ParamUtils.checkParameter(adminPassword, "Admin password or session id");
        authenticationManager.authenticate("admin", adminPassword, true);

        // Obtain the authentication origin parameters
        CatalogMetaDBAdaptor metaDBAdaptor = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor();
        QueryResult<AuthenticationOrigin> authenticationOrigin = metaDBAdaptor.getAuthenticationOrigin(authOrigin);
        if (authenticationOrigin.getNumResults() == 0) {
            throw new CatalogException("The authentication origin id " + authOrigin + " does not correspond with any id in our database.");
        }

        // Check account type
        if (accountType != null) {
            if (!Account.FULL.equalsIgnoreCase(accountType) && !Account.GUEST.equalsIgnoreCase(accountType)) {
                throw new CatalogException("The account type specified does not correspond with any of the valid ones. Valid account types:"
                        + Account.FULL + " and " + Account.GUEST);
            }
        }

        String type;
        if (Account.GUEST.equalsIgnoreCase(accountType)) {
            type = Account.GUEST;
        } else {
            type = Account.FULL;
        }


        // Obtain the users from LDAP

        QueryOptions queryOptions = new QueryOptions();
        List<QueryResult<User>> resultList = new LinkedList();

        // FOR ALL USERS...
        // Check if the user already exists in catalog
        String userId = null;
        if (userDBAdaptor.userExists(userId)) {
            resultList.add(new QueryResult<>(userId, -1, 0, 0, "", "User " + userId + " already exists", Collections.emptyList()));
            // If the account of the user is the same, check the groups
//            continue;
        }

        // Create the users in catalog
        Account account = new Account().setType(type).setAuthOrigin(authenticationOrigin.first().getId());

        if (params.get("expirationDate") != null) {
//            account.setExpirationDate(...);
        }

        User user = new User(userId, userId, "", "", "organization", account, User.UserStatus.READY, "", -1, -1, new ArrayList<>(),
                new ArrayList<>(), new ArrayList<>(), new HashMap<>(), new HashMap<>());

        QueryResult<User> userQueryResult = userDBAdaptor.insertUser(user, queryOptions);
        userQueryResult.setId(userId);

        resultList.add(userQueryResult);
// Check the groups

        return resultList;
    }

    private void checkGroupsFromExternalUser(String userId, List<String> groupIds, List<String> studyIds) throws CatalogException {
        for (String studyStr : studyIds) {
            long studyId = catalogManager.getStudyManager().getStudyId(userId, studyStr);
            if (studyId < 1) {
                throw new CatalogException("Study " + studyStr + " not found.");
            }

            // TODO: What do we do with admin session id when logging in? We will need to update the groups.
            //catalogManager.getSessionManager().createToken("admin", "private")
            for (String groupId : groupIds) {
//                try {
//                    catalogManager.getStudyManager().updateGroup(Long.toString(studyId), groupId, userId, null, null, )
//                } catch (CatalogDBException e) {
//                    // The user already belonged to the group.
//                } catch (CatalogException e) {
//                    // The group does not exist.
//                }
            }

        }
    }

    @Override
    public QueryResult<User> read(String userId, QueryOptions options, String sessionId) throws CatalogException {
        return read(userId, null, options, sessionId);
    }

    @Override
    public QueryResult<User> read(String userId, String lastModified, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (!options.containsKey(QueryOptions.INCLUDE) || options.get(QueryOptions.INCLUDE) == null) {
            List<String> excludeList;
            if (options.containsKey(QueryOptions.EXCLUDE)) {
                List<String> asStringList = options.getAsStringList(QueryOptions.EXCLUDE, ",");
                excludeList = new ArrayList<>(asStringList.size() + 3);
                excludeList.addAll(asStringList);
            } else {
                excludeList = new ArrayList<>(3);
            }
            excludeList.add(CatalogUserDBAdaptor.QueryParams.SESSIONS.key());
            excludeList.add(CatalogUserDBAdaptor.QueryParams.PASSWORD.key());
            if (!excludeList.contains(CatalogUserDBAdaptor.QueryParams.PROJECTS.key())) {
                excludeList.add("projects.studies.variableSets");
            }
            options.put(QueryOptions.EXCLUDE, excludeList);
        }

        QueryResult<User> user = userDBAdaptor.getUser(userId, options, lastModified);
        return user;
    }

    @Override
    public QueryResult<User> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * Modify some params from the user profile.
     * name
     * email
     * organization
     * attributes
     * configs
     *
     * @throws CatalogException
     */
    @Override
    public QueryResult<User> update(String userId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkObj(parameters, "parameters");

        if (sessionId != null && !sessionId.isEmpty()) {
            ParamUtils.checkParameter(sessionId, "sessionId");
            checkSessionId(userId, sessionId);
            for (String s : parameters.keySet()) {
                if (!s.matches("name|email|organization|attributes|configs")) {
                    throw new CatalogDBException("Parameter '" + s + "' can't be changed");
                }
            }
        } else {
            if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
                throw new CatalogException("Nor the administrator password nor the session id could be found. The user could not be "
                        + "updated.");
            }
            authenticationManager.authenticate("admin", catalogConfiguration.getAdmin().getPassword(), true);
        }

        if (parameters.containsKey("email")) {
            checkEmail(parameters.getString("email"));
        }
        userDBAdaptor.updateUserLastModified(userId);
        QueryResult<User> queryResult = userDBAdaptor.update(userId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.user, userId, userId, parameters, null, null);
        return queryResult;
    }

    @Override
    public List<QueryResult<User>> delete(String userIdList, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(userIdList, "userIdList");

        List<String> userIds = Arrays.asList(userIdList.split(","));
        List<QueryResult<User>> deletedUsers = new ArrayList<>(userIds.size());
        for (String userId : userIds) {
            if (sessionId != null && !sessionId.isEmpty()) {
                ParamUtils.checkParameter(sessionId, "sessionId");
                checkSessionId(userId, sessionId);
            } else {
                if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
                    throw new CatalogException("Nor the administrator password nor the session id could be found. The user could not be "
                            + "deleted.");
                }
                authenticationManager.authenticate("admin", catalogConfiguration.getAdmin().getPassword(), true);
            }

            QueryResult<User> deletedUser = userDBAdaptor.delete(userId, options);
            auditManager.recordDeletion(AuditRecord.Resource.user, userId, userId, deletedUser.first(), null, null);
            deletedUsers.add(deletedUser);
        }
        return deletedUsers;
    }

    @Override
    public List<QueryResult<User>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CatalogUserDBAdaptor.QueryParams.ID.key());
        QueryResult<User> userQueryResult = userDBAdaptor.get(query, queryOptions);
        List<String> userIds = userQueryResult.getResult().stream().map(User::getId).collect(Collectors.toList());
        String userIdStr = StringUtils.join(userIds, ",");
        return delete(userIdStr, options, sessionId);
    }

    @Override
    public List<QueryResult<User>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<User>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException("User: Operation not supported.");
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException("User: Operation not supported.");
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException("User: Operation not supported.");
    }

    @Override
    public QueryResult resetPassword(String userId) throws CatalogException {
        return authenticationManager.resetPassword(userId);
    }

    @Deprecated
    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp, 20);

        String userId = "anonymous_" + session.getId();

        // TODO sessionID should be created here

        catalogIOManagerFactory.getDefault().createAnonymousUser(userId);

        try {
            return userDBAdaptor.loginAsAnonymous(session);
        } catch (CatalogDBException e) {
            catalogIOManagerFactory.getDefault().deleteUser(userId);
            throw e;
        }

    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp) throws CatalogException, IOException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(password, "password");
        ParamUtils.checkParameter(sessionIp, "sessionIp");

        QueryResult<User> user = userDBAdaptor.getUser(userId, new QueryOptions(), "");
        if (user.getNumResults() == 0) {
            throw new CatalogException("The user id " + userId + " does not exist.");
        }

        // Check that the authentication id is valid
        String authId = user.first().getAccount().getAuthOrigin();
        // Obtain authentication manager from the map ...
//        QueryResult<AuthenticationOrigin> authOrigin = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().getAuthenticationOrigin(authId);
//        if (authOrigin.getNumResults() == 0) {
//            throw new CatalogException("Authentication id " + authId + " not found.");
//        }



        authenticationManager.authenticate(userId, password, true);
        return catalogManager.getSessionManager().createToken(userId, sessionIp);
    }

    @Override
    public QueryResult<ObjectMap> getNewUserSession(String sessionId, String userId) throws CatalogException {
        authenticationManager.authenticate("admin", sessionId, true);
        return catalogManager.getSessionManager().createToken(userId, "ADMIN");
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        return userDBAdaptor.logout(userId, sessionId);
//        switch (authorizationManager.getUserRole(userId)) {
//            case ANONYMOUS:
//                return logoutAnonymous(sessionId);
//            default:
////                List<Session> sessions = Collections.singletonList(sessionManager.logout(userId, sessionId));
////                return new QueryResult<>("logout", 0, 1, 1, "", "", sessions);
//                return userDBAdaptor.logout(userId, sessionId);
//        }
    }

    @Deprecated
    @Override
    public QueryResult logoutAnonymous(String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getUserId(sessionId);
        ParamUtils.checkParameter(userId, "userId");
        checkSessionId(userId, sessionId);

        logger.info("logout anonymous user. userId: " + userId + " sesionId: " + sessionId);

        catalogIOManagerFactory.getDefault().deleteAnonymousUser(userId);
        return userDBAdaptor.logoutAnonymous(sessionId);
    }

    @Override
    public void addQueryFilter(String sessionId, QueryFilter queryFilter) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getUserId(sessionId);
        userDBAdaptor.addQueryFilter(userId, queryFilter);
    }

    @Override
    public QueryResult<Long> deleteQueryFilter(String sessionId, String filterId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getUserId(sessionId);
        return userDBAdaptor.deleteQueryFilter(userId, filterId);
    }

    @Override
    public QueryResult<QueryFilter> getQueryFilter(String sessionId, String filterId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getUserId(sessionId);
        return userDBAdaptor.getQueryFilter(userId, filterId);
    }

    private void checkSessionId(String userId, String sessionId) throws CatalogException {
        String userIdBySessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userIdBySessionId.equals(userId)) {
            throw new CatalogException("Invalid sessionId for user: " + userId);
        }
    }

    private void checkUserExists(String userId) throws CatalogException {
        if (userId.toLowerCase().equals("admin")) {
            throw new CatalogException("Permission denied: It is not allowed the creation of another admin user.");
        } else if (userId.toLowerCase().equals("anonymous") || userId.toLowerCase().equals("daemon") || userId.equals("*")) {
            throw new CatalogException("Permission denied: Cannot create users with special treatments in catalog.");
        }

        if (userDBAdaptor.userExists(userId)) {
            throw new CatalogException("The user already exists in our database. Please, choose a different one.");
        }
    }

}
