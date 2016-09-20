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
import org.opencb.opencga.catalog.auth.authentication.LDAPAuthenticationManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.AuthenticationOrigin;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
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

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserManager extends AbstractManager implements IUserManager {

    private String INTERNAL_AUTHORIZATION = "internal";
    private Map<String, AuthenticationManager> authenticationManagerMap;

    protected static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    //    private final SessionManager sessionManager;
    protected static final Pattern EMAILPATTERN = Pattern.compile(EMAIL_PATTERN);
    protected static Logger logger = LoggerFactory.getLogger(UserManager.class);
//    protected final Policies.UserCreation creationUserPolicy;

    public UserManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                       DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);

        authenticationManagerMap = new HashMap<>();
        if (catalogConfiguration.getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : catalogConfiguration.getAuthenticationOrigins()) {
                if (authenticationOrigin.getId() != null) {
                    switch (authenticationOrigin.getType()) {
                        case LDAP:
                            authenticationManagerMap.put(authenticationOrigin.getId(),
                                    new LDAPAuthenticationManager(authenticationOrigin.getHost()));
                            break;
//                    case OPENCGA:
                        default:
//                        authenticationManagerMap.put(authenticationOrigin.getId(),
//                                new CatalogAuthenticationManager(catalogDBAdaptorFactory, catalogConfiguration));
//                        INTERNAL_AUTHORIZATION = authenticationOrigin.getId();
                            break;
                    }
                }
            }
        }
        // Even if internal authentication is not present in the configuration file, create it
        authenticationManagerMap.putIfAbsent(INTERNAL_AUTHORIZATION,
                new CatalogAuthenticationManager(catalogDBAdaptorFactory, catalogConfiguration));
        AuthenticationOrigin authenticationOrigin = new AuthenticationOrigin();
        if (catalogConfiguration.getAuthenticationOrigins() == null) {
            catalogConfiguration.setAuthenticationOrigins(Arrays.asList(authenticationOrigin));
        } else {
            // Check if OPENCGA authentication is already present in catalog configuration
            boolean catalogPresent = false;
            for (AuthenticationOrigin origin : catalogConfiguration.getAuthenticationOrigins()) {
                if (AuthenticationOrigin.AuthenticationType.OPENCGA == origin.getType()) {
                    catalogPresent = true;
                    break;
                }
            }
            if (!catalogPresent) {
                List<AuthenticationOrigin> linkedList = new LinkedList<>();
                linkedList.addAll(catalogConfiguration.getAuthenticationOrigins());
                linkedList.add(authenticationOrigin);
                catalogConfiguration.setAuthenticationOrigins(linkedList);
            }
        }
    }

    static void checkEmail(String email) throws CatalogException {
        if (email == null || !EMAILPATTERN.matcher(email).matches()) {
            throw new CatalogException("email not valid");
        }
    }

    @Override
    public String getId(String sessionId) throws CatalogException {
        return authenticationManagerMap.get(INTERNAL_AUTHORIZATION).getUserId(sessionId);
//        if (sessionId == null || sessionId.isEmpty() || sessionId.equalsIgnoreCase("anonymous")) {
//            return "anonymous";
//        }
//        return userDBAdaptor.getUserIdBySessionId(sessionId);
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(oldPassword, "oldPassword");
        ParamUtils.checkParameter(newPassword, "newPassword");
        userDBAdaptor.checkId(userId);
        String authOrigin = getAuthenticationOriginId(userId);
        authenticationManagerMap.get(authOrigin).changePassword(userId, oldPassword, newPassword);
        userDBAdaptor.updateUserLastModified(userId);
    }

    private String getAuthenticationOriginId(String userId) throws CatalogException {
        QueryResult<User> user = userDBAdaptor.get(userId, new QueryOptions(), "");
        if (user == null || user.getNumResults() == 0) {
            throw new CatalogException(userId + " user not found");
        }
        return user.first().getAccount().getAuthOrigin();
    }

    private AuthenticationOrigin getAuthenticationOrigin(String authOrigin) {
        if (catalogConfiguration.getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : catalogConfiguration.getAuthenticationOrigins()) {
                if (authOrigin.equals(authenticationOrigin.getId())) {
                    return authenticationOrigin;
                }
            }
        }
        return null;
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
                authenticationManagerMap.get(INTERNAL_AUTHORIZATION).authenticate("admin", adminPassword, true);
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
        user.getAccount().setAuthOrigin(INTERNAL_AUTHORIZATION);

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
            QueryResult<User> queryResult = userDBAdaptor.insert(user, options);
//            auditManager.recordCreation(AuditRecord.Resource.user, user.getId(), userId, queryResult.first(), null, null);
            auditManager.recordAction(AuditRecord.Resource.user, AuditRecord.Action.create, AuditRecord.Magnitude.low, user.getId(), userId,
                    null, queryResult.first(), null, null);
            authenticationManagerMap.get(INTERNAL_AUTHORIZATION).newPassword(user.getId(), password);
            return queryResult;
        } catch (CatalogIOException | CatalogDBException e) {
            if (!userDBAdaptor.exists(user.getId())) {
                logger.error("ERROR! DELETING USER! " + user.getId());
                catalogIOManagerFactory.getDefault().deleteUser(user.getId());
            }
            throw e;
        }
    }

    @Override
    public List<QueryResult<User>> importFromExternalAuthOrigin(String authOrigin, String accountType, ObjectMap params,
                                                                String adminPassword) throws CatalogException, NamingException {
        // Validate the admin password.
        ParamUtils.checkParameter(adminPassword, "Admin password or session id");
        authenticationManagerMap.get(INTERNAL_AUTHORIZATION).authenticate("admin", adminPassword, true);

        if (INTERNAL_AUTHORIZATION.equals(authOrigin)) {
            throw new CatalogException("Cannot import users from catalog. Authentication origin should be external.");
        }

        // Obtain the authentication origin parameters
        AuthenticationOrigin authenticationOrigin = getAuthenticationOrigin(authOrigin);
        if (authenticationOrigin == null) {
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

        List<String> users = params.getAsStringList("users");
        if (users == null || users.size() == 0) {
            throw new CatalogException("Cannot import users. List of users is empty.");
        }

        String userFilter;
        if (users.size() == 1) {
            userFilter = "(uid=" + users.get(0) + ")";
        } else {
            userFilter = StringUtils.join(users.toArray(), ")(uid=");
            userFilter = "(|(uid=" + userFilter + "))";
        }

        // Obtain users from external origin
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, authenticationOrigin.getHost());

        DirContext dctx = new InitialDirContext(env);
        String base = ((String) authenticationOrigin.getOptions().get(AuthenticationOrigin.USERS_SEARCH));
        String[] attributeFilter = { "displayname", "mail", "uid", "gecos"};
        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        sc.setReturningAttributes(attributeFilter);
        NamingEnumeration<SearchResult> search = dctx.search(base, userFilter, sc);

        QueryOptions queryOptions = new QueryOptions();
        List<QueryResult<User>> resultList = new LinkedList();
        while (search.hasMore()) {
            SearchResult sr = search.next();
            Attributes attrs = sr.getAttributes();

            String displayname = (String) attrs.get("displayname").get(0);
            String mail = (String) attrs.get("mail").get(0);
            String uid = (String) attrs.get("uid").get(0);
            String rdn = (String) attrs.get("gecos").get(0);

            // Check if the user already exists in catalog
            if (userDBAdaptor.exists(uid)) {
                resultList.add(new QueryResult<>(uid, -1, 0, 0, "", "User " + uid + " already exists", Collections.emptyList()));
                // TODO: If the account of the user is the same, check the groups
                continue;
            }

            // Create the users in catalog
            Account account = new Account().setType(type).setAuthOrigin(authOrigin);

            // TODO: Parse expiration date
//            if (params.get("expirationDate") != null) {
//                account.setExpirationDate(...);
//            }

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("LDAP_RDN", rdn);
            User user = new User(uid, displayname, mail, "", base, account, User.UserStatus.READY, "", -1, -1, new ArrayList<>(),
                    new ArrayList<>(), new ArrayList<>(), new HashMap<>(), attributes);

            QueryResult<User> userQueryResult = userDBAdaptor.insert(user, queryOptions);
            userQueryResult.setId(uid);

            resultList.add(userQueryResult);
        }

        return resultList;
    }

    private void checkGroupsFromExternalUser(String userId, List<String> groupIds, List<String> studyIds) throws CatalogException {
        for (String studyStr : studyIds) {
            long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            if (studyId < 1) {
                throw new CatalogException("Study " + studyStr + " not found.");
            }

            // TODO: What do we do with admin session id when logging in? We will need to update the groups.
            //catalogManager.getSessionManager().createToken("admin", "private")
//            for (String groupId : groupIds) {
//                try {
//                    catalogManager.getStudyManager().updateGroup(Long.toString(studyId), groupId, userId, null, null, )
//                } catch (CatalogDBException e) {
//                    // The user already belonged to the group.
//                } catch (CatalogException e) {
//                    // The group does not exist.
//                }
//            }

        }
    }

    @Override
    public QueryResult<User> get(String userId, QueryOptions options, String sessionId) throws CatalogException {
        return get(userId, null, options, sessionId);
    }

    @Override
    public QueryResult<User> get(String userId, String lastModified, QueryOptions options, String sessionId)
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
            excludeList.add(UserDBAdaptor.QueryParams.SESSIONS.key());
            excludeList.add(UserDBAdaptor.QueryParams.PASSWORD.key());
            if (!excludeList.contains(UserDBAdaptor.QueryParams.PROJECTS.key())) {
                excludeList.add("projects.studies.variableSets");
            }
            options.put(QueryOptions.EXCLUDE, excludeList);
        }

        QueryResult<User> user = userDBAdaptor.get(userId, options, lastModified);
        return user;
    }

    @Override
    public QueryResult<User> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
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
            authenticationManagerMap.get(INTERNAL_AUTHORIZATION).authenticate("admin", catalogConfiguration.getAdmin().getPassword(), true);
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
                authenticationManagerMap.get(INTERNAL_AUTHORIZATION)
                        .authenticate("admin", catalogConfiguration.getAdmin().getPassword(), true);
            }

            QueryResult<User> deletedUser = userDBAdaptor.delete(userId, options);
            auditManager.recordDeletion(AuditRecord.Resource.user, userId, userId, deletedUser.first(), null, null);
            deletedUsers.add(deletedUser);
        }
        return deletedUsers;
    }

    @Override
    public List<QueryResult<User>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.ID.key());
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
        String authOrigin = getAuthenticationOriginId(userId);
        return authenticationManagerMap.get(authOrigin).resetPassword(userId);
    }

    @Override
    public void validatePassword(String userId, String password, boolean throwException) throws CatalogException {
        if (userId.equalsIgnoreCase("admin")) {
            authenticationManagerMap.get(INTERNAL_AUTHORIZATION).authenticate("admin", password, throwException);
        } else {
            String authOrigin = getAuthenticationOriginId(userId);
            authenticationManagerMap.get(authOrigin).authenticate(userId, password, throwException);
        }
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

        String authId;
        QueryResult<User> user = null;
        if (!userId.equals("admin")) {
            user = userDBAdaptor.get(userId, new QueryOptions(), null);
            if (user.getNumResults() == 0) {
                throw new CatalogException("The user id " + userId + " does not exist.");
            }

            // Check that the authentication id is valid
            authId = getAuthenticationOriginId(userId);
        } else {
            authId = INTERNAL_AUTHORIZATION;
        }
        AuthenticationOrigin authenticationOrigin = getAuthenticationOrigin(authId);

        if (authenticationOrigin == null) {
            throw new CatalogException("Could not find authentication origin " + authId + " for user " + userId);
        }

        if (AuthenticationOrigin.AuthenticationType.LDAP == authenticationOrigin.getType()) {
            if (user == null) {
                throw new CatalogException("Internal error: This error should never happen.");
            }
            authenticationManagerMap.get(authId).authenticate(((String) user.first().getAttributes().get("LDAP_RDN")), password, true);
        } else {
            authenticationManagerMap.get(authId).authenticate(userId, password, true);
        }

        return catalogManager.getSessionManager().createToken(userId, sessionIp);
    }

    @Override
    public QueryResult<ObjectMap> getNewUserSession(String sessionId, String userId) throws CatalogException {
        authenticationManagerMap.get(INTERNAL_AUTHORIZATION).authenticate("admin", sessionId, true);
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
        String userId = getId(sessionId);
        ParamUtils.checkParameter(userId, "userId");
        checkSessionId(userId, sessionId);

        logger.info("logout anonymous user. userId: " + userId + " sesionId: " + sessionId);

        catalogIOManagerFactory.getDefault().deleteAnonymousUser(userId);
        return userDBAdaptor.logoutAnonymous(sessionId);
    }

    @Override
    public void addQueryFilter(String sessionId, QueryFilter queryFilter) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getId(sessionId);
        userDBAdaptor.addQueryFilter(userId, queryFilter);
    }

    @Override
    public QueryResult<Long> deleteQueryFilter(String sessionId, String filterId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getId(sessionId);
        return userDBAdaptor.deleteQueryFilter(userId, filterId);
    }

    @Override
    public QueryResult<QueryFilter> getQueryFilter(String sessionId, String filterId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = getId(sessionId);
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

        if (userDBAdaptor.exists(userId)) {
            throw new CatalogException("The user already exists in our database. Please, choose a different one.");
        }
    }

}
