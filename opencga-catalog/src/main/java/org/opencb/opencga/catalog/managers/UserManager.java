package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserManager extends AbstractManager implements IUserManager {

    protected static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    //    private final SessionManager sessionManager;
    protected static final Pattern EMAILPATTERN = Pattern.compile(EMAIL_PATTERN);
    protected static Logger logger = LoggerFactory.getLogger(UserManager.class);
    protected final String creationUserPolicy;

    public UserManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                       AuditManager auditManager,
                       CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
        creationUserPolicy = catalogProperties.getProperty(CatalogManager.CATALOG_MANAGER_POLICY_CREATION_USER, "always");
//        sessionManager = new CatalogSessionManager(userDBAdaptor, authenticationManager);
    }

    static void checkEmail(String email) throws CatalogException {
        if (email == null || !EMAILPATTERN.matcher(email).matches()) {
            throw new CatalogException("email not valid");
        }
    }

    @Override
    public String getUserId(String sessionId) {
        return userDBAdaptor.getUserIdBySessionId(sessionId);
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword)
            throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(oldPassword, "oldPassword");
        ParamUtils.checkParameter(newPassword, "newPassword");
//        checkSessionId(userId, sessionId);  //Only the user can change his own password
        userDBAdaptor.updateUserLastActivity(userId);
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
                objectMap.getString("organization"),
                options, sessionId
        );
    }

    @Override
    public QueryResult<User> create(String id, String name, String email, String password, String organization,
                                    QueryOptions options, String sessionId) throws CatalogException {

        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(password, "password");
        ParamUtils.checkParameter(name, "name");
        checkEmail(email);
        organization = organization != null ? organization : "";

        User user = new User(id, name, email, "", organization, User.Role.USER, new Status());

        String userId;
        switch (creationUserPolicy) {
            case "onlyAdmin": {
                userId = getUserId(sessionId);
                if (!userId.isEmpty() && authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
                    user.getAttributes().put("creatorUserId", userId);
                } else {
                    throw new CatalogException("CreateUser Fail. Required Admin role");
                }
                break;
            }
            case "anyLoggedUser": {
                ParamUtils.checkParameter(sessionId, "sessionId");
                userId = getUserId(sessionId);
                if (userId.isEmpty()) {
                    throw new CatalogException("CreateUser Fail. Required existing account");
                }
                user.getAttributes().put("creatorUserId", userId);
                break;
            }
            case "always":
            default:
                userId = id;
                break;
        }


        try {
            catalogIOManagerFactory.getDefault().createUser(user.getId());
            QueryResult<User> queryResult = userDBAdaptor.insertUser(user, options);
            auditManager.recordCreation(AuditRecord.Resource.user, user.getId(), userId, queryResult.first(), null, null);
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
    public QueryResult<User> read(String userId, QueryOptions options, String sessionId)
            throws CatalogException {
        return read(userId, null, options, sessionId);
    }

    @Override
    public QueryResult<User> read(String userId, String lastActivity, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (!options.containsKey("include") && !options.containsKey("exclude")) {
            options.put("exclude", Arrays.asList("password", "sessions"));
        }
//        if(options.containsKey("exclude")) {
//            options.getListAs("exclude", String.class).add("sessions");
//        }
        //FIXME: Should other users get access to other user information? (If so, then filter projects)
        //FIXME: Should setPassword(null)??
        QueryResult<User> user = userDBAdaptor.getUser(userId, options, lastActivity);
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
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkObj(parameters, "parameters");
        checkSessionId(userId, sessionId);
        for (String s : parameters.keySet()) {
            if (!s.matches("name|email|organization|attributes|configs")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }
        if (parameters.containsKey("email")) {
            checkEmail(parameters.getString("email"));
        }
        userDBAdaptor.updateUserLastActivity(userId);
        QueryResult<User> queryResult = userDBAdaptor.update(userId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.user, userId, userId, parameters, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<User> delete(String userId, QueryOptions options, String sessionId)
            throws CatalogException {
        QueryResult<User> user = read(userId, options, sessionId);
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userIdBySessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (userIdBySessionId.equals(userId) || authorizationManager.getUserRole(userIdBySessionId).equals(User.Role.ADMIN)) {
            try {
                catalogIOManagerFactory.getDefault().deleteUser(userId);
            } catch (CatalogIOException e) {
                e.printStackTrace();
            }
            userDBAdaptor.delete(userId, false);
        }
        user.setId("deleteUser");
        auditManager.recordDeletion(AuditRecord.Resource.user, userId, userId, user, null, null);
        return user;
    }

    @Override
    public QueryResult resetPassword(String userId, String email) throws CatalogException {
        return authenticationManager.resetPassword(userId, email);
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

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
    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(password, "password");
        ParamUtils.checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

//        Session login = sessionManager.login(userId, password, sessionIp);
//        ObjectMap objectMap = new ObjectMap("sessionId", login.getId());
//        objectMap.put("userId", userId);
//        return new QueryResult<>("login", 0, 1, 1, null, null, Collections.singletonList(objectMap));

        // FIXME This should code above
        return userDBAdaptor.login(userId, (password.length() != 40)
                ? CatalogAuthenticationManager.cipherPassword(password)
                : password, session);
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        switch (authorizationManager.getUserRole(userId)) {
            case ANONYMOUS:
                return logoutAnonymous(sessionId);
            default:
//                List<Session> sessions = Collections.singletonList(sessionManager.logout(userId, sessionId));
//                return new QueryResult<>("logout", 0, 1, 1, "", "", sessions);
                return userDBAdaptor.logout(userId, sessionId);
        }
    }

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

    private void checkSessionId(String userId, String sessionId) throws CatalogException {
        String userIdBySessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userIdBySessionId.equals(userId)) {
            throw new CatalogException("Invalid sessionId for user: " + userId);
        }
    }

}
