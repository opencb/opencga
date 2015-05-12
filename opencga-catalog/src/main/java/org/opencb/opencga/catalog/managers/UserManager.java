package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.MailUtils;
import org.opencb.opencga.core.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserManager extends AbstractManager implements IUserManager {

    protected final String creationUserPolicy;

    protected static Logger logger = LoggerFactory.getLogger(UserManager.class);
    protected static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    protected static final Pattern emailPattern = Pattern.compile(EMAIL_PATTERN);

    public UserManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                       CatalogDBAdaptor catalogDBAdaptor, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, authenticationManager, catalogDBAdaptor, ioManagerFactory, catalogProperties);
        creationUserPolicy = catalogProperties.getProperty(CatalogManager.CATALOG_MANAGER_POLICY_CREATION_USER, "always");
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

    @Override
    public QueryResult<User> create(QueryOptions params, String sessionId)
            throws CatalogException {
        return create(
                params.getString("id"),
                params.getString("name"),
                params.getString("email"),
                params.getString("password"),
                params.getString("organization"),
                params,sessionId
        );
    }

    @Override
    public QueryResult<User> create(String id, String name, String email, String password, String organization,
                                    QueryOptions options, String sessionId)
            throws CatalogException {

        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(password, "password");
        ParamUtils.checkParameter(name, "name");
        checkEmail(email);
        organization = organization != null ? organization : "";

        User user = new User(id, name, email, password, organization, User.Role.USER, "");

        switch (creationUserPolicy) {
            case "onlyAdmin": {
                String userId = getUserId(sessionId);
                if (!userId.isEmpty() && authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
                    user.getAttributes().put("creatorUserId", userId);
                } else {
                    throw new CatalogException("CreateUser Fail. Required Admin role");
                }
                break;
            }
            case "anyLoggedUser": {
                ParamUtils.checkParameter(sessionId, "sessionId");
                String userId = getUserId(sessionId);
                if (userId.isEmpty()) {
                    throw new CatalogException("CreateUser Fail. Required existing account");
                }
                user.getAttributes().put("creatorUserId", userId);
                break;
            }
            case "always":
            default:
                break;
        }


        try {
            catalogIOManagerFactory.getDefault().createUser(user.getId());
            return userDBAdaptor.insertUser(user, options);
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
    public QueryResult<User> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        return null;
    }

    /**
     * Modify some params from the user profile:
     *  name
     *  email
     *  organization
     *  attributes
     *  configs
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
        return userDBAdaptor.modifyUser(userId, parameters);
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
            userDBAdaptor.deleteUser(userId);
        }
        user.setId("deleteUser");
        return user;
    }

    @Override
    public QueryResult resetPassword(String userId, String email) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(email, "email");
        userDBAdaptor.updateUserLastActivity(userId);

        String newPassword = StringUtils.randomString(6);
        String newCryptPass;
        try {
            newCryptPass = StringUtils.sha1(newPassword);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogDBException("could not encode password");
        }

        QueryResult qr = userDBAdaptor.resetPassword(userId, email, newCryptPass);

        String mailUser = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_USER);
        String mailPassword = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PASSWORD);
        String mailHost = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_HOST);
        String mailPort = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PORT);

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return qr;
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

        return userDBAdaptor.login(userId, password, session);
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        switch (authorizationManager.getUserRole(userId)) {
            default:
                return userDBAdaptor.logout(userId, sessionId);
            case ANONYMOUS:
                return logoutAnonymous(sessionId);
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

    static void checkEmail(String email) throws CatalogException {
        if (email == null || !emailPattern.matcher(email).matches()) {
            throw new CatalogException("email not valid");
        }
    }

}
