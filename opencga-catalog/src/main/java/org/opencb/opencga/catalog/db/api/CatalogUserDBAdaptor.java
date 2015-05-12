package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Acl;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogUserDBAdaptor {

    /**
     * User methods
     * ***************************
     */
    boolean checkUserCredentials(String userId, String sessionId);

    boolean userExists(String userId);

    QueryResult<User> createUser(String userId, String userName, String email, String password, String organization, QueryOptions options)
    throws CatalogDBException;

    QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException;

    QueryResult<Integer> deleteUser(String userId) throws CatalogDBException;

    QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException;

    QueryResult logout(String userId, String sessionId) throws CatalogDBException;

    QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogDBException;

    QueryResult logoutAnonymous(String sessionId) throws CatalogDBException;

    QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException;

    QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException;

    QueryResult changeEmail(String userId, String newEmail) throws CatalogDBException;

    void updateUserLastActivity(String userId) throws CatalogDBException;

    QueryResult<User> modifyUser(String userId, ObjectMap parameters) throws CatalogDBException;

    QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException;

    QueryResult getSession(String userId, String sessionId) throws CatalogDBException;

    String getUserIdBySessionId(String sessionId);


    /**
     * Project methods
     * ***************************
     */

    QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException;

    boolean projectExists(int projectId);

    QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException;

    QueryResult<Project> getProject(int project, QueryOptions options) throws CatalogDBException;

    QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException;

    QueryResult renameProjectAlias(int projectId, String newProjectName) throws CatalogDBException;

    QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException;

    int getProjectId(String userId, String projectAlias) throws CatalogDBException;

    String getProjectOwnerId(int projectId) throws CatalogDBException;

    QueryResult<Acl> getProjectAcl(int projectId, String userId) throws CatalogDBException;

    QueryResult setProjectAcl(int projectId, Acl newAcl) throws CatalogDBException;

}
