package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.Project;
import org.opencb.opencga.catalog.beans.Session;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogUserDBAdaptor {

    /**
     * User methods
     * ***************************
     */
    public abstract boolean checkUserCredentials(String userId, String sessionId);

    public abstract boolean userExists(String userId);

    public abstract QueryResult<User> createUser(String userId, String userName, String email, String password, String organization, QueryOptions options)
            throws CatalogDBException;

    public abstract QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Integer> deleteUser(String userId) throws CatalogDBException;

    public abstract QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException;

    public abstract QueryResult logout(String userId, String sessionId) throws CatalogDBException;

    public abstract QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogDBException;

    public abstract QueryResult logoutAnonymous(String sessionId) throws CatalogDBException;

    public abstract QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException;

    public abstract QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException;

    public abstract QueryResult changeEmail(String userId, String newEmail) throws CatalogDBException;

    public abstract void updateUserLastActivity(String userId) throws CatalogDBException;

    public abstract QueryResult<User> modifyUser(String userId, ObjectMap parameters) throws CatalogDBException;

    public abstract QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException;

    public abstract QueryResult getSession(String userId, String sessionId) throws CatalogDBException;

    public abstract String getUserIdBySessionId(String sessionId);


    /**
     * Project methods
     * ***************************
     */

    public abstract QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException;

    public abstract boolean projectExists(int projectId);

    public abstract QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Project> getProject(int project, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException;

    public abstract QueryResult renameProjectAlias(int projectId, String newProjectName) throws CatalogDBException;

    public abstract QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException;

    public abstract int getProjectId(String userId, String projectAlias) throws CatalogDBException;

    public abstract String getProjectOwnerId(int projectId) throws CatalogDBException;

    public abstract QueryResult<Acl> getProjectAcl(int projectId, String userId) throws CatalogDBException;

    public abstract QueryResult setProjectAcl(int projectId, Acl newAcl) throws CatalogDBException;

}
