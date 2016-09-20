package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IProjectManager;
import org.opencb.opencga.catalog.models.Account;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.ParamUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectManager extends AbstractManager implements IProjectManager {

    @Deprecated
    public ProjectManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                          DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                          Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public ProjectManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                          DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                          CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                catalogConfiguration);
    }

    @Override
    public String getUserId(long projectId) throws CatalogException {
        return projectDBAdaptor.getOwnerId(projectId);
    }

    @Override
    public long getId(String userId, String projectStr) throws CatalogDBException {
        if (StringUtils.isNumeric(projectStr)) {
            return Long.parseLong(projectStr);
        }

        String userOwner;
        String projectAlias;

        String[] split = projectStr.split("@");
        if (split.length == 2) {
            // user@project
            userOwner = split[0];
            projectAlias = split[1];
        } else {
            // project
            userOwner = userId;
            projectAlias = projectStr;
        }
        return projectDBAdaptor.getId(userOwner, projectAlias);
    }

    @Deprecated
    @Override
    public long getId(String projectId) throws CatalogException {
        if (StringUtils.isNumeric(projectId)) {
            return Long.parseLong(projectId);
        }

        String[] split = projectId.split("@");
        if (split.length != 2) {
            return -1;
        }
        return projectDBAdaptor.getId(split[0], split[1]);
    }

    @Override
    public QueryResult<Project> create(String name, String alias, String description,
                                       String organization, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkAlias(alias, "alias");
        ParamUtils.checkParameter(sessionId, "sessionId");

        //Only the user can create a project
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (userId.isEmpty()) {
            throw new CatalogException("The session id introduced does not correspond to any registered user.");
        }

        // Check that the account type is not guest
        QueryResult<User> user = userDBAdaptor.get(userId, new QueryOptions(), null);
        if (user.getNumResults() == 0) {
            throw new CatalogException("Internal error happened. Could not find user " + userId);
        }

        if (Account.GUEST.equalsIgnoreCase(user.first().getAccount().getType())) {
            throw new CatalogException("User " + userId + " has a guest account and is not authorized to create new projects. If you "
                    + " think this might be an error, please contact with your administrator.");
        }

        description = description != null ? description : "";
        organization = organization != null ? organization : "";

        Project project = new Project(name, alias, description, new Status(), organization);

        QueryResult<Project> queryResult = projectDBAdaptor.insert(project, userId, options);
        project = queryResult.getResult().get(0);

        try {
            catalogIOManagerFactory.getDefault().createProject(userId, Long.toString(project.getId()));
        } catch (CatalogIOException e) {
            e.printStackTrace();
            projectDBAdaptor.delete(project.getId(), new QueryOptions());
        }
        userDBAdaptor.updateUserLastModified(userId);
//        auditManager.recordCreation(AuditRecord.Resource.project, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.project, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Deprecated
    @Override
    public QueryResult<Project> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        return create(
                objectMap.getString("name"),
                objectMap.getString("alias"),
                objectMap.getString("description"),
                objectMap.getString("organization"),
                options, sessionId
        );
    }

    @Override
    public QueryResult<Project> get(Long projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkProjectPermission(projectId, userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);
        QueryResult<Project> projectResult = projectDBAdaptor.get(projectId, options);
        if (!projectResult.getResult().isEmpty()) {
            authorizationManager.filterStudies(userId, projectResult.getResult().get(0).getStudies());
        }
        return projectResult;
    }

    @Override
    public QueryResult<Project> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (userId.isEmpty()) {
            throw new CatalogException("The session id is not valid.");
        }

        String ownerId = query.getString("ownerId", query.getString("userId", userId));

        ParamUtils.checkParameter(ownerId, "ownerId");
        ParamUtils.checkParameter(sessionId, "sessionId");

        QueryResult<Project> allProjects = projectDBAdaptor.get(ownerId, options);

        List<Project> projects = allProjects.getResult();
        authorizationManager.filterProjects(userId, projects);
        allProjects.setResult(projects);
        allProjects.setNumResults(projects.size());

        return allProjects;
    }

    @Override
    public QueryResult<Project> update(Long projectId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = projectDBAdaptor.getOwnerId(projectId);

        if (!userId.equals(ownerId)) {
            throw new CatalogException("Permission denied: Only the owner of the project can update it.");
        }

        if (parameters.containsKey("alias")) {
            rename(projectId, parameters.getString("alias"), sessionId);

            //Clone and remove alias from parameters. Do not modify the original parameter
            parameters = new ObjectMap(parameters);
            parameters.remove("alias");
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|description|organization|status|attributes")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }
        userDBAdaptor.updateUserLastModified(ownerId);
        QueryResult<Project> queryResult = new QueryResult<>();
        if (parameters.size() > 0) {
            queryResult = projectDBAdaptor.update(projectId, parameters);
        }
        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, parameters, null, null);
        return queryResult;
    }

    public QueryResult rename(long projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(newProjectAlias, "newProjectAlias");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = projectDBAdaptor.getOwnerId(projectId);

        if (!userId.equals(ownerId)) {
            throw new CatalogException("Permission denied: Only the owner of the project can update it.");
        }

        userDBAdaptor.updateUserLastModified(ownerId);
        QueryResult queryResult = projectDBAdaptor.renameAlias(projectId, newProjectAlias);
        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, new ObjectMap("alias", newProjectAlias), null, null);
        return queryResult;
    }

    @Override
    public List<QueryResult<Project>> delete(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<Project>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<Project>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<Project>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult rank(String userId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", -1, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(String userId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", -1, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(String userId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", -1, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }
}
