package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IProjectManager;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.utils.ParamUtils;

import java.util.List;
import java.util.Properties;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectManager extends AbstractManager implements IProjectManager {

    public ProjectManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                          AuditManager auditManager,
                          CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                          Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    @Override
    public String getUserId(int projectId) throws CatalogException {
        return projectDBAdaptor.getProjectOwnerId(projectId);
    }

    @Override
    public int getProjectId(String projectId) throws CatalogException {
        try {
            return Integer.parseInt(projectId);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        String[] split = projectId.split("@");
        if (split.length != 2) {
            return -1;
        }
        return projectDBAdaptor.getProjectId(split[0], split[1]);
    }

    @Override
    public QueryResult<Project> create(String ownerId, String name, String alias, String description,
                                       String organization, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(ownerId, "ownerId");
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkAlias(alias, "alias");
        ParamUtils.checkParameter(sessionId, "sessionId");

        //Only the user can create a project
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)
                && !userId.equals(ownerId)) {
            throw new CatalogException("Only the user \"" + ownerId + "\" can create a project with himself as owner");
        }

        description = description != null ? description : "";
        organization = organization != null ? organization : "";

        Project project = new Project(name, alias, description, "", organization);

        /* Add default ACL */
        //Add generic permissions to the project.
        project.getAcl().add(new AclEntry(AclEntry.USER_OTHERS_ID, false, false, false, false));

        QueryResult<Project> queryResult = projectDBAdaptor.createProject(ownerId, project, options);
        project = queryResult.getResult().get(0);

        try {
            catalogIOManagerFactory.getDefault().createProject(ownerId, Integer.toString(project.getId()));
        } catch (CatalogIOException e) {
            e.printStackTrace();
            projectDBAdaptor.deleteProject(project.getId());
        }
        userDBAdaptor.updateUserLastActivity(ownerId);
        auditManager.recordCreation(AuditRecord.Resource.project, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Project> create(QueryOptions params, String sessionId) throws CatalogException {
        return create(params.getString("ownerId"),
                params.getString("name"),
                params.getString("alias"),
                params.getString("description"),
                params.getString("organization"),
                params, sessionId
        );
    }

    @Override
    public QueryResult<Project> read(Integer projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.READ);
        QueryResult<Project> projectResult = projectDBAdaptor.getProject(projectId, options);
        if (!projectResult.getResult().isEmpty()) {
            authorizationManager.filterStudies(userId, projectResult.getResult().get(0).getStudies());
        }
        return projectResult;
    }

    @Override
    public QueryResult<Project> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = query.getString("ownerId", query.getString("userId", userId));

        ParamUtils.checkParameter(ownerId, "ownerId");
        ParamUtils.checkParameter(sessionId, "sessionId");

        QueryResult<Project> allProjects = projectDBAdaptor.getAllProjects(ownerId, options);

        List<Project> projects = allProjects.getResult();
        authorizationManager.filterProjects(userId, projects);
        allProjects.setResult(projects);
        allProjects.setNumResults(projects.size());

        return allProjects;
    }

    @Override
    public QueryResult<Project> update(Integer projectId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = projectDBAdaptor.getProjectOwnerId(projectId);
        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.WRITE);

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
        userDBAdaptor.updateUserLastActivity(ownerId);
        QueryResult<Project> queryResult = projectDBAdaptor.modifyProject(projectId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, parameters, null, null);
        return queryResult;
    }

    public QueryResult rename(int projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(newProjectAlias, "newProjectAlias");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = projectDBAdaptor.getProjectOwnerId(projectId);

        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.WRITE);

        userDBAdaptor.updateUserLastActivity(ownerId);
        QueryResult queryResult = projectDBAdaptor.renameProjectAlias(projectId, newProjectAlias);
        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, new ObjectMap("alias", newProjectAlias), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Project> delete(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
