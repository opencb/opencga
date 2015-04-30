package org.opencb.opencga.catalog;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.api.IProjectManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.Project;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;

import java.util.List;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class ProjectManager implements IProjectManager{
    protected final AuthorizationManager authorizationManager;
    protected final CatalogIOManager ioManager;
    protected final CatalogUserDBAdaptor userDBAdaptor;
    protected final CatalogStudyDBAdaptor studyDBAdaptor;
    protected final CatalogFileDBAdaptor fileDBAdaptor;
    protected final CatalogSamplesDBAdaptor sampleDBAdaptor;
    protected final CatalogJobDBAdaptor jobDBAdaptor;

    public ProjectManager(AuthorizationManager authorizationManager, CatalogDBAdaptor catalogDBAdaptor,
                          CatalogIOManager ioManager) {
        this.authorizationManager = authorizationManager;
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptor.getCatalogSamplesDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        this.ioManager = ioManager;
    }

    @Override
    public String getUserId(int projectId) throws CatalogException {
        return userDBAdaptor.getProjectOwnerId(projectId);
    }

    @Override
    public int getProjectId(String projectId) throws CatalogException {
        try {
            return Integer.parseInt(projectId);
        } catch (NumberFormatException ignore) {
        }

        String[] split = projectId.split("@");
        if (split.length != 2) {
            return -1;
        }
        return userDBAdaptor.getProjectId(split[0], split[1]);
    }

    @Override
    public QueryResult<Project> create(String ownerId, String name, String alias, String description,
                                       String organization, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamsUtils.checkParameter(ownerId, "ownerId");
        ParamsUtils.checkParameter(name, "name");
        ParamsUtils.checkAlias(alias, "alias");
        ParamsUtils.checkParameter(sessionId, "sessionId");

        //Only the user can create a project
        String userIdBySessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getUserRole(userIdBySessionId).equals(User.Role.ADMIN)
                && !userIdBySessionId.equals(ownerId)) {
            throw new CatalogException("Only the user \"" + ownerId + "\" can create a project with himself as owner");
        }

        description = description != null ? description : "";
        organization = organization != null ? organization : "";

        Project project = new Project(name, alias, description, "", organization);

        /* Add default ACL */
        //Add generic permissions to the project.
        project.getAcl().add(new Acl(Acl.USER_OTHERS_ID, false, false, false, false));

        QueryResult<Project> result = userDBAdaptor.createProject(ownerId, project, options);
        project = result.getResult().get(0);

        try {
            ioManager.createProject(ownerId, Integer.toString(project.getId()));
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            userDBAdaptor.deleteProject(project.getId());
        }
        userDBAdaptor.updateUserLastActivity(ownerId);
        return result;    }

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
        ParamsUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        Acl projectAcl = authorizationManager.getProjectACL(userId, projectId);
        if (projectAcl.isRead()) {
            QueryResult<Project> projectResult = userDBAdaptor.getProject(projectId, options);
            if (!projectResult.getResult().isEmpty()) {
                authorizationManager.filterStudies(userId, projectAcl, projectResult.getResult().get(0).getStudies());
            }
            return projectResult;
        } else {
            throw new CatalogDBException("Permission denied. Can't read project.");
        }
    }

    @Override
    public QueryResult<Project> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = query.getString("ownerId", query.getString("userId", userId));

        ParamsUtils.checkParameter(ownerId, "ownerId");
        ParamsUtils.checkParameter(sessionId, "sessionId");

        QueryResult<Project> allProjects = userDBAdaptor.getAllProjects(ownerId, options);

        List<Project> projects = allProjects.getResult();
        authorizationManager.filterProjects(userId, projects);
        allProjects.setResult(projects);

        return allProjects;
    }

    @Override
    public QueryResult<Project> update(Integer projectId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamsUtils.checkObj(parameters, "Parameters");
        ParamsUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);
        if (!authorizationManager.getProjectACL(userId, projectId).isWrite()) {
            throw new CatalogDBException("User '" + userId + "' can't modify the project " + projectId);
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
        userDBAdaptor.updateUserLastActivity(ownerId);
        return userDBAdaptor.modifyProject(projectId, parameters);
    }

    public QueryResult rename(int projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        ParamsUtils.checkAlias(newProjectAlias, "newProjectAlias");
        ParamsUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);

        Acl projectAcl = authorizationManager.getProjectACL(userId, projectId);
        if (projectAcl.isWrite()) {
            userDBAdaptor.updateUserLastActivity(ownerId);
            return userDBAdaptor.renameProjectAlias(projectId, newProjectAlias);
        } else {
            throw new CatalogDBException("Permission denied. Can't rename project");
        }
    }

    @Override
    public QueryResult<Project> delete(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
