/*
 * Copyright 2015-2016 OpenCB
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

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IProjectManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.ParamUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
                          Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
    }

    @Override
    public String getUserId(long projectId) throws CatalogException {
        return projectDBAdaptor.getOwnerId(projectId);
    }

    @Override
    public long getId(String userId, String projectStr) throws CatalogException {
        if (StringUtils.isNumeric(projectStr)) {
            long projectId = Long.parseLong(projectStr);
            if (projectId > configuration.getCatalog().getOffset()) {
                projectDBAdaptor.checkId(projectId);
                return projectId;
            }
        }

        String userOwner;
        String projectAlias;

        if (StringUtils.isBlank(projectStr)) {
            userOwner = userId;
            projectAlias = null;
        } else {
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
        }

        if (!userOwner.equals("anonymous") && StringUtils.isNotBlank(projectAlias)) {
            return projectDBAdaptor.getId(userOwner, projectAlias);
        } else {
            // Anonymous user
            Query query = new Query();
            if (StringUtils.isNotBlank(projectAlias)) {
                query.put(ProjectDBAdaptor.QueryParams.ALIAS.key(), projectAlias);
            }
            if (!userOwner.equals("anonymous")) {
                query.put(ProjectDBAdaptor.QueryParams.USER_ID.key(), userOwner);
            }
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key());
            QueryResult<Project> projectQueryResult = projectDBAdaptor.get(query, options);

            if (projectQueryResult.getNumResults() != 1) {
                if (projectQueryResult.getNumResults() == 0) {
                    throw new CatalogException("No projects found with alias " + projectAlias);
                } else {
                    throw new CatalogException("More than one project found with alias " + projectAlias);
                }
            }

            return projectQueryResult.first().getId();
        }
    }

    @Override
    public List<Long> getIds(String userId, String projectStr) throws CatalogException {
        if (StringUtils.isNumeric(projectStr)) {
            return Arrays.asList(Long.parseLong(projectStr));
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

        if (!userOwner.equals("anonymous")) {
            return Arrays.asList(projectDBAdaptor.getId(userOwner, projectAlias));
        } else {
            // Anonymous user
            Query query = new Query(ProjectDBAdaptor.QueryParams.ALIAS.key(), projectAlias);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key());
            QueryResult<Project> projectQueryResult = projectDBAdaptor.get(query, options);

            if (projectQueryResult.getNumResults() == 0) {
                throw new CatalogException("No projects found with alias " + projectAlias);
            }

            return projectQueryResult.getResult().stream().map(project -> project.getId()).collect(Collectors.toList());
        }
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
    public QueryResult<Project> create(String name, String alias, String description, String organization, String scientificName,
                                       String commonName, String taxonomyCode, String assembly, QueryOptions options, String sessionId)
            throws CatalogException {

        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkParameter(scientificName, "organism.scientificName");
        ParamUtils.checkParameter(assembly, "organism.assembly");
        ParamUtils.checkAlias(alias, "alias", configuration.getCatalog().getOffset());
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

        // Organism
        Project.Organism organism = new Project.Organism(scientificName, assembly);
        if (StringUtils.isNumeric(taxonomyCode)) {
            organism.setTaxonomyCode(Integer.parseInt(taxonomyCode));
        }
        if (StringUtils.isNotEmpty(commonName)) {
            organism.setCommonName(assembly);
        }

        Project project = new Project(name, alias, description, new Status(), organization, organism, 1);

        QueryResult<Project> queryResult = projectDBAdaptor.insert(project, userId, options);
        project = queryResult.getResult().get(0);

        try {
            catalogIOManagerFactory.getDefault().createProject(userId, Long.toString(project.getId()));
        } catch (CatalogIOException e) {
            try {
                projectDBAdaptor.delete(project.getId());
            } catch (Exception e1) {
                logger.error("Error deleting project from catalog after failing creating the folder in the filesystem", e1);
                throw e;
            }
            throw e;
        }
        userDBAdaptor.updateUserLastModified(userId);
//        auditManager.recordCreation(AuditRecord.Resource.project, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.project, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Project> get(Long projectId, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

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
        String userId = catalogManager.getUserManager().getId(sessionId);

        String ownerId = query.getString("ownerId", query.getString("userId", userId));

        ParamUtils.checkParameter(ownerId, "ownerId");

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

        QueryResult<Project> queryResult = new QueryResult<>();
        if (parameters.containsKey("alias")) {
            rename(projectId, parameters.getString("alias"), sessionId);

            //Clone and remove alias from parameters. Do not modify the original parameter
            parameters = new ObjectMap(parameters);
            parameters.remove("alias");
        }

        // Update organism information only if any of the fields was not properly defined
        if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key())
                || parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key())
                || parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key())
                || parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key())) {
            QueryResult<Project> projectQR = projectDBAdaptor
                    .get(projectId, new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()));
            if (projectQR.getNumResults() == 0) {
                throw new CatalogException("Project " + projectId + " not found");
            }
            ObjectMap objectMap = new ObjectMap();
            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key())
                    && StringUtils.isEmpty(projectQR.first().getOrganism().getScientificName())) {
                objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key(),
                        parameters.getString(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key()));
                parameters.remove(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key());
            }
            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key())
                    && StringUtils.isEmpty(projectQR.first().getOrganism().getCommonName())) {
                objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(),
                        parameters.getString(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key()));
                parameters.remove(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key());
            }
            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key())
                    && projectQR.first().getOrganism().getTaxonomyCode() <= 0) {
                objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key(),
                        parameters.getInt(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key()));
                parameters.remove(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key());
            }
            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key())
                    && StringUtils.isEmpty(projectQR.first().getOrganism().getAssembly())) {
                objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(),
                        parameters.getString(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key()));
                parameters.remove(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key());
            }
            if (!objectMap.isEmpty()) {
                queryResult = projectDBAdaptor.update(projectId, objectMap);
            } else {
                throw new CatalogException("Cannot update organism information that is already filled in");
            }
        }

        for (String s : parameters.keySet()) {
            if (!s.matches("name|description|organization|attributes")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }
        userDBAdaptor.updateUserLastModified(ownerId);
        if (parameters.size() > 0) {
            queryResult = projectDBAdaptor.update(projectId, parameters);
        }
        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, parameters, null, null);
        return queryResult;
    }

    public QueryResult rename(long projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(newProjectAlias, "newProjectAlias", configuration.getCatalog().getOffset());
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
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        throw new NotImplementedException("Project: Operation not yet supported");
//        ParamUtils.checkParameter(sessionId, "sessionId");
//        String userId = catalogManager.getUserManager().getId(sessionId);
//        long projectId = getId(userId, id);
//        String ownerId = projectDBAdaptor.getOwnerId(projectId);
//
//        if (!userId.equals(ownerId)) {
//            throw new CatalogException("Permission denied: Only the owner of the project can update the status.");
//        }
//
//        if (!Status.isValid(status)) {
//            throw new CatalogException("The status " + status + " is not valid project status.");
//        }
//
//        ObjectMap param = new ObjectMap(ProjectDBAdaptor.QueryParams.STATUS_NAME.key(), status);
//        projectDBAdaptor.update(projectId, param);
//        userDBAdaptor.updateUserLastModified(ownerId);
//        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, param, null, null);
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

    @Override
    public QueryResult<Project> getSharedProjects(String userId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        queryOptions = ParamUtils.defaultObject(queryOptions, QueryOptions::new);
        long startTime = System.currentTimeMillis();

        String userSessionId = catalogManager.getUserManager().getId(sessionId);
        if (!userSessionId.equals(userId)) {
            throw new CatalogException("Invalid session id: The user corresponding to the session provided is not " + userId);
        }

        // Search all studies shared with the user
        // 1. Look for userId in a group in all the studies.
        Query query = new Query(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), userId);
        QueryResult<Study> studyGroupQR = catalogManager.getStudyManager().get(query, queryOptions, sessionId);
        // The studies obtained are already filtered in studyManager, so if we get them is because those have been shared with the user

        // 2. Look for userId in an ACL in all the studies.
        query = new Query(StudyDBAdaptor.QueryParams.ACL_MEMBER.key(), userId);
        QueryResult<Study> studyACLQR = catalogManager.getStudyManager().get(query, queryOptions, sessionId);

        List<Study> studyList = new ArrayList<>();
        studyList.addAll(studyGroupQR.getResult());
        studyList.addAll(studyACLQR.getResult());

        if (studyList.size() == 0) {
            // No studies are shared with userId
            return new QueryResult<>(userId, (int) (System.currentTimeMillis() - startTime), 0, 0, "", "", Collections.emptyList());
        }

        // Obtain the projects corresponding to each study
        List<Long> projectIds = new LinkedList<>();
        Map<Long, List<Study>> projectStudyMap = new LinkedMap();
        for (Study study : studyList) {
            Long projectId = catalogManager.getStudyManager().getProjectId(study.getId());
            if (!projectStudyMap.containsKey(projectId)) {
                projectStudyMap.put(projectId, new LinkedList<>());
                projectIds.add(projectId);
            }
            projectStudyMap.get(projectId).add(study);
        }

        // Obtain the project info of all the project ids needed
        query = new Query(ProjectDBAdaptor.QueryParams.ID.key(), projectIds);
        QueryOptions options = new QueryOptions(queryOptions); // Copy of queryOptions
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            excludeList.add("projects.studies");
            options.put(QueryOptions.EXCLUDE, excludeList);
        } else {
            options.add(QueryOptions.EXCLUDE, "projects.studies");
        }

        QueryResult<Project> projectQueryResult = projectDBAdaptor.get(query, options);
        for (Project project : projectQueryResult.getResult()) {
            // Update with the studies shared with the user
            project.setStudies(projectStudyMap.get(project.getId()));

            // Add user info to the alias
            String ownerId = projectDBAdaptor.getOwnerId(project.getId());
            project.setAlias(ownerId + "@" + project.getAlias());
        }

        authorizationManager.filterProjects(userSessionId, projectQueryResult.getResult());

        projectQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        projectQueryResult.setId(userId);

        return projectQueryResult;
    }
}
