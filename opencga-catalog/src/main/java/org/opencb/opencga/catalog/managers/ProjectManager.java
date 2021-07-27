/*
 * Copyright 2015-2020 OpenCB
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.project.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectManager extends AbstractManager {

    private final CatalogIOManager catalogIOManager;

    private static final Set<String> UPDATABLE_FIELDS = new HashSet<>(Arrays.asList(
            ProjectDBAdaptor.QueryParams.ID.key(),
            ProjectDBAdaptor.QueryParams.NAME.key(),
            ProjectDBAdaptor.QueryParams.DESCRIPTION.key(),
            ProjectDBAdaptor.QueryParams.CREATION_DATE.key(),
            ProjectDBAdaptor.QueryParams.ORGANIZATION.key(),
            ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key(),
            ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(),
            ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(),
            ProjectDBAdaptor.QueryParams.ATTRIBUTES.key()
    ));
    private static final Set<String> PROTECTED_UPDATABLE_FIELDS = new HashSet<>(Arrays.asList(
            ProjectDBAdaptor.QueryParams.INTERNAL_DATASTORES_VARIANT.key(),
            ProjectDBAdaptor.QueryParams.INTERNAL_CELLBASE.key()
    ));

    ProjectManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
    }

    public String getOwner(long projectId) throws CatalogException {
        return projectDBAdaptor.getOwnerId(projectId);
    }

    /**
     * Fetch the project qualifying the projectStr structure as long as userId has permissions to see it.
     *
     * @param projectStr string that can contain the full qualified name (owner@projectId) or just the projectId.
     * @param userId     user asking for the project information.
     * @return a OpenCGAResult containing the project.
     * @throws CatalogException if multiple projects are found.
     */
    Project resolveId(String projectStr, String userId) throws CatalogException {
        if (StringUtils.isEmpty(userId)) {
            throw new CatalogException("Missing mandatory parameter userId");
        }

        String auxProject = "";
        String auxOwner = "";
        boolean isUuid = false;

        if (StringUtils.isNotEmpty(projectStr)) {
            if (UuidUtils.isOpenCgaUuid(projectStr)) {
                isUuid = true;
            } else {
                String[] split = projectStr.split("@");
                if (split.length == 1) {
                    auxProject = projectStr;
                } else if (split.length == 2) {
                    auxOwner = split[0];
                    auxProject = split[1];
                } else {
                    throw new CatalogException(projectStr + " does not follow the expected pattern [ownerId@projectId]");
                }
            }
        }

        QueryOptions projectOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(
                        ProjectDBAdaptor.QueryParams.UID.key(), ProjectDBAdaptor.QueryParams.UUID.key(),
                        ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.FQN.key(),
                        ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));

        if (StringUtils.isEmpty(auxOwner) || auxOwner.equals(userId)) {
            // We look for own projects
            Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId);
            if (isUuid) {
                query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.UUID.key(), projectStr);
            } else {
                query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ID.key(), auxProject);
            }

            OpenCGAResult<Project> projectDataResult = projectDBAdaptor.get(query, projectOptions);
            if (projectDataResult.getNumResults() > 1) {
                throw new CatalogException("Please be more concrete with the project. More than one project found for " + userId + " user");
            } else if (projectDataResult.getNumResults() == 1) {
                return projectDataResult.first();
            }
        }

        // Look for shared projects. First, we will check all the studies in which the user is a member
        Query query = new Query();
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.OWNER.key(), auxOwner);
        if (isUuid) {
            query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_UUID.key(), projectStr);
        } else {
            query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), auxProject);
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key());
        OpenCGAResult<Study> studyDataResult = studyDBAdaptor.get(query, queryOptions, userId);

        if (studyDataResult.getNumResults() > 0) {
            Set<String> projectFqnSet = new HashSet<>();
            for (Study study : studyDataResult.getResults()) {
                projectFqnSet.add(StringUtils.split(study.getFqn(), ":")[0]);
            }

            if (projectFqnSet.size() == 1) {
                query = new Query(ProjectDBAdaptor.QueryParams.FQN.key(), projectFqnSet);
                return projectDBAdaptor.get(query, projectOptions).first();
            } else {
                throw new CatalogException("More than one project shared with user " + userId + ". Please, be more specific. "
                        + "The accepted pattern is [ownerId@projectId]");
            }
        } else {
            if (StringUtils.isNotEmpty(projectStr)) {
                // Check if it is a matter of permissions
                if (studyDBAdaptor.count(query).getNumMatches() == 0) {
                    throw new CatalogException("Project " + projectStr + " not found");
                } else {
                    throw CatalogAuthorizationException.deny(userId, "view", "project", projectStr, null);
                }
            } else {
                throw new CatalogException("No projects shared or owned by user " + userId);
            }
        }
    }

    private OpenCGAResult<Project> getProject(String userId, String projectUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId)
                .append(ProjectDBAdaptor.QueryParams.UUID.key(), projectUuid);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        return projectDBAdaptor.get(query, options);
    }

    /**
     * Obtain the list of projects and studies that are shared with the user.
     *
     * @param userId       user whose projects and studies are being shared with.
     * @param queryOptions QueryOptions object.
     * @param sessionId    Session id which should correspond to userId.
     * @return A OpenCGAResult object containing the list of projects and studies that are shared with the user.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Project> getSharedProjects(String userId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        OpenCGAResult<Project> result = get(new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "!=" + userId), queryOptions, sessionId);
        for (Event event : result.getEvents()) {
            if (event.getType() == Event.Type.ERROR) {
                throw new CatalogAuthorizationException(event.getMessage());
            }
        }
        return result;
    }

    @Deprecated
    public OpenCGAResult<Project> create(String id, String name, String description, String scientificName, String commonName,
                                         String assembly, QueryOptions options, String sessionId) throws CatalogException {
        ProjectCreateParams projectCreateParams = new ProjectCreateParams(id, name, description, null,
                new ProjectOrganism(scientificName, commonName, assembly), null);
        return create(projectCreateParams, options, sessionId);
    }

    public OpenCGAResult<Project> create(ProjectCreateParams projectCreateParams, QueryOptions options, String token)
            throws CatalogException {
        //Only the user can create a project
        String userId = this.catalogManager.getUserManager().getUserId(token);
        if (userId.isEmpty()) {
            throw new CatalogException("The token introduced does not correspond to any registered user.");
        }

        ObjectMap auditParams = new ObjectMap()
                .append("project", projectCreateParams)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Project> queryResult;
        Project project;
        try {
            ParamUtils.checkObj(projectCreateParams, "ProjectCreateParams");

            // Check that the account type is not guest
            OpenCGAResult<User> user = userDBAdaptor.get(userId, QueryOptions.empty());
            if (user.getNumResults() == 0) {
                throw new CatalogException("Internal error happened. Could not find user " + userId);
            }

            if (Account.AccountType.FULL != user.first().getAccount().getType()) {
                if (user.first().getAccount().getType() == Account.AccountType.ADMINISTRATOR) {
                    // Check it is the first project
                    if (user.first().getProjects() != null && !user.first().getProjects().isEmpty()) {
                        String errorMsg = "Cannot create more projects for ADMINISTRATOR user '" + user.first().getId() + "'.";
                        auditManager.auditCreate(userId, Enums.Resource.PROJECT, projectCreateParams.getId(), "", "", "", auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", errorMsg)));
                        throw new CatalogException(errorMsg);
                    }
                } else {

                    String errorMsg = "User " + userId + " is not authorized to create new projects. Only users with "
                            + Account.AccountType.FULL + " accounts are allowed to do so.";
                    auditManager.auditCreate(userId, Enums.Resource.PROJECT, projectCreateParams.getId(), "", "", "", auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", errorMsg)));
                    throw new CatalogException(errorMsg);
                }
            }

            project = projectCreateParams.toProject();
            validateProjectForCreation(project, user.first());

            projectDBAdaptor.insert(project, userId, options);
            queryResult = getProject(userId, project.getUuid(), options);
            project = queryResult.first();
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.PROJECT, projectCreateParams.getId(), "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            catalogIOManager.createProject(userId, Long.toString(project.getUid()));
        } catch (CatalogIOException e) {
            auditManager.auditCreate(userId, Enums.Resource.PROJECT, project.getId(), "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            try {
                projectDBAdaptor.delete(project);
            } catch (Exception e1) {
                logger.error("Error deleting project from catalog after failing creating the folder in the filesystem", e1);
                throw e;
            }
            throw e;
        }
        auditManager.auditCreate(userId, Enums.Resource.PROJECT, project.getId(), project.getUuid(), "", "", auditParams,
                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

        return queryResult;
    }

    private void validateProjectForCreation(Project project, User user) throws CatalogParameterException {
        ParamUtils.checkParameter(project.getId(), ProjectDBAdaptor.QueryParams.ID.key());
        project.setName(ParamUtils.defaultString(project.getName(), project.getId()));
        project.setDescription(ParamUtils.defaultString(project.getDescription(), ""));
        project.setCreationDate(ParamUtils.checkCreationDateOrGetCurrentCreationDate(project.getCreationDate()));
        project.setModificationDate(TimeUtils.getTime());
        project.setCurrentRelease(1);
        project.setInternal(ProjectInternal.init());
        project.setAttributes(ParamUtils.defaultObject(project.getAttributes(), HashMap::new));

        if (user.getAccount().getType() != Account.AccountType.ADMINISTRATOR
                && (project.getOrganism() == null || StringUtils.isEmpty(project.getOrganism().getAssembly())
                || StringUtils.isEmpty(project.getOrganism().getScientificName()))) {
            throw new CatalogParameterException("Missing mandatory organism information");
        }

        project.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PROJECT));

        if (project.getStudies() != null && !project.getStudies().isEmpty()) {
            throw new CatalogParameterException("Creating project and studies in a single transaction is forbidden");
        }
    }

    /**
     * Reads a project from Catalog given a project id or alias.
     *
     * @param projectId Project id or alias.
     * @param options    Read options
     * @param token  sessionId
     * @return The specified object
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Project> get(String projectId, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("projectId", projectId)
                .append("options", options)
                .append("token", token);
        try {
            Project project = resolveId(projectId, userId);
            OpenCGAResult<Project> queryResult = projectDBAdaptor.get(project.getUid(), options);
            auditManager.auditInfo(userId, Enums.Resource.PROJECT, project.getId(), project.getUuid(), "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditInfo(userId, Enums.Resource.PROJECT, projectId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Project> get(List<String> projectList, QueryOptions options, boolean ignoreException, String sessionId)
            throws CatalogException {
        OpenCGAResult<Project> result = OpenCGAResult.empty();

        for (int i = 0; i < projectList.size(); i++) {
            String project = projectList.get(i);
            try {
                OpenCGAResult<Project> projectResult = get(project, options, sessionId);
                result.append(projectResult);
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, projectList.get(i), e.getMessage());
                String warning = "Missing " + projectList.get(i) + ": " + e.getMessage();
                if (ignoreException) {
                    logger.error(warning, e);
                    result.getEvents().add(event);
                } else {
                    logger.error(warning);
                    throw e;
                }
            }
        }
        return result;
    }

    /**
     * Fetch all the project objects matching the query.
     *
     * @param query     Query to catalog.
     * @param options   Query options, like "include", "exclude", "limit" and "skip"
     * @param token sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Project> get(Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = catalogManager.getUserManager().getUserId(token);
        query = new Query(query);

        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);

        try {
            fixQueryObject(query);
            // If study is provided, we need to check if it will be study alias or id
            if (StringUtils.isNotEmpty(query.getString(ProjectDBAdaptor.QueryParams.STUDY.key()))) {
                List<Study> studies = catalogManager.getStudyManager()
                        .resolveIds(query.getAsStringList(ProjectDBAdaptor.QueryParams.STUDY.key()), userId);
                query.remove(ProjectDBAdaptor.QueryParams.STUDY.key());
                query.put(ProjectDBAdaptor.QueryParams.STUDY_UID.key(), studies.stream().map(Study::getUid).collect(Collectors.toList()));
            }

            OpenCGAResult<Project> queryResult = projectDBAdaptor.get(query, options, userId);
            auditManager.auditSearch(userId, Enums.Resource.PROJECT, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.PROJECT, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));

            throw e;
        }
    }

    /**
     * Update metada from projects.
     *
     * @param projectId Project id or alias.
     * @param parameters Parameters to change.
     * @param options    options
     * @param token  sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Project> update(String projectId, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        return update(projectId, parameters, options, false, token);
    }

    /**
     * Update metada from projects.
     *
     * @param projectId Project id or alias.
     * @param parameters Parameters to change.
     * @param options    options
     * @param allowProtectedUpdates  Allow protected updates
     * @param token  sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    private OpenCGAResult<Project> update(String projectId, ObjectMap parameters, QueryOptions options, boolean allowProtectedUpdates,
                                          String token)
            throws CatalogException {
        String userId = this.catalogManager.getUserManager().getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("project", projectId)
                .append("updateParams", parameters)
                .append("options", options)
                .append("token", token);

        Project project;
        try {
            project = resolveId(projectId, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.PROJECT, projectId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            ParamUtils.checkObj(parameters, "Parameters");
            ParamUtils.checkParameter(token, "token");
            long projectUid = project.getUid();
            authorizationManager.checkCanEditProject(projectUid, userId);

            for (String s : parameters.keySet()) {
                if (UPDATABLE_FIELDS.contains(s)) {
                    continue;
                } else if (allowProtectedUpdates && PROTECTED_UPDATABLE_FIELDS.contains(s)) {
                    logger.info("Updating protected field '{}' from project '{}'", s, project.getFqn());
                } else {
                    throw new CatalogDBException("Parameter '" + s + "' can't be changed");
                }
            }

            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.CREATION_DATE.key())) {
                // Validate creationDate format
                String creationDate = parameters.getString(ProjectDBAdaptor.QueryParams.CREATION_DATE.key());
                ParamUtils.checkCreationDateFormat(creationDate);
            }

            // Update organism information only if any of the fields was not properly defined
            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key())
                    || parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key())
                    || parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key())) {
                OpenCGAResult<Project> projectQR = projectDBAdaptor
                        .get(projectUid, new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()));
                if (projectQR.getNumResults() == 0) {
                    throw new CatalogException("Project " + projectUid + " not found");
                }
                boolean canBeUpdated = false;
                if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key())
                        && StringUtils.isEmpty(projectQR.first().getOrganism().getScientificName())) {
                    canBeUpdated = true;
                }
                if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key())
                        && StringUtils.isEmpty(projectQR.first().getOrganism().getCommonName())) {
                    canBeUpdated = true;
                }
                if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key())
                        && StringUtils.isEmpty(projectQR.first().getOrganism().getAssembly())) {
                    canBeUpdated = true;
                }
                if (!canBeUpdated) {
                    throw new CatalogException("Cannot update organism information that is already filled in");
                }
            }

            if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ID.key())) {
                ParamUtils.checkIdentifier(parameters.getString(ProjectDBAdaptor.QueryParams.ID.key()), "id");
            }

            OpenCGAResult result = projectDBAdaptor.update(projectUid, parameters, QueryOptions.empty());
            auditManager.auditUpdate(userId, Enums.Resource.PROJECT, project.getId(), project.getUuid(), "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            OpenCGAResult<Project> queryResult = projectDBAdaptor.get(projectUid,
                    new QueryOptions(QueryOptions.INCLUDE, parameters.keySet()));
            queryResult.setTime(queryResult.getTime() + result.getTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.PROJECT, project.getId(), project.getUuid(), "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Project> setDatastoreVariant(String projectStr, DataStore dataStore, String token) throws CatalogException {
        return update(projectStr,
                new ObjectMap(ProjectDBAdaptor.QueryParams.INTERNAL_DATASTORES_VARIANT.key(), dataStore), new QueryOptions(), true, token);
    }

    public OpenCGAResult<Project> setInternalCellbaseConfiguration(String projectStr, CellBaseConfiguration configuration, String token)
            throws CatalogException {
        return update(projectStr,
                new ObjectMap(ProjectDBAdaptor.QueryParams.INTERNAL_CELLBASE.key(), configuration), new QueryOptions(), true, token);
    }

    public Map<String, Object> facet(String projectStr, String fileFields, String sampleFields, String individualFields,
                                     String cohortFields, String familyFields, String jobFields, boolean defaultStats, String sessionId)
            throws CatalogException, IOException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Project project = resolveId(projectStr, userId);
        Query query = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
        OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().search(query, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.FQN.key(), StudyDBAdaptor.QueryParams.ID.key())), sessionId);

        Map<String, Object> result = new HashMap<>();
        for (Study study : studyDataResult.getResults()) {
            result.put(study.getId(), catalogManager.getStudyManager().facet(study.getFqn(), fileFields, sampleFields, individualFields,
                    cohortFields, familyFields, jobFields, defaultStats, sessionId));
        }

        return result;
    }

    public OpenCGAResult<Integer> incrementRelease(String projectStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);

        try {
            Project project = resolveId(projectStr, userId);
            long projectUid = project.getUid();

            authorizationManager.checkCanEditProject(projectUid, userId);

            // Obtain the current release number
            int currentRelease = project.getCurrentRelease();

            // Check current release has been used at least in one study or file or cohort or individual...
            List<Study> allStudiesInProject = project.getStudies();
            if (allStudiesInProject.isEmpty()) {
                throw new CatalogException("Cannot increment current release number. No studies found for release " + currentRelease);
            }

            if (checkCurrentReleaseInUse(allStudiesInProject, currentRelease)) {
                // Increment current project release
                OpenCGAResult writeResult = projectDBAdaptor.incrementCurrentRelease(projectUid);
                OpenCGAResult<Project> projectDataResult = projectDBAdaptor.get(projectUid,
                        new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));
                OpenCGAResult<Integer> queryResult = new OpenCGAResult<>(projectDataResult.getTime() + writeResult.getTime(),
                        Collections.emptyList(), 1, Collections.singletonList(projectDataResult.first().getCurrentRelease()), 1);

                // Upgrade release in every versioned data model
                for (Study study : allStudiesInProject) {
                    sampleDBAdaptor.updateProjectRelease(study.getUid(), queryResult.first());
                    individualDBAdaptor.updateProjectRelease(study.getUid(), queryResult.first());
                    familyDBAdaptor.updateProjectRelease(study.getUid(), queryResult.first());
                    panelDBAdaptor.updateProjectRelease(study.getUid(), queryResult.first());
                    interpretationDBAdaptor.updateProjectRelease(study.getUid(), queryResult.first());
                }

                auditManager.audit(userId, Enums.Action.INCREMENT_PROJECT_RELEASE, Enums.Resource.PROJECT, project.getId(),
                        project.getUuid(), "", "", new ObjectMap(), new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return queryResult;
            } else {
                throw new CatalogException("Cannot increment current release number. The current release " + currentRelease
                        + " has not yet been used in any entry");
            }
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.INCREMENT_PROJECT_RELEASE, Enums.Resource.PROJECT, projectStr, "", "", "",
                    new ObjectMap(), new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public void importReleases(String owner, String inputDirStr, String sessionId) throws CatalogException, IOException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        if (!authorizationManager.isInstallationAdministrator(userId)) {
            throw new CatalogAuthorizationException("Only admin of OpenCGA is authorised to import data");
        }

        OpenCGAResult<User> userDataResult = userDBAdaptor.get(owner, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.ACCOUNT.key(), UserDBAdaptor.QueryParams.PROJECTS.key())));
        if (userDataResult.getNumResults() == 0) {
            throw new CatalogException("User " + owner + " not found");
        }

        Path inputDir = Paths.get(inputDirStr);
        if (inputDir == null) {
            throw new CatalogException("Missing directory containing the exported data");
        }
        if (inputDir.toFile().exists() && !inputDir.toFile().isDirectory()) {
            throw new CatalogException("The output directory parameter seems not to be directory");
        }

        List<String> fileNames = Arrays.asList("projects.json", "studies.json", "samples.json", "individuals.json", "families.json",
                "files.json", "clinical_analysis.json", "cohorts.json", "jobs.json");
        for (String fileName : fileNames) {
            if (!inputDir.resolve(fileName).toFile().exists()) {
                throw new CatalogException(fileName + " file not found");
            }
        }

        ObjectMapper objectMapper = getDefaultObjectMapper();

        // Reading project
        Map<String, Object> project = (Map<String, Object>) objectMapper.readValue(inputDir.resolve("projects.json").toFile(), Map.class)
                .get("projects");
        project.put(ProjectDBAdaptor.QueryParams.UID.key(), ParamUtils.getAsLong(project.get(ProjectDBAdaptor.QueryParams.UID.key())));

        // Check the projectId
        if (projectDBAdaptor.exists((Long) project.get(ProjectDBAdaptor.QueryParams.UID.key()))) {
            throw new CatalogException("The database is not empty. Project " + project.get(ProjectDBAdaptor.QueryParams.NAME.key())
                    + " already exists");
        }
        logger.info("Importing projects...");
        projectDBAdaptor.nativeInsert(project, owner);

        // Reading studies
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("studies.json").toFile()))) {
            logger.info("Importing studies...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> study = objectMapper.readValue(line, Map.class);
                studyDBAdaptor.nativeInsert(study, owner);
            }
        }

        // Reading files
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("files.json").toFile()))) {
            logger.info("Importing files...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> file = objectMapper.readValue(line, Map.class);
                fileDBAdaptor.nativeInsert(file, owner);
            }
        }

        // Reading samples
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("samples.json").toFile()))) {
            logger.info("Importing samples...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> sample = objectMapper.readValue(line, Map.class);
                sampleDBAdaptor.nativeInsert(sample, owner);
            }
        }

        // Reading individuals
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("individuals.json").toFile()))) {
            logger.info("Importing individuals...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> file = objectMapper.readValue(line, Map.class);
                individualDBAdaptor.nativeInsert(file, owner);
            }
        }

        // Reading families
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("families.json").toFile()))) {
            logger.info("Importing families...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> file = objectMapper.readValue(line, Map.class);
                familyDBAdaptor.nativeInsert(file, owner);
            }
        }

        // Reading search analysis
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("clinical_analysis.json").toFile()))) {
            logger.info("Importing search analysis...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> file = objectMapper.readValue(line, Map.class);
                clinicalDBAdaptor.nativeInsert(file, owner);
            }
        }

        // Reading cohorts
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("cohorts.json").toFile()))) {
            logger.info("Importing cohorts...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> file = objectMapper.readValue(line, Map.class);
                cohortDBAdaptor.nativeInsert(file, owner);
            }
        }

        // Reading jobs
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("jobs.json").toFile()))) {
            logger.info("Importing jobs...");
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, Object> file = objectMapper.readValue(line, Map.class);
                jobDBAdaptor.nativeInsert(file, owner);
            }
        }
    }

    public void exportByFileNames(String studyStr, File outputDir, File filePath, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        if (!authorizationManager.isInstallationAdministrator(userId)) {
            throw new CatalogAuthorizationException("Only admin of OpenCGA is authorised to export data");
        }

        if (!filePath.exists() || !filePath.isFile()) {
            throw new CatalogException(filePath + " is not a valid file containing the file ids");
        }

        if (outputDir == null) {
            throw new CatalogException("Missing output directory");
        }
        if (!outputDir.isDirectory()) {
            throw new CatalogException("The output directory parameter seems not to contain a directory");
        }

        ObjectMapper objectMapper = new ObjectMapper();

        // We obtain the owner of the study
        OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().get(studyStr,
                new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.FQN.key(),
                                StudyDBAdaptor.QueryParams.VARIABLE_SET.key())), token);

        // Export the list of variable sets
        List<Object> variableSetList = studyDataResult.first().getVariableSets().stream().collect(Collectors.toList());
        exportToFile(variableSetList, outputDir.toPath().resolve("variablesets.json").toFile(), objectMapper);

        String owner = studyDataResult.first().getFqn().split("@")[0];

        String ownerToken = catalogManager.getUserManager().getNonExpiringToken(owner, token);

        try (BufferedReader buf = new BufferedReader(new FileReader(filePath))) {

            while (true) {
                String vcfFile = buf.readLine();
                if (vcfFile != null) {

                    List fileList = new ArrayList<>();
                    List sampleList = null;
                    List individualList = new ArrayList<>();
                    List cohortList = null;

                    Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), "file://" + vcfFile);
                    OpenCGAResult<org.opencb.opencga.core.models.file.File> fileDataResult = catalogManager.getFileManager()
                            .search(studyStr, query, QueryOptions.empty(), ownerToken);
                    if (fileDataResult.getNumResults() == 0) {
                        logger.error("File " + vcfFile + " not found. Skipping...");
                        continue;
                    }
                    // Add file information
                    fileList.add(fileDataResult.first());

                    List<String> sampleIds = fileDataResult.first().getSampleIds();
                    if (ListUtils.isNotEmpty(sampleIds)) {

                        // Look for the BAM and BIGWIG files associated to the samples (if any)
                        query = new Query()
                                .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleIds)
                                .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(
                                        org.opencb.opencga.core.models.file.File.Format.BAM,
                                        org.opencb.opencga.core.models.file.File.Format.BAI,
                                        // TODO: I think I will need to perform the query some other way to capture bigwigs...
                                        org.opencb.opencga.core.models.file.File.Format.BIGWIG));

                        OpenCGAResult<org.opencb.opencga.core.models.file.File> otherFiles = catalogManager.getFileManager()
                                .search(studyStr, query, QueryOptions.empty(), ownerToken);
                        if (otherFiles.getNumResults() > 0) {
                            fileList.addAll(otherFiles.getResults());
                        }

                        // Look for the whole sample information
                        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);
                        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager()
                                .search(studyStr, query, QueryOptions.empty(), ownerToken);
                        if (sampleDataResult.getNumResults() == 0 || sampleDataResult.getNumResults() != sampleIds.size()) {
                            logger.error("Unexpected error when looking for whole sample information. Could only find {} results. "
                                    + "Samples ids {}", sampleDataResult.getNumResults(), sampleIds);
                            continue;
                        }

                        sampleList = sampleDataResult.getResults();


                        // Get the list of individuals
                        // Look for the whole sample information
                        query = new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleIds);
                        OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager()
                                .search(studyStr, query, QueryOptions.empty(), ownerToken);

//                        for (Individual individual : individualDataResult.getResults()) {
//                            OpenCGAResult<ObjectMap> annotationSetAsMap = catalogManager.getIndividualManager()
//                                    .getAnnotationSetAsMap(String.valueOf(individual.getId()), studyStr, null, ownerToken);
//                            // We store the annotationsets as map in the attributes field to avoid issues
//                            individual.getAttributes().put("_annotationSets", annotationSetAsMap.getResults());
//                        }
                        individualList = individualDataResult.getResults();


                        if (individualDataResult.getNumResults() == 0) {
                            logger.info("No individuals found for samples '{}'", sampleIds);
                        }

                        // Look for the cohorts
                        query = new Query()
                                .append(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleIds)
                                .append(CohortDBAdaptor.QueryParams.ID.key(), "!=ALL");
                        OpenCGAResult<Cohort> cohortDataResult = catalogManager.getCohortManager()
                                .search(studyStr, query, QueryOptions.empty(), ownerToken);

//                        for (Cohort cohort : cohortDataResult.getResults()) {
//                            OpenCGAResult<ObjectMap> annotationSetAsMap = catalogManager.getCohortManager()
//                                    .getAnnotationSetAsMap(String.valueOf(cohort.getId()), studyStr, null, ownerToken);
//                            // We store the annotationsets as map in the attributes field to avoid issues
//                            cohort.getAttributes().put("_annotationSets", annotationSetAsMap.getResults());
//                        }
                        cohortList = cohortDataResult.getResults();

                        if (cohortDataResult.getNumResults() == 0) {
                            logger.info("No cohorts found for samples {}", sampleIds);
                        } else {
                            cohortList = cohortDataResult.getResults();
                        }
                    }

                    // Create a directory where we will store all the information to be exported
                    Path exportDir = outputDir.toPath().resolve(fileDataResult.first().getName());
                    if (Files.exists(exportDir)) {
                        logger.warn("Replicated file found: {}", fileDataResult.first().getName());

                        int count = 1;
                        exportDir = outputDir.toPath().resolve(fileDataResult.first().getName() + count);
                        while (Files.exists(exportDir)) {
                            exportDir = outputDir.toPath().resolve(fileDataResult.first().getName() + count++);
                        }
                    }
                    Files.createDirectory(exportDir);

                    logger.info("Exporting data from " + fileDataResult.first().getName());
                    exportToFile(fileList, exportDir.resolve("file.json").toFile(), objectMapper);
                    exportToFile(sampleList, exportDir.resolve("sample.json").toFile(), objectMapper);
                    exportToFile(individualList, exportDir.resolve("individual.json").toFile(), objectMapper);
                    exportToFile(cohortList, exportDir.resolve("cohort.json").toFile(), objectMapper);

                } else {
                    break;
                }
            }

        } catch (IOException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    private void exportToFile(List<Object> dataList, File file, ObjectMapper objectMapper) throws CatalogException {
        if (ListUtils.isEmpty(dataList)) {
            return;
        }

        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(file);
        } catch (IOException e) {
            logger.error("Error creating fileWriter: {}", e.getMessage(), e);
            throw new CatalogException("Error creating fileWriter: " + e.getMessage());
        }

        for (Object object : dataList) {
            try {
                fileWriter.write(objectMapper.writeValueAsString(object));
                fileWriter.write("\n");
            } catch (IOException e) {
                logger.error("Error writing to file: {}", e.getMessage(), e);
                throw new CatalogException("Error writing to file: " + e.getMessage());
            }
        }
        try {
            fileWriter.close();
        } catch (IOException e) {
            logger.error("Error closing FileWriter: {}", e.getMessage(), e);
        }
    }

    public void exportReleases(String projectStr, int release, String outputDirStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        if (!authorizationManager.isInstallationAdministrator(userId)) {
            throw new CatalogAuthorizationException("Only admin of OpenCGA is authorised to export data");
        }

        Path outputDir = Paths.get(outputDirStr);
        if (outputDir == null) {
            throw new CatalogException("Missing output directory");
        }
        if (outputDir.toFile().exists() && !outputDir.toFile().isDirectory()) {
            throw new CatalogException("The output directory parameter seems not to contain a directory");
        }

        if (!outputDir.toFile().exists()) {
            try {
                Files.createDirectory(outputDir);
            } catch (IOException e) {
                logger.error("Error when attempting to create directory for exported data: {}", e.getMessage(), e);
                throw new CatalogException("Error when attempting to create directory for exported data: " + e.getMessage());
            }
        }

        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(
                        ProjectDBAdaptor.QueryParams.UID.key(),
                        ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()
                ))
                .append(QueryOptions.EXCLUDE, "studies");
        Project project = resolveId(projectStr, userId);

        long projectId = project.getUid();
        int currentRelease = project.getCurrentRelease();

        release = Math.min(currentRelease, release);

        ObjectMapper objectMapper = getDefaultObjectMapper();

        Query query = new Query(ProjectDBAdaptor.QueryParams.UID.key(), projectId);
        DBIterator dbIterator = projectDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("projects.json").toFile(), objectMapper, "project");

        // Get all the studies contained in the project
        query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId)
                .append(StudyDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
        OpenCGAResult<Study> studyDataResult = studyDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UID.key()));
        if (studyDataResult.getNumResults() == 0) {
            logger.info("The project does not contain any study under the specified release");
            return;
        }
        dbIterator = studyDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("studies.json").toFile(), objectMapper, "studies");

        List<Long> studyIds = studyDataResult.getResults().stream().map(Study::getUid).collect(Collectors.toList());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), "<=" + release)
                .append(Constants.ALL_VERSIONS, true);
        dbIterator = sampleDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("samples.json").toFile(), objectMapper, "samples");

        query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(IndividualDBAdaptor.QueryParams.SNAPSHOT.key(), "<=" + release)
                .append(Constants.ALL_VERSIONS, true);
        dbIterator = individualDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("individuals.json").toFile(), objectMapper, "individuals");

        query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(FamilyDBAdaptor.QueryParams.SNAPSHOT.key(), "<=" + release)
                .append(Constants.ALL_VERSIONS, true);
        dbIterator = familyDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("families.json").toFile(), objectMapper, "families");

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(FileDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
        dbIterator = fileDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("files.json").toFile(), objectMapper, "files");

        query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
        dbIterator = clinicalDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("clinical_analysis.json").toFile(), objectMapper, "search analysis");

        query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(CohortDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
        dbIterator = cohortDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("cohorts.json").toFile(), objectMapper, "cohorts");

        query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(JobDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
        dbIterator = jobDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("jobs.json").toFile(), objectMapper, "jobs");

    }

    private void exportToFile(DBIterator dbIterator, File file, ObjectMapper objectMapper, String entity) throws CatalogException {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(file);
        } catch (IOException e) {
            logger.error("Error creating fileWriter: {}", e.getMessage(), e);
            throw new CatalogException("Error creating fileWriter: " + e.getMessage());
        }
        logger.info("Exporting " + entity + "...");
        while (dbIterator.hasNext()) {
            Map<String, Object> next = (Map) dbIterator.next();
            if (next.get("groups") != null) {
                next.put("groups", Collections.emptyList());
            }
            if (next.get("_acl") != null) {
                next.put("_acl", Collections.emptyList());
            }
            try {
                fileWriter.write(objectMapper.writeValueAsString(next));
                fileWriter.write("\n");
            } catch (IOException e) {
                logger.error("Error writing to file: {}", e.getMessage(), e);
                throw new CatalogException("Error writing to file: " + e.getMessage());
            }
        }
        try {
            fileWriter.close();
        } catch (IOException e) {
            logger.error("Error closing FileWriter: {}", e.getMessage(), e);
        }
    }

    public OpenCGAResult rank(String userId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = this.catalogManager.getUserManager().getUserId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", null, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult groupBy(String userId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = this.catalogManager.getUserManager().getUserId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", null, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult groupBy(String userId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = this.catalogManager.getUserManager().getUserId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", null, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // Return true if currentRelease is found in any entry
    private boolean checkCurrentReleaseInUse(List<Study> allStudiesInProject, int currentRelease) throws CatalogException {
        for (Study study : allStudiesInProject) {
            if (study.getRelease() == currentRelease) {
                return true;
            }
        }
        List<Long> studyIds = allStudiesInProject.stream().map(Study::getUid).collect(Collectors.toList());
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(FileDBAdaptor.QueryParams.RELEASE.key(), currentRelease);
        if (fileDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }
        if (sampleDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }
        if (individualDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }
        if (cohortDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }
        if (familyDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }
        if (jobDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }
//        if (diseasePanelDBAdaptor.count(query).getNumMatches() > 0) {
//            return true;
//        }
        if (clinicalDBAdaptor.count(query).getNumMatches() > 0) {
            return true;
        }

        return false;
    }

}
