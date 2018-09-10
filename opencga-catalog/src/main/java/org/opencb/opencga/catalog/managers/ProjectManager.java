/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;

import java.io.*;
import java.io.File;
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

    ProjectManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                   Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
    }

    public String getOwner(long projectId) throws CatalogException {
        return projectDBAdaptor.getOwnerId(projectId);
    }

    /**
     * Fetch the project qualifying the projectStr structure as long as userId has permissions to see it.
     *
     * @param projectStr string that can contain the full qualified name (owner@projectId) or just the projectId.
     * @param userId     user asking for the project information.
     * @return a QueryResult containing the project.
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
            if (UUIDUtils.isOpenCGAUUID(projectStr)) {
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

            QueryResult<Project> projectQueryResult = projectDBAdaptor.get(query, projectOptions);
            if (projectQueryResult.getNumResults() > 1) {
                throw new CatalogException("Please be more concrete with the project. More than one project found for " + userId + " user");
            } else if (projectQueryResult.getNumResults() == 1) {
                return projectQueryResult.first();
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
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, queryOptions, userId);

        if (studyQueryResult.getNumResults() > 0) {
            Set<String> projectFqnSet = new HashSet<>();
            for (Study study : studyQueryResult.getResult()) {
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
                if (studyDBAdaptor.count(query).first() == 0) {
                    throw new CatalogException("Project " + projectStr + " not found");
                } else {
                    throw CatalogAuthorizationException.deny(userId, "view", "project", projectStr, null);
                }
            } else {
                throw new CatalogException("No projects shared or owned by user " + userId);
            }
        }
    }

    /**
     * Obtain the list of projects and studies that are shared with the user.
     *
     * @param userId       user whose projects and studies are being shared with.
     * @param queryOptions QueryOptions object.
     * @param sessionId    Session id which should correspond to userId.
     * @return A QueryResult object containing the list of projects and studies that are shared with the user.
     * @throws CatalogException CatalogException
     */
    public QueryResult<Project> getSharedProjects(String userId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return get(new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "!=" + userId), queryOptions, sessionId);
    }

    public QueryResult<Project> create(String id, String name, String description, String organization, String scientificName,
                                       String commonName, String taxonomyCode, String assembly, QueryOptions options, String sessionId)
            throws CatalogException {

        ParamUtils.checkParameter(name, ProjectDBAdaptor.QueryParams.NAME.key());
        ParamUtils.checkParameter(scientificName, ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key());
        ParamUtils.checkParameter(assembly, ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key());
        ParamUtils.checkAlias(id, ProjectDBAdaptor.QueryParams.ID.key());
        ParamUtils.checkParameter(sessionId, "token");

        //Only the user can create a project
        String userId = this.catalogManager.getUserManager().getUserId(sessionId);
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

        Project project = new Project(id, name, description, new Status(), organization, organism, 1);

        project.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PROJECT));
        QueryResult<Project> queryResult = projectDBAdaptor.insert(project, userId, options);
        project = queryResult.getResult().get(0);

        try {
            catalogIOManagerFactory.getDefault().createProject(userId, Long.toString(project.getUid()));
        } catch (CatalogIOException e) {
            try {
                projectDBAdaptor.delete(project.getUid());
            } catch (Exception e1) {
                logger.error("Error deleting project from catalog after failing creating the folder in the filesystem", e1);
                throw e;
            }
            throw e;
        }
        userDBAdaptor.updateUserLastModified(userId);
        auditManager.recordCreation(AuditRecord.Resource.project, queryResult.first().getUid(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    /**
     * Reads a project from Catalog given a project id or alias.
     *
     * @param projectStr Project id or alias.
     * @param options    Read options
     * @param sessionId  sessionId
     * @return The specified object
     * @throws CatalogException CatalogException
     */
    @Deprecated
    public QueryResult<Project> get(String projectStr, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Project project = resolveId(projectStr, userId);
        return projectDBAdaptor.get(project.getUid(), options);
    }

    public List<QueryResult<Project>> get(List<String> projectList, QueryOptions options, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<Project>> results = new ArrayList<>(projectList.size());

        for (int i = 0; i < projectList.size(); i++) {
            String project = projectList.get(i);
            try {
                QueryResult<Project> projectResult = get(project, options, sessionId);
                results.add(projectResult);
            } catch (CatalogException e) {
                if (silent) {
                    results.add(new QueryResult<>(projectList.get(i), 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return results;
    }

    /**
     * Fetch all the project objects matching the query.
     *
     * @param query     Query to catalog.
     * @param options   Query options, like "include", "exclude", "limit" and "skip"
     * @param sessionId sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    public QueryResult<Project> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = new Query(query);
        String userId = catalogManager.getUserManager().getUserId(sessionId);

        // If study is provided, we need to check if it will be study alias or id
        if (StringUtils.isNotEmpty(query.getString(ProjectDBAdaptor.QueryParams.STUDY.key()))) {
            List<String> studyList = query.getAsStringList(ProjectDBAdaptor.QueryParams.STUDY.key());
            List<Long> idList = new ArrayList<>();
            List<String> aliasList = new ArrayList<>();
            for (String studyStr : studyList) {
                if (StringUtils.isNumeric(studyStr) && Long.parseLong(studyStr) > configuration.getCatalog().getOffset()) {
                    idList.add(Long.parseLong(studyStr));
                } else {
                    aliasList.add(studyStr);
                }
            }

            query.remove(ProjectDBAdaptor.QueryParams.STUDY.key());
            if (CollectionUtils.isNotEmpty(idList)) {
                query.put(ProjectDBAdaptor.QueryParams.STUDY_UID.key(), StringUtils.join(idList, ","));
            }
            if (CollectionUtils.isNotEmpty(aliasList)) {
                query.put(ProjectDBAdaptor.QueryParams.STUDY_ALIAS.key(), StringUtils.join(aliasList, ","));
            }
        }

        return projectDBAdaptor.get(query, options, userId);
    }

    /**
     * Update metada from projects.
     *
     * @param projectStr Project id or alias.
     * @param parameters Parameters to change.
     * @param options    options
     * @param sessionId  sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    public QueryResult<Project> update(String projectStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = this.catalogManager.getUserManager().getUserId(sessionId);
        Project project = resolveId(projectStr, userId);
        long projectId = project.getUid();
        authorizationManager.checkCanEditProject(projectId, userId);

        QueryResult<Project> queryResult = new QueryResult<>();
        if (parameters.containsKey(ProjectDBAdaptor.QueryParams.ID.key())) {
            editId(project, parameters.getString(ProjectDBAdaptor.QueryParams.ID.key()), sessionId);

            //Clone and remove alias from parameters. Do not modify the original parameter
            parameters = new ObjectMap(parameters);
            parameters.remove(ProjectDBAdaptor.QueryParams.ID.key());
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
                queryResult = projectDBAdaptor.update(projectId, objectMap, QueryOptions.empty());
            } else {
                throw new CatalogException("Cannot update organism information that is already filled in");
            }
        }

        for (String s : parameters.keySet()) {
            if (!s.matches("name|description|organization|attributes")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }
        userDBAdaptor.updateUserLastModified(userId);
        if (parameters.size() > 0) {
            queryResult = projectDBAdaptor.update(projectId, parameters, QueryOptions.empty());
        }
        auditManager.recordUpdate(AuditRecord.Resource.project, projectId, userId, parameters, null, null);
        return queryResult;
    }

    void editId(Project project, String newProjectId, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(newProjectId, "new project id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = this.catalogManager.getUserManager().getUserId(sessionId);
        authorizationManager.checkCanEditProject(project.getUid(), userId);

        String owner = project.getFqn().split("@")[0];
        if (StringUtils.isEmpty(owner)) {
            throw new CatalogException("Internal error. Project fqn required");
        }

        userDBAdaptor.updateUserLastModified(userId);
        projectDBAdaptor.editId(owner, project.getUid(), project.getId(), newProjectId);
        auditManager.recordUpdate(AuditRecord.Resource.project, project.getUid(), userId,
                new ObjectMap(ProjectDBAdaptor.QueryParams.ID.key(), newProjectId), null, null);
    }

    public QueryResult<Integer> incrementRelease(String projectStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);

        Project project = resolveId(projectStr, userId);
        long projectId = project.getUid();

        authorizationManager.checkCanEditProject(projectId, userId);

        // Obtain the current release number
        int currentRelease = project.getCurrentRelease();

        // Check current release has been used at least in one study or file or cohort or individual...
        List<Study> allStudiesInProject = project.getStudies();
        if (allStudiesInProject.isEmpty()) {
            throw new CatalogException("Cannot increment current release number. No studies found for release " + currentRelease);
        }

        if (checkCurrentReleaseInUse(allStudiesInProject, currentRelease)) {
            // Increment current project release
            QueryResult<Integer> integerQueryResult = projectDBAdaptor.incrementCurrentRelease(projectId);

            // Upgrade release in sample, family and individuals
            for (Study study : allStudiesInProject) {
                sampleDBAdaptor.updateProjectRelease(study.getUid(), integerQueryResult.first());
                individualDBAdaptor.updateProjectRelease(study.getUid(), integerQueryResult.first());
                familyDBAdaptor.updateProjectRelease(study.getUid(), integerQueryResult.first());
                panelDBAdaptor.updateProjectRelease(study.getUid(), integerQueryResult.first());
            }

            return integerQueryResult;
        } else {
            throw new CatalogException("Cannot increment current release number. The current release " + currentRelease + " has not yet "
                    + "been used in any entry");
        }
    }

    public void importReleases(String owner, String inputDirStr, String sessionId) throws CatalogException, IOException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        if (!"admin".equals(userId)) {
            throw new CatalogAuthorizationException("Only admin of OpenCGA is authorised to import data");
        }

        QueryResult<User> userQueryResult = userDBAdaptor.get(owner, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.ACCOUNT.key(), UserDBAdaptor.QueryParams.PROJECTS.key())), null);
        if (userQueryResult.getNumResults() == 0) {
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
        project.put(ProjectDBAdaptor.QueryParams.SIZE.key(), ParamUtils.getAsLong(project.get(ProjectDBAdaptor.QueryParams.SIZE.key())));

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

        // Reading clinical analysis
        try (BufferedReader br = new BufferedReader(new FileReader(inputDir.resolve("clinical_analysis.json").toFile()))) {
            logger.info("Importing clinical analysis...");
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

    public void exportReleases(String projectStr, int release, String outputDirStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        if (!"admin".equals(userId)) {
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
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UID.key()));
        if (studyQueryResult.getNumResults() == 0) {
            logger.info("The project does not contain any study under the specified release");
            return;
        }
        dbIterator = studyDBAdaptor.nativeIterator(query, QueryOptions.empty());
        exportToFile(dbIterator, outputDir.resolve("studies.json").toFile(), objectMapper, "studies");

        List<Long> studyIds = studyQueryResult.getResult().stream().map(Study::getUid).collect(Collectors.toList());

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
        exportToFile(dbIterator, outputDir.resolve("clinical_analysis.json").toFile(), objectMapper, "clinical analysis");

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

    public QueryResult rank(String userId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = this.catalogManager.getUserManager().getUserId(sessionId);
        if (!userOfQuery.equals(userId)) {
            // The user cannot read projects of other users.
            throw CatalogAuthorizationException.cantRead(userOfQuery, "Project", -1, userId);
        }

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = projectDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult groupBy(String userId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = this.catalogManager.getUserManager().getUserId(sessionId);
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

    public QueryResult groupBy(String userId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(userId, "userId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userOfQuery = this.catalogManager.getUserManager().getUserId(sessionId);
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

    // Return true if currentRelease is found in any entry
    private boolean checkCurrentReleaseInUse(List<Study> allStudiesInProject, int currentRelease) throws CatalogDBException {
        for (Study study : allStudiesInProject) {
            if (study.getRelease() == currentRelease) {
                return true;
            }
        }
        List<Long> studyIds = allStudiesInProject.stream().map(Study::getUid).collect(Collectors.toList());
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyIds)
                .append(FileDBAdaptor.QueryParams.RELEASE.key(), currentRelease);
        if (fileDBAdaptor.count(query).first() > 0) {
            return true;
        }
        if (sampleDBAdaptor.count(query).first() > 0) {
            return true;
        }
        if (individualDBAdaptor.count(query).first() > 0) {
            return true;
        }
        if (cohortDBAdaptor.count(query).first() > 0) {
            return true;
        }
        if (familyDBAdaptor.count(query).first() > 0) {
            return true;
        }
        if (jobDBAdaptor.count(query).first() > 0) {
            return true;
        }
//        if (diseasePanelDBAdaptor.count(query).first() > 0) {
//            return true;
//        }
        if (clinicalDBAdaptor.count(query).first() > 0) {
            return true;
        }

        return false;
    }

}
