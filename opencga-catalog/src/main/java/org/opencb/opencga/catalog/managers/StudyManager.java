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
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.DiseasePanelAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.models.summaries.VariableSetSummary;
import org.opencb.opencga.catalog.models.summaries.VariableSummary;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyManager extends AbstractManager implements IStudyManager {

    protected static Logger logger = LoggerFactory.getLogger(StudyManager.class);

    @Deprecated
    public StudyManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                        Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public StudyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                        Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
    }

    @Override
    public String getUserId(long studyId) throws CatalogException {
        return studyDBAdaptor.getOwnerId(studyId);
    }

    @Override
    public Long getProjectId(long studyId) throws CatalogException {
        return studyDBAdaptor.getProjectIdByStudyId(studyId);
    }

    public List<Long> getIds(String userId, String studyStr) throws CatalogException {
        if (StringUtils.isNumeric(studyStr)) {
            long studyId = Long.parseLong(studyStr);
            if (studyId > configuration.getCatalog().getOffset()) {
                studyDBAdaptor.checkId(studyId);
                return Arrays.asList(studyId);
            }
        }

        Query query = new Query();
        final QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.ID.key());

        if (StringUtils.isEmpty(studyStr)) {
            if (!userId.equals("anonymous")) {
                // Obtain the projects of the user
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key());
                QueryResult<Project> projectQueryResult = projectDBAdaptor.get(userId, options);
                if (projectQueryResult.getNumResults() == 1) {
                    projectDBAdaptor.checkId(projectQueryResult.first().getId());
                    query.put(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectQueryResult.first().getId());
                } else {
                    if (projectQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No projects found for user " + userId);
                    } else {
                        throw new CatalogException("More than one project found for user " + userId);
                    }
                }
            } else {
                // Anonymous user
                // 1. Check if the anonymous user has been given permissions in any study
                query.append(StudyDBAdaptor.QueryParams.ACL_MEMBER.key(), "anonymous");
                query.append(StudyDBAdaptor.QueryParams.ACL_PERMISSIONS.key(), StudyAclEntry.StudyPermissions.VIEW_STUDY);
            }

            QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, queryOptions);
            if (studyQueryResult.getNumResults() == 0) {
                throw new CatalogException("No studies found for user " + userId);
            } else {
                return studyQueryResult.getResult().stream().map(study -> study.getId()).collect(Collectors.toList());
            }

        } else {

            String[] split = studyStr.split(":");
            List<Long> projectIds;
            if (split.length > 2) {
                throw new CatalogException("More than one : separator found. Format: [[user@]project:]study");
            }

            String aliasStudy;
            String aliasProject = null;
            if (split.length == 2) {
                aliasStudy = split[1];
                aliasProject = split[0];
            } else {
                aliasStudy = studyStr;
            }

            List<Long> retStudies = new ArrayList<>();
            List<String> aliasList = new ArrayList<>();
            if (!aliasStudy.equals("*")) {
                // Check if there is more than one study listed in aliasStudy
                String[] split1 = aliasStudy.split(",");
                for (String studyStrAux : split1) {
                    if (StringUtils.isNumeric(studyStrAux)) {
                        retStudies.add(Long.parseLong(studyStrAux));
                    } else {
                        aliasList.add(studyStrAux);
                    }
                }
            }

            if (aliasList.size() == 0 && retStudies.size() > 0) { // The list of provided studies were all long ids
                return retStudies;
            }

            if (!userId.equals("anonymous")) {
                if (aliasProject != null) {
                    projectIds = Arrays.asList(catalogManager.getProjectManager().getId(userId, aliasProject));
                } else {
                    // Obtain the projects of the user
                    QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key());
                    QueryResult<Project> projectQueryResult = projectDBAdaptor.get(userId, options);
                    if (projectQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No projects found for user " + userId);
                    } else {
                        projectIds = projectQueryResult.getResult().stream().map(project -> project.getId()).collect(Collectors.toList());
                    }
                }
                query.put(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectIds);
            } else {
                // Anonymous user
                if (aliasProject != null) {
                    projectIds = catalogManager.getProjectManager().getIds(userId, aliasProject);
                    query.put(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectIds);
                }

                // Add permissions for user anonymous in the query
                query.append(StudyDBAdaptor.QueryParams.ACL_MEMBER.key(), "anonymous");
                query.append(StudyDBAdaptor.QueryParams.ACL_PERMISSIONS.key(), StudyAclEntry.StudyPermissions.VIEW_STUDY);
            }

            if (aliasList.size() > 0) {
                // This if is justified by the fact that we might not have any alias or id but an *
                query.put(StudyDBAdaptor.QueryParams.ALIAS.key(), aliasList);
            }

            QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, queryOptions);
            if (studyQueryResult.getNumResults() == 0) {
                throw new CatalogException("No studies found for user " + userId);
            } else {
                if (aliasList.size() > 0 && studyQueryResult.getNumResults() != aliasList.size()) {
                    throw new CatalogException("Not all the studies were found. Found " + studyQueryResult.getNumResults() + " out of "
                            + aliasList.size());

                } else {
                    retStudies.addAll(studyQueryResult.getResult().stream().map(study -> study.getId()).collect(Collectors.toList()));
                    return retStudies;
                }
            }
        }
    }

    @Override
    public Long getId(String userId, String studyStr) throws CatalogException {
        logger.debug("user {}, study {}", userId, studyStr);
        if (studyStr != null && studyStr.contains(",")) {
            throw new CatalogException("Only one study is allowed. More than one study found in " + studyStr);
        }
        List<Long> ids = getIds(userId, studyStr);
        if (ids.size() > 1) {
            throw new CatalogException("More than one study was found for study '" + studyStr + '\'');
        } else {
            return ids.get(0);
        }
    }

    @Deprecated
    @Override
    public Long getId(String studyId) throws CatalogException {
        if (StringUtils.isNumeric(studyId)) {
            return Long.parseLong(studyId);
        }

        String[] split = studyId.split("@");
        if (split.length != 2) {
            return -1L;
        }
        String[] projectStudy = split[1].replace(':', '/').split("/", 2);
        if (projectStudy.length != 2) {
            return -2L;
        }
        long projectId = projectDBAdaptor.getId(split[0], projectStudy[0]);
        return studyDBAdaptor.getId(projectId, projectStudy[1]);
    }

    @Override
    public QueryResult<Study> create(long projectId, String name, String alias, Study.Type type, String creationDate,
                                     String description, Status status, String cipher, String uriScheme, URI uri,
                                     Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats, Map<String, Object> attributes,
                                     QueryOptions options, String sessionId) throws CatalogException {

        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkParameter(alias, "alias");
        ParamUtils.checkObj(type, "type");
        ParamUtils.checkAlias(alias, "alias", configuration.getCatalog().getOffset());

        String userId = catalogManager.getUserManager().getId(sessionId);
        description = ParamUtils.defaultString(description, "");
//        creatorId = ParamUtils.defaultString(creatorId, userId);
        creationDate = ParamUtils.defaultString(creationDate, TimeUtils.getTime());
        status = ParamUtils.defaultObject(status, Status::new);
        cipher = ParamUtils.defaultString(cipher, "none");
        if (uri != null) {
            if (uri.getScheme() == null) {
                throw new CatalogException("StudyUri must specify the scheme");
            } else {
                if (uriScheme != null && !uriScheme.isEmpty()) {
                    if (!uriScheme.equals(uri.getScheme())) {
                        throw new CatalogException("StudyUri must specify the scheme");
                    }
                } else {
                    uriScheme = uri.getScheme();
                }
            }
        } else {
            uriScheme = catalogIOManagerFactory.getDefaultCatalogScheme();
        }
        datastores = ParamUtils.defaultObject(datastores, HashMap<File.Bioformat, DataStore>::new);
        stats = ParamUtils.defaultObject(stats, HashMap<String, Object>::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uriScheme);

//        String projectOwnerId = projectDBAdaptor.getProjectOwnerId(projectId);


        /* Check project permissions */
        if (!projectDBAdaptor.getOwnerId(projectId).equals(userId)) {
            throw new CatalogException("Permission denied: Only the owner of the project can create studies.");
        }

        LinkedList<File> files = new LinkedList<>();
        LinkedList<Experiment> experiments = new LinkedList<>();
        LinkedList<Job> jobs = new LinkedList<>();

        File rootFile = new File(".", File.Type.DIRECTORY, null, null, "", "study root folder",
                new File.FileStatus(File.FileStatus.READY), 0);
        files.add(rootFile);

        // We set all the permissions for the owner of the study.
        // StudyAcl studyAcl = new StudyAcl(userId, AuthorizationManager.getAdminAcls());

        Study study = new Study(-1, name, alias, type, creationDate, description, status, TimeUtils.getTime(),
                0, cipher, new LinkedList<>(), new LinkedList<>(), experiments, files, jobs, new LinkedList<>(), new LinkedList<>(),
                new LinkedList<>(), new LinkedList<>(), Collections.emptyList(), new LinkedList<>(), null, datastores, stats, attributes);

        /* CreateStudy */
        QueryResult<Study> result = studyDBAdaptor.insert(projectId, study, options);
        study = result.getResult().get(0);

        //URI studyUri;
        if (uri == null) {
            try {
                uri = catalogIOManager.createStudy(userId, Long.toString(projectId), Long.toString(study.getId()));
            } catch (CatalogIOException e) {
                try {
                    studyDBAdaptor.delete(study.getId());
                } catch (Exception e1) {
                    logger.error("Can't delete study after failure creating study", e1);
                }
                throw e;
            }
        }

        study = studyDBAdaptor.update(study.getId(), new ObjectMap("uri", uri)).first();
//        auditManager.recordCreation(AuditRecord.Resource.study, study.getId(), userId, study, null, null);
        auditManager.recordAction(AuditRecord.Resource.study, AuditRecord.Action.create, AuditRecord.Magnitude.low, study.getId(), userId,
                null, study, null, null);
        long rootFileId = fileDBAdaptor.getId(study.getId(), "");    //Set studyUri to the root folder too
        rootFile = fileDBAdaptor.update(rootFileId, new ObjectMap("uri", uri)).first();
//        auditManager.recordCreation(AuditRecord.Resource.file, rootFile.getId(), userId, rootFile, null, null);
        auditManager.recordAction(AuditRecord.Resource.file, AuditRecord.Action.create, AuditRecord.Magnitude.low, rootFile.getId(), userId,
                null, rootFile, null, null);
        userDBAdaptor.updateUserLastModified(userId);
        return result;
    }

    @Deprecated
    @Override
    public QueryResult<Study> share(long studyId, AclEntry acl) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void membersHavePermissionsInStudy(long studyId, List<String> members) throws CatalogException {
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
                throw new CatalogException("Cannot update ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }
    }

    private boolean memberExists(long studyId, String member) throws CatalogException {
        QueryResult<StudyAclEntry> acl = authorizationManager.getAcl(studyId, Arrays.asList(member),
                MongoDBAdaptorFactory.STUDY_COLLECTION);
        return acl.getNumResults() > 0;
    }

    @Override
    public QueryResult<Study> get(Long studyId, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getId(sessionId);
        studyDBAdaptor.checkId(studyId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);

        QueryResult<Study> studyResult = studyDBAdaptor.get(studyId, options);
        if (!studyResult.getResult().isEmpty()) {
            authorizationManager.filterFiles(userId, studyId, studyResult.getResult().get(0).getFiles());
        }

        return studyResult;
    }

    @Override
    public QueryResult<Study> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        QueryOptions qOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        String userId = catalogManager.getUserManager().getId(sessionId);

        if (!qOptions.containsKey("include") || qOptions.get("include") == null || qOptions.getAsStringList("include").isEmpty()) {
            qOptions.addToListOption("exclude", "projects.studies.attributes.studyConfiguration");
        }

        QueryResult<Study> allStudies = studyDBAdaptor.get(query, qOptions);
        List<Study> studies = allStudies.getResult();

        authorizationManager.filterStudies(userId, studies);
        allStudies.setResult(studies);
        allStudies.setNumResults(studies.size());

        return allStudies;
    }

    @Override
    public QueryResult<Study> update(Long studyId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkId(studyId, "studyId");
        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        if (parameters.containsKey("alias")) {
            rename(studyId, parameters.getString("alias"), sessionId);

            //Clone and remove alias from parameters. Do not modify the original parameter
            parameters = new ObjectMap(parameters);
            parameters.remove("alias");
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|type|description|attributes|stats")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);
        userDBAdaptor.updateUserLastModified(ownerId);
        QueryResult<Study> result = studyDBAdaptor.update(studyId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.study, studyId, userId, parameters, null, null);
        return result;
    }

    private QueryResult rename(long studyId, String newStudyAlias, String sessionId) throws CatalogException {
        ParamUtils.checkAlias(newStudyAlias, "newStudyAlias", configuration.getCatalog().getOffset());
        String userId = catalogManager.getUserManager().getId(sessionId);
//        String studyOwnerId = studyDBAdaptor.getStudyOwnerId(studyId);

        //User can't write/modify the study
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        // Both users must bu updated
        userDBAdaptor.updateUserLastModified(userId);
//        userDBAdaptor.updateUserLastModified(studyOwnerId);
        //TODO get all shared users to updateUserLastModified

        //QueryResult queryResult = studyDBAdaptor.renameStudy(studyId, newStudyAlias);
        auditManager.recordUpdate(AuditRecord.Resource.study, studyId, userId, new ObjectMap("alias", newStudyAlias), null, null);
        return new QueryResult();

    }


    @Override
    public List<QueryResult<Study>> delete(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<Study>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<Study>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QueryResult<Study>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        throw new NotImplementedException("Project: Operation not yet supported");
    }

    @Override
    public QueryResult rank(long projectId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(projectId, "projectId");

        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkProjectPermission(projectId, userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long projectId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(projectId, "projectId");

        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkProjectPermission(projectId, userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long projectId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(projectId, "projectId");

        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkProjectPermission(projectId, userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult<StudySummary> getSummary(long studyId, String sessionId, QueryOptions queryOptions) throws CatalogException {

        long startTime = System.currentTimeMillis();

        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);

        Study studyInfo = get(studyId, queryOptions, sessionId).first();

        StudySummary studySummary = new StudySummary()
                .setAlias(studyInfo.getAlias())
                .setAttributes(studyInfo.getAttributes())
                .setCipher(studyInfo.getCipher())
                .setCreationDate(studyInfo.getCreationDate())
                .setDatasets(studyInfo.getDatasets().size())
                .setDescription(studyInfo.getDescription())
                .setDiskUsage(studyInfo.getSize())
                .setExperiments(studyInfo.getExperiments())
                .setGroups(studyInfo.getGroups())
                .setName(studyInfo.getName())
                .setStats(studyInfo.getStats())
                .setStatus(studyInfo.getStatus())
                .setType(studyInfo.getType())
                .setVariableSets(studyInfo.getVariableSets());


        Long nFiles = fileDBAdaptor.count(
                new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.FILE)
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setFiles(nFiles);

        Long nSamples = sampleDBAdaptor.count(
                new Query(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setSamples(nSamples);

        Long nJobs = jobDBAdaptor.count(
                new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setJobs(nJobs);

        Long nCohorts = cohortDBAdaptor.count(
                new Query(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setCohorts(nCohorts);

        Long nIndividuals = individualDBAdaptor.count(
                new Query(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(IndividualDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setIndividuals(nIndividuals);

        return new QueryResult<>("Study summary", (int) (System.currentTimeMillis() - startTime), 1, 1, "", "",
                Collections.singletonList(studySummary));
    }

//    @Deprecated
//    @Override
//    public QueryResult<StudyAclEntry> getAcls(String studyStr, List<String> members, String sessionId) throws CatalogException {
//        long startTime = System.currentTimeMillis();
//        String userId = catalogManager.getUserManager().getId(sessionId);
//        long studyId = getId(userId, studyStr);
//        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
//
//        // Split and obtain the set of members (users + groups), users and groups
//        Set<String> memberSet = new HashSet<>();
//        Set<String> userIds = new HashSet<>();
//        Set<String> groupIds = new HashSet<>();
//        for (String member: members) {
//            memberSet.add(member);
//            if (!member.startsWith("@")) {
//                userIds.add(member);
//            } else {
//                groupIds.add(member);
//            }
//        }
//
//
//        // Obtain the groups the user might belong to in order to be able to get the permissions properly
//        // (the permissions might be given to the group instead of the user)
//        // Map of group -> users
//        Map<String, List<String>> groupUsers = new HashMap<>();
//
//        if (userIds.size() > 0) {
//            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
//            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
//            // We add the groups where the users might belong to to the memberSet
//            if (groups.getNumResults() > 0) {
//                for (Group group : groups.getResult()) {
//                    for (String tmpUserId : group.getUserIds()) {
//                        if (userIds.contains(tmpUserId)) {
//                            memberSet.add(group.getName());
//
//                            if (!groupUsers.containsKey(group.getName())) {
//                                groupUsers.put(group.getName(), new ArrayList<>());
//                            }
//                            groupUsers.get(group.getName()).add(tmpUserId);
//                        }
//                    }
//                }
//            }
//        }
//        List<String> memberList = memberSet.stream().collect(Collectors.toList());
//        QueryResult<StudyAclEntry> studyAclQueryResult = studyDBAdaptor.getAcl(studyId, memberList);
//
//        if (members.size() == 0) {
//            return studyAclQueryResult;
//        }
//
//        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the
// user
//        // instead of the group.
//        Map<String, StudyAclEntry> studyAclHashMap = new HashMap<>();
//        for (StudyAclEntry studyAcl : studyAclQueryResult.getResult()) {
//            String tmpMember = studyAcl.getMember();
//            if (memberList.contains(tmpMember)) {
//                if (tmpMember.startsWith("@")) {
//                    // Check if the user was demanding the group directly or a user belonging to the group
//                    if (groupIds.contains(tmpMember)) {
//                        studyAclHashMap.put(tmpMember, studyAcl);
//                    } else {
//                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
//                        if (groupUsers.containsKey(tmpMember)) {
//                            for (String tmpUserId : groupUsers.get(tmpMember)) {
//                                if (userIds.contains(tmpUserId)) {
//                                    studyAclHashMap.put(tmpUserId, new StudyAclEntry(tmpUserId, studyAcl.getPermissions()));
//                                }
//                            }
//                        }
//                    }
//                } else {
//                    // Add the user
//                    studyAclHashMap.put(tmpMember, studyAcl);
//                }
//            }
//
//        }
//
//        // We recreate the output that is in studyAclHashMap but in the same order the members were queried.
//        List<StudyAclEntry> studyAclList = new ArrayList<>(studyAclHashMap.size());
//        for (String member : members) {
//            if (studyAclHashMap.containsKey(member)) {
//                studyAclList.add(studyAclHashMap.get(member));
//            }
//        }
//
//        // Update queryResult info
//        studyAclQueryResult.setId(studyStr);
//        studyAclQueryResult.setNumResults(studyAclList.size());
//        studyAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
//        studyAclQueryResult.setResult(studyAclList);
//
//        return studyAclQueryResult;
//    }

    @Override
    public List<QueryResult<StudyAclEntry>> updateAcl(String studyStr, String memberIds, Study.StudyAclParams aclParams, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(studyStr)) {
            throw new CatalogException("Missing study parameter");
        }

        if (aclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
            permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, StudyAclEntry.StudyPermissions::valueOf);
        }

        if (StringUtils.isNotEmpty(aclParams.getTemplate())) {
            EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = null;
            if (aclParams.getTemplate().equals(AuthorizationManager.ROLE_ADMIN)) {
                studyPermissions = AuthorizationManager.getAdminAcls();
            } else if (aclParams.getTemplate().equals(AuthorizationManager.ROLE_ANALYST)) {
                studyPermissions = AuthorizationManager.getAnalystAcls();
            } else if (aclParams.getTemplate().equals(AuthorizationManager.ROLE_VIEW_ONLY)) {
                studyPermissions = AuthorizationManager.getViewOnlyAcls();
            }

            if (studyPermissions != null) {
                // Merge permissions from the template with the ones written
                Set<String> uniquePermissions = new HashSet<>();
                uniquePermissions.addAll(permissions);

                for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
                    uniquePermissions.add(studyPermission.toString());
                }

                permissions = new ArrayList<>(uniquePermissions.size());
                permissions.addAll(uniquePermissions);
            }
        }

        String userId = catalogManager.getUserManager().getId(sessionId);
        List<Long> studyIds = getIds(userId, studyStr);

        // Check the user has the permissions needed to change permissions
        for (Long studyId : studyIds) {
            authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        for (Long studyId : studyIds) {
            CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, studyId, members);
        }

        switch (aclParams.getAction()) {
            case SET:
                return authorizationManager.setStudyAcls(studyIds, members, permissions);
            case ADD:
                return authorizationManager.addStudyAcls(studyIds, members, permissions);
            case REMOVE:
                return authorizationManager.removeStudyAcls(studyIds, members, permissions);
            case RESET:
                removeAllPermissionsFromOtherEntities(studyIds, members);
//                // TODO: Improve this way of doing things
//                for (Long studyId : studyIds) {
//                    for (String member : members) {
//                        sampleDBAdaptor.removeAclsFromStudy(studyId, member);
//                        fileDBAdaptor.removeAclsFromStudy(studyId, member);
//                        jobDBAdaptor.removeAclsFromStudy(studyId, member);
//                        datasetDBAdaptor.removeAclsFromStudy(studyId, member);
//                        individualDBAdaptor.removeAclsFromStudy(studyId, member);
//                        cohortDBAdaptor.removeAclsFromStudy(studyId, member);
//                        panelDBAdaptor.removeAclsFromStudy(studyId, member);
//                    }
//                }
                return authorizationManager.removeStudyAcls(studyIds, members, null);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    private void removeAllPermissionsFromOtherEntities(List<Long> studyIds, List<String> members) throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(FileDBAdaptor.QueryParams.ACL_MEMBER.key(), members);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key());

        List<Long> sampleIds = new ArrayList<>();
        DBIterator<Sample> sampleDBIterator = sampleDBAdaptor.iterator(query, options);
        while (sampleDBIterator.hasNext()) {
            sampleIds.add(sampleDBIterator.next().getId());
        }
        authorizationManager.removeAcls(sampleIds, members, null, MongoDBAdaptorFactory.SAMPLE_COLLECTION);

        List<Long> fileIds = new ArrayList<>();
        DBIterator<File> fileDBIterator = fileDBAdaptor.iterator(query, options);
        while (fileDBIterator.hasNext()) {
            fileIds.add(fileDBIterator.next().getId());
        }
        authorizationManager.removeAcls(fileIds, members, null, MongoDBAdaptorFactory.FILE_COLLECTION);

        List<Long> jobIds = new ArrayList<>();
        DBIterator<Job> jobDBIterator = jobDBAdaptor.iterator(query, options);
        while (jobDBIterator.hasNext()) {
            jobIds.add(jobDBIterator.next().getId());
        }
        authorizationManager.removeAcls(jobIds, members, null, MongoDBAdaptorFactory.JOB_COLLECTION);

        List<Long> datasetIds = new ArrayList<>();
        DBIterator<Dataset> datasetDBIterator = datasetDBAdaptor.iterator(query, options);
        while (datasetDBIterator.hasNext()) {
            datasetIds.add(datasetDBIterator.next().getId());
        }
        authorizationManager.removeAcls(datasetIds, members, null, MongoDBAdaptorFactory.DATASET_COLLECTION);

        List<Long> individualIds = new ArrayList<>();
        DBIterator<Individual> individualDBIterator = individualDBAdaptor.iterator(query, options);
        while (individualDBIterator.hasNext()) {
            individualIds.add(individualDBIterator.next().getId());
        }
        authorizationManager.removeAcls(individualIds, members, null, MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);

        List<Long> cohortIds = new ArrayList<>();
        DBIterator<Cohort> cohortDBIterator = cohortDBAdaptor.iterator(query, options);
        while (cohortDBIterator.hasNext()) {
            cohortIds.add(cohortDBIterator.next().getId());
        }
        authorizationManager.removeAcls(cohortIds, members, null, MongoDBAdaptorFactory.COHORT_COLLECTION);

        List<Long> panelIds = new ArrayList<>();
        DBIterator<DiseasePanel> panelDBIterator = panelDBAdaptor.iterator(query, options);
        while (panelDBIterator.hasNext()) {
            panelIds.add(panelDBIterator.next().getId());
        }
        authorizationManager.removeAcls(panelIds, members, null, MongoDBAdaptorFactory.PANEL_COLLECTION);
    }

    @Override
    public QueryResult<Group> createGroup(String studyStr, String groupId, String users, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        studyDBAdaptor.checkId(studyId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        // Fix the groupId
        if (!groupId.startsWith("@")) {
            groupId = "@" + groupId;
        }

        // Create the list of users
        List<String> userList;
        if (users != null && !users.isEmpty()) {
            userList = Arrays.asList(users.split(","));
        } else {
            userList = Collections.emptyList();
        }

        // Check group exists
        if (existsGroup(studyId, groupId)) {
            throw new CatalogException("The group " + groupId + " already exists.");
        }

        // Check the list of users is ok
        for (String user : userList) {
            userDBAdaptor.checkId(user);
        }

        // Check that none of the users belong to other group
        StringBuilder errorMessage = new StringBuilder("Cannot create group. These users already belong to other group: ");
        boolean errorFlag = false;
        for (String user : userList) {
            if (memberBelongsToGroup(studyId, user)) {
                errorMessage.append(user).append(",");
                errorFlag = true;
            }
        }

        if (errorFlag) {
            throw new CatalogException(errorMessage.toString());
        }

        // Create the group
        return studyDBAdaptor.createGroup(studyId, groupId, userList);
    }

    private boolean existsGroup(long studyId, String groupId) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.GROUP_NAME.key(), groupId);
        return studyDBAdaptor.count(query).first() > 0;
    }

    private boolean memberBelongsToGroup(long studyId, String member) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), member);
        return studyDBAdaptor.count(query).first() > 0;
    }

    @Override
    public QueryResult<Group> getAllGroups(String studyStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        studyDBAdaptor.checkId(studyId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        Query query = new Query(StudyDBAdaptor.QueryParams.ID.key(), studyId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.GROUPS.key());

        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, queryOptions);
        List<Group> groupList;
        if (studyQueryResult != null && studyQueryResult.getNumResults() == 1) {
            groupList = studyQueryResult.first().getGroups();
        } else {
            groupList = Collections.emptyList();
        }

        return new QueryResult<>("Get all groups", studyQueryResult.getDbTime(), groupList.size(), groupList.size(),
                studyQueryResult.getWarningMsg(), studyQueryResult.getErrorMsg(), groupList);
    }

    @Override
    public QueryResult<Group> getGroup(String studyStr, String groupId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        studyDBAdaptor.checkId(studyId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        // Fix the groupId
        if (!groupId.startsWith("@")) {
            groupId = "@" + groupId;
        }

        return studyDBAdaptor.getGroup(studyId, groupId, Collections.emptyList());
    }

    @Override
    public QueryResult<Group> updateGroup(String studyStr, String groupId, @Nullable String addUsers, @Nullable String removeUsers,
                                          @Nullable String setUsers, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        studyDBAdaptor.checkId(studyId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        // Fix the groupId
        if (!groupId.startsWith("@")) {
            groupId = "@" + groupId;
        }

        // Check the group exists
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.GROUP_NAME.key(), groupId);
        if (studyDBAdaptor.count(query).first() == 0) {
            throw new CatalogException("The group " + groupId + " does not exist.");
        }

        List<String> userList;
        if (StringUtils.isNotEmpty(setUsers)) {
            userList = Arrays.asList(setUsers.split(","));
            studyDBAdaptor.setUsersToGroup(studyId, groupId, userList);
        } else {
            if (StringUtils.isNotEmpty(addUsers)) {
                userList = Arrays.asList(addUsers.split(","));
                studyDBAdaptor.addUsersToGroup(studyId, groupId, userList);
            }

            if (StringUtils.isNotEmpty(removeUsers)) {
                userList = Arrays.asList(removeUsers.split(","));
                studyDBAdaptor.removeUsersFromGroup(studyId, groupId, userList);
            }
        }

        return studyDBAdaptor.getGroup(studyId, groupId, Collections.emptyList());
    }

    @Override
    public QueryResult<Group> syncGroupWith(String studyStr, String groupId, Group.Sync syncedFrom, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        studyDBAdaptor.checkId(studyId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        if (StringUtils.isEmpty(groupId)) {
            throw new CatalogException("Missing group name parameter");
        }

        if (syncedFrom == null) {
            throw new CatalogException("Missing sync object");
        }

        // Fix the groupId
        if (!groupId.startsWith("@")) {
            groupId = "@" + groupId;
        }

        QueryResult<Group> group = studyDBAdaptor.getGroup(studyId, groupId, Collections.emptyList());
        if (group.first().getSyncedFrom() != null && StringUtils.isNotEmpty(group.first().getSyncedFrom().getAuthOrigin())
                && StringUtils.isNotEmpty(group.first().getSyncedFrom().getRemoteGroup())) {
            throw new CatalogException("Cannot modify already existing sync information.");
        }

        // Check the group exists
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.GROUP_NAME.key(), groupId);
        if (studyDBAdaptor.count(query).first() == 0) {
            throw new CatalogException("The group " + groupId + " does not exist.");
        }

        studyDBAdaptor.updateSyncFromGroup(studyId, groupId, syncedFrom);

        return studyDBAdaptor.getGroup(studyId, groupId, Collections.emptyList());
    }

    @Override
    public QueryResult<Group> deleteGroup(String studyStr, String groupId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        studyDBAdaptor.checkId(studyId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPDATE_STUDY);

        // Fix the groupId
        if (!groupId.startsWith("@")) {
            groupId = "@" + groupId;
        }

        QueryResult<Group> group = studyDBAdaptor.getGroup(studyId, groupId, Collections.emptyList());
        group.setId("Delete group");

        // Remove the permissions the group might have had
        if (authorizationManager.memberHasPermissionsInStudy(studyId, groupId)) {
            Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
            updateAcl(Long.toString(studyId), groupId, aclParams, sessionId);
        }

        studyDBAdaptor.deleteGroup(studyId, groupId);

        return group;
    }

    @Override
    public Long getDiseasePanelId(String userId, String panelStr) throws CatalogException {
        if (StringUtils.isNumeric(panelStr)) {
            return Long.parseLong(panelStr);
        }

        // Resolve the studyIds and filter the panelName
        ObjectMap parsedPanelStr = parseFeatureId(userId, panelStr);
        List<Long> studyIds = getStudyIds(parsedPanelStr);
        String panelName = parsedPanelStr.getString("featureName");

        Query query = new Query(PanelDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(PanelDBAdaptor.QueryParams.NAME.key(), panelName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.panels.id");
        QueryResult<DiseasePanel> queryResult = panelDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one panel id found based on " + panelName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Override
    public QueryResult<DiseasePanel> createDiseasePanel(String studyStr, String name, String disease, String description,
                                                        String genes, String regions, String variants,
                                                        QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);
        ParamUtils.checkParameter(disease, "disease");
        description = ParamUtils.defaultString(description, "");
        List<String> geneList = Collections.emptyList();
        List<String> regionList = Collections.emptyList();
        List<String> variantList = Collections.emptyList();
        if (genes != null) {
            geneList = Arrays.asList(genes.split(","));
        }
        if (regions != null) {
            regionList = Arrays.asList(regions.split(","));
        }
        if (variants != null) {
            variantList = Arrays.asList(variants.split(","));
        }

        if (geneList.size() == 0 && regionList.size() == 0 && variantList.size() == 0) {
            throw new CatalogException("Cannot create a new disease panel with no genes, regions and variants. At least, one of them should"
                    + " be provided.");
        }

        DiseasePanel diseasePanel = new DiseasePanel(-1, name, disease, description, geneList, regionList, variantList,
                new DiseasePanel.PanelStatus());

        QueryResult<DiseasePanel> queryResult = panelDBAdaptor.insert(diseasePanel, studyId, options);
//        auditManager.recordCreation(AuditRecord.Resource.panel, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.panel, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;

    }

    @Override
    public QueryResult<DiseasePanel> getDiseasePanel(String panelStr, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        Long panelId = getDiseasePanelId(userId, panelStr);
        authorizationManager.checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.VIEW);
        QueryResult<DiseasePanel> queryResult = panelDBAdaptor.get(panelId, options);
        return queryResult;
    }

    @Override
    public QueryResult<DiseasePanel> updateDiseasePanel(String panelStr, ObjectMap parameters, String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        String userId = catalogManager.getUserManager().getId(sessionId);
        Long diseasePanelId = getDiseasePanelId(userId, panelStr);
        authorizationManager.checkDiseasePanelPermission(diseasePanelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.UPDATE);

        for (String s : parameters.keySet()) {
            if (!s.matches("name|disease")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }

        QueryResult<DiseasePanel> result = panelDBAdaptor.update(diseasePanelId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.panel, diseasePanelId, userId, parameters, null, null);
        return result;
    }

    @Override
    public QueryResult<VariableSetSummary> getVariableSetSummary(long variableSetId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET);

        QueryResult<VariableSet> variableSet = studyDBAdaptor.getVariableSet(variableSetId, new QueryOptions());
        if (variableSet.getNumResults() == 0) {
            logger.error("getVariableSetSummary: Could not find variable set id {}. {} results returned", variableSetId,
                    variableSet.getNumResults());
            throw new CatalogDBException("Variable set " + variableSetId + " not found.");
        }

        int dbTime = 0;

        VariableSetSummary variableSetSummary = new VariableSetSummary(variableSetId, variableSet.first().getName());

        QueryResult<VariableSummary> annotationSummary = sampleDBAdaptor.getAnnotationSummary(variableSetId);
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setSamples(annotationSummary.getResult());

        annotationSummary = cohortDBAdaptor.getAnnotationSummary(variableSetId);
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setCohorts(annotationSummary.getResult());

        annotationSummary = individualDBAdaptor.getAnnotationSummary(variableSetId);
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setIndividuals(annotationSummary.getResult());

        return new QueryResult<>("Variable set summary", dbTime, 1, 1, "", "", Arrays.asList(variableSetSummary));
    }

    /*
     * Variables Methods
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                                      Map<String, Object> attributes, List<Variable> variables, String sessionId)
            throws CatalogException {

        ParamUtils.checkObj(variables, "Variables List");
        Set<Variable> variablesSet = new HashSet<>(variables);
        if (variables.size() != variablesSet.size()) {
            throw new CatalogException("Error. Repeated variables");
        }
        return createVariableSet(studyId, name, unique, description, attributes, variablesSet, sessionId);
    }

    @Override
    public QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                                      Map<String, Object> attributes, Set<Variable> variables, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(variables, "Variables Set");
        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_VARIABLE_SET);
        unique = ParamUtils.defaultObject(unique, true);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, new HashMap<String, Object>());

        for (Variable variable : variables) {
            ParamUtils.checkParameter(variable.getName(), "variable ID");
            ParamUtils.checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(ParamUtils.defaultObject(variable.getAllowedValues(), Collections.<String>emptyList()));
            variable.setAttributes(ParamUtils.defaultObject(variable.getAttributes(), Collections.<String, Object>emptyMap()));
            variable.setCategory(ParamUtils.defaultString(variable.getCategory(), ""));
            variable.setDependsOn(ParamUtils.defaultString(variable.getDependsOn(), ""));
            variable.setDescription(ParamUtils.defaultString(variable.getDescription(), ""));
            variable.setTitle(ParamUtils.defaultString(variable.getTitle(), ""));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }

        VariableSet variableSet = new VariableSet(-1, name, unique, description, variables, attributes);
        CatalogAnnotationsValidator.checkVariableSet(variableSet);

        QueryResult<VariableSet> queryResult = studyDBAdaptor.createVariableSet(studyId, variableSet);
//      auditManager.recordCreation(AuditRecord.Resource.variableSet, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.variableSet, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> readVariableSet(long variableSet, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSet);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET);
        return studyDBAdaptor.getVariableSet(variableSet, options);
    }

    @Override
    public QueryResult<VariableSet> searchVariableSets(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);
        query.put(StudyDBAdaptor.VariableSetParams.STUDY_ID.key(), studyId);
        return studyDBAdaptor.getVariableSets(query, options);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String sessionId) throws
            CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.DELETE_VARIABLE_SET);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.deleteVariableSet(variableSetId, queryOptions);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_VARIABLE_SET);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.addFieldToVariableSet(variableSetId, variable);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_VARIABLE_SET);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.removeFieldFromVariableSet(variableSetId, name);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> renameFieldFromVariableSet(long variableSetId, String oldName, String newName, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_VARIABLE_SET);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.renameFieldVariableSet(variableSetId, oldName, newName);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }
}
