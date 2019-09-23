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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authentication.LDAPUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.stats.solr.converters.*;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.*;
import org.opencb.opencga.core.models.summaries.StudySummary;
import org.opencb.opencga.core.models.summaries.VariableSetSummary;
import org.opencb.opencga.core.models.summaries.VariableSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.naming.NamingException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyManager extends AbstractManager {

    private static final String MEMBERS = "@members";
    private static final String ADMINS = "@admins";
    //[A-Za-z]([-_.]?[A-Za-z0-9]
    private static final String USER_PATTERN = "[A-Za-z][[-_.]?[A-Za-z0-9]?]*";
    private static final String PROJECT_PATTERN = "[A-Za-z0-9][[-_.]?[A-Za-z0-9]?]*";
    private static final String STUDY_PATTERN = "[A-Za-z0-9\\-_.]+|\\*";
    private static final Pattern USER_PROJECT_STUDY_PATTERN = Pattern.compile("^(" + USER_PATTERN + ")@(" + PROJECT_PATTERN + "):("
            + STUDY_PATTERN + ")$");
    private static final Pattern PROJECT_STUDY_PATTERN = Pattern.compile("^(" + PROJECT_PATTERN + "):(" + STUDY_PATTERN + ")$");

    static final QueryOptions INCLUDE_STUDY_UID = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UID.key());
    static final QueryOptions INCLUDE_STUDY_ID = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
            StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.UUID.key(), StudyDBAdaptor.QueryParams.FQN.key()));
    static final QueryOptions INCLUDE_VARIABLE_SET = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key());

    protected Logger logger;

    StudyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                 DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        logger = LoggerFactory.getLogger(StudyManager.class);
    }

    public String getProjectId(long studyId) throws CatalogException {
        return studyDBAdaptor.getProjectIdByStudyUid(studyId);
    }

    public List<Study> resolveIds(List<String> studyList, String userId) throws CatalogException {
        if (studyList == null || studyList.isEmpty() || (studyList.size() == 1 && studyList.get(0).endsWith("*"))) {
            String studyStr = "*";
            if (studyList != null && !studyList.isEmpty()) {
                studyStr = studyList.get(0);
            }

            return smartResolutor(studyStr, userId, null).getResult();
        }

        List<Study> returnList = new ArrayList<>(studyList.size());
        for (String study : studyList) {
            returnList.add(resolveId(study, userId));
        }
        return returnList;
    }

    public Study resolveId(String studyStr, String userId) throws CatalogException {
        return resolveId(studyStr, userId, null);
    }

    public Study resolveId(String studyStr, String userId, QueryOptions options) throws CatalogException {
        QueryResult<Study> studyQueryResult = smartResolutor(studyStr, userId, options);

        if (studyQueryResult.getNumResults() > 1) {
            String studyMessage = "";
            if (StringUtils.isNotEmpty(studyStr)) {
                studyMessage = " given '" + studyStr + "'";
            }
            throw new CatalogException("More than one study found" + studyMessage + ". Please, be more specific."
                    + " The accepted pattern is [ownerId@projectId:studyId]");
        }

        return studyQueryResult.first();
    }

    private QueryResult<Study> smartResolutor(String studyStr, String userId, QueryOptions options) throws CatalogException {
        String owner = null;
        String project = null;

        Query query = new Query();
        QueryOptions queryOptions;
        if (options == null) {
            queryOptions = new QueryOptions();
        } else {
            queryOptions = new QueryOptions(options);
        }

        if (StringUtils.isNotEmpty(studyStr)) {
            if (UUIDUtils.isOpenCGAUUID(studyStr)) {
                query.putIfNotEmpty(StudyDBAdaptor.QueryParams.UUID.key(), studyStr);
            } else {
                String study;

                Matcher matcher = USER_PROJECT_STUDY_PATTERN.matcher(studyStr);
                if (matcher.find()) {
                    // studyStr contains the full path (owner@project:study)
                    owner = matcher.group(1);
                    project = matcher.group(2);
                    study = matcher.group(3);
                } else {
                    matcher = PROJECT_STUDY_PATTERN.matcher(studyStr);
                    if (matcher.find()) {
                        // studyStr contains the path (project:study)
                        project = matcher.group(1);
                        study = matcher.group(2);
                    } else {
                        // studyStr only contains the study information
                        study = studyStr;
                    }
                }

                if (study.equals("*")) {
                    // If the user is asking for all the studies...
                    study = null;
                }

                query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), study);
            }
        }

        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.OWNER.key(), owner);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), project);
//        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ALIAS.key(), study);

        if (queryOptions.isEmpty()) {
            queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    StudyDBAdaptor.QueryParams.UUID.key(), StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.UID.key(),
                    StudyDBAdaptor.QueryParams.ALIAS.key(), StudyDBAdaptor.QueryParams.CREATION_DATE.key(),
                    StudyDBAdaptor.QueryParams.FQN.key(), StudyDBAdaptor.QueryParams.URI.key()
            ));
        } else {
            List<String> includeList = new ArrayList<>(queryOptions.getAsStringList(QueryOptions.INCLUDE));
            includeList.addAll(Arrays.asList(
                    StudyDBAdaptor.QueryParams.UUID.key(), StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.UID.key(),
                    StudyDBAdaptor.QueryParams.ALIAS.key(), StudyDBAdaptor.QueryParams.CREATION_DATE.key(),
                    StudyDBAdaptor.QueryParams.FQN.key(), StudyDBAdaptor.QueryParams.URI.key()));
            // We create a new object in case there was an exclude or any other field. We only want to include fields in this case
            queryOptions = new QueryOptions(QueryOptions.INCLUDE, includeList);
        }

        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, queryOptions, userId);

        if (studyQueryResult.getNumResults() == 0) {
            studyQueryResult = studyDBAdaptor.get(query, queryOptions);
            if (studyQueryResult.getNumResults() == 0) {
                String studyMessage = "";
                if (StringUtils.isNotEmpty(studyStr)) {
                    studyMessage = " given '" + studyStr + "'";
                }
                throw new CatalogException("No study found" + studyMessage + " or the user '" + userId
                        + "'  does not have permissions to view any.");
            } else {
                throw CatalogAuthorizationException.deny(userId, "view", "study", studyQueryResult.first().getFqn(), null);
            }
        }

        return studyQueryResult;
    }

    private QueryResult<Study> getStudy(long projectUid, String studyUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUid)
                .append(StudyDBAdaptor.QueryParams.UUID.key(), studyUuid);
        return studyDBAdaptor.get(query, options);
    }

    public QueryResult<Study> create(String projectStr, String id, String alias, String name, Study.Type type, String creationDate,
                                     String description, Status status, String cipher, String uriScheme, URI uri,
                                     Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats, Map<String, Object> attributes,
                                     QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkObj(type, "type");
        ParamUtils.checkAlias(id, "id");

        String userId = catalogManager.getUserManager().getUserId(token);
        Project project = catalogManager.getProjectManager().resolveId(projectStr, userId);

        long projectId = project.getUid();

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
        datastores = ParamUtils.defaultObject(datastores, HashMap::new);
        stats = ParamUtils.defaultObject(stats, HashMap::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap::new);

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uriScheme);

        ObjectMap auditParams = new ObjectMap()
                .append("id", id)
                .append("alias", alias)
                .append("name", name)
                .append("type", type)
                .append("creationDate", creationDate)
                .append("description", description)
                .append("status", status)
                .append("cipher", cipher)
                .append("uriScheme", uriScheme)
                .append("uri", uri)
                .append("datastores", datastores)
                .append("stats", stats)
                .append("attributes", attributes)
                .append("options", options)
                .append("token", token);

        try {
            /* Check project permissions */
            if (!project.getFqn().startsWith(userId + "@")) {
                throw new CatalogException("Permission denied: Only the owner of the project can create studies.");
            }

            if (uri == null) {
                try {
                    uri = catalogIOManager.getStudyURI(userId, project.getId(), id);
                } catch (CatalogIOException e) {
                    throw new CatalogException("Cannot create study: " + e.getMessage(), e);
                }
            }

            LinkedList<File> files = new LinkedList<>();
            File rootFile = new File(".", File.Type.DIRECTORY, null, null, "", uri, "study root folder",
                    new File.FileStatus(File.FileStatus.READY), 0, project.getCurrentRelease());
            files.add(rootFile);

            Study study = new Study(id, name, alias, type, creationDate, description, status, TimeUtils.getTime(),
                    0, cipher, Arrays.asList(new Group(MEMBERS, Collections.singletonList(userId)),
                    new Group(ADMINS, Collections.emptyList())), null, files, null, null, null, null, null, null, null, null, uri,
                    datastores, project.getCurrentRelease(), stats, attributes);

            /* CreateStudy */
            study.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.STUDY));
            studyDBAdaptor.insert(project, study, options);
            QueryResult<Study> result = getStudy(projectId, study.getUuid(), options);
            study = result.getResult().get(0);

            //URI studyUri;
            if (uri == null) {
                try {
                    catalogIOManager.createStudy(uri);
                } catch (CatalogIOException e) {
                    try {
                        studyDBAdaptor.delete(study.getUid());
                    } catch (Exception e1) {
                        logger.error("Can't delete study from DB -> {}, after failure creating study directories -> {}", e1.getMessage(),
                                e.getMessage(), e);
                        throw new CatalogException("Can't delete study from DB -> " + e1.getMessage() + ", after failure creating study "
                                + "directories -> " + e.getMessage(), e);
                    }
                    throw e;
                }
            }

            auditManager.auditCreate(userId, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            userDBAdaptor.updateUserLastModified(userId);

            result.setResult(Arrays.asList(study));
            return result;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, AuditRecord.Resource.STUDY, id, "", id, "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public int getCurrentRelease(Study study, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        authorizationManager.checkCanViewStudy(study.getUid(), userId);
        return getCurrentRelease(study);
    }

    int getCurrentRelease(Study study) throws CatalogException {
        String[] split = StringUtils.split(study.getFqn(), ":");
        String userId = StringUtils.split(split[0], "@")[0];
        return catalogManager.getProjectManager().resolveId(split[0], userId).getCurrentRelease();
    }

    MyResourceId getVariableSetId(String variableStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(variableStr)) {
            throw new CatalogException("Missing variableSet parameter");
        }

        String userId;
        long studyId;
        long variableSetId;

        if (StringUtils.isNumeric(variableStr) && Long.parseLong(variableStr) > configuration.getCatalog().getOffset()) {
            variableSetId = Long.parseLong(variableStr);
            Query query = new Query(StudyDBAdaptor.QueryParams.VARIABLE_SET_UID.key(), variableSetId);
            QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE,
                    StudyDBAdaptor.QueryParams.UID.key()));
            if (studyQueryResult.getNumResults() == 0) {
                throw new CatalogException("Variable set " + variableStr + " not found");
            }
            studyId = studyQueryResult.first().getUid();
            userId = catalogManager.getUserManager().getUserId(sessionId);
        } else {
            if (variableStr.contains(",")) {
                throw new CatalogException("More than one variable set found. Please, choose just one variable set");
            }

            userId = catalogManager.getUserManager().getUserId(sessionId);
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            studyId = study.getUid();

            Query query = new Query()
                    .append(StudyDBAdaptor.VariableSetParams.STUDY_UID.key(), study.getUid())
                    .append(StudyDBAdaptor.VariableSetParams.ID.key(), variableStr);
            QueryOptions queryOptions = new QueryOptions();
            QueryResult<VariableSet> variableSetQueryResult = studyDBAdaptor.getVariableSets(query, queryOptions);
            if (variableSetQueryResult.getNumResults() == 0) {
                throw new CatalogException("Variable set " + variableStr + " not found in study " + studyStr);
            } else if (variableSetQueryResult.getNumResults() > 1) {
                throw new CatalogException("More than one variable set found under " + variableStr + " in study " + studyStr);
            }
            variableSetId = variableSetQueryResult.first().getUid();
        }

        return new MyResourceId(userId, studyId, variableSetId);
    }

    /**
     * Fetch a study from Catalog given a study id or alias.
     *
     * @param studyStr  Study id or alias.
     * @param options   Read options
     * @param token sessionId
     * @return The specified object
     * @throws CatalogException CatalogException
     */
    public QueryResult<Study> get(String studyStr, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(token);
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("options", options)
                .append("token", token);
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

            Query query = new Query(StudyDBAdaptor.QueryParams.UID.key(), study.getUid());
            QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, options, userId);
            if (studyQueryResult.getNumResults() <= 0) {
                throw CatalogAuthorizationException.deny(userId, "view", "study", study.getFqn(), "");
            }
            auditManager.auditInfo(userId, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return studyQueryResult;
        } catch (CatalogException e) {
            auditManager.auditInfo(userId, AuditRecord.Resource.STUDY, studyStr, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public List<QueryResult<Study>> get(List<String> studyList, QueryOptions queryOptions, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<Study>> results = new ArrayList<>(studyList.size());
        for (String study : studyList) {
            try {
                QueryResult<Study> studyObj = get(study, queryOptions, sessionId);
                results.add(studyObj);
            } catch (CatalogException e) {
                if (silent) {
                    results.add(new QueryResult<>(study, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return results;
    }

    /**
     * Fetch all the study objects matching the query.
     *
     * @param projectStr Project id or alias.
     * @param query      Query to catalog.
     * @param options    Query options, like "include", "exclude", "limit" and "skip"
     * @param sessionId  sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    public QueryResult<Study> get(String projectStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(projectStr, "project");
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        String auxProject = null;
        String auxOwner = null;
        if (StringUtils.isNotEmpty(projectStr)) {
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

        query.putIfNotNull(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), auxProject);
        query.putIfNotNull(StudyDBAdaptor.QueryParams.OWNER.key(), auxOwner);

        return get(query, options, sessionId);
    }

    public List<QueryResult<Study>> get(List<String> projectList, Query query, QueryOptions options, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<Study>> results = new ArrayList<>(projectList.size());
        for (String project : projectList) {
            try {
                QueryResult<Study> studyObj = get(project, query, options, sessionId);
                results.add(studyObj);
            } catch (CatalogException e) {
                if (silent) {
                    results.add(new QueryResult<>(project, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return results;
    }


    /**
     * Fetch all the study objects matching the query.
     *
     * @param query     Query to catalog.
     * @param options   Query options, like "include", "exclude", "limit" and "skip"
     * @param token sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    public QueryResult<Study> get(Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        QueryOptions qOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        String userId = catalogManager.getUserManager().getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);
        if (!qOptions.containsKey("include") || qOptions.get("include") == null || qOptions.getAsStringList("include").isEmpty()) {
            qOptions.addToListOption("exclude", "projects.studies.attributes.studyConfiguration");
        }

        try {
            QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, qOptions, userId);
            auditManager.auditSearch(userId, AuditRecord.Resource.STUDY, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return studyQueryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, AuditRecord.Resource.STUDY, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Update an existing catalog study.
     *
     * @param studyId   Study id or alias.
     * @param parameters Parameters to change.
     * @param options    options
     * @param token  sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    public QueryResult<Study> update(String studyId, ObjectMap parameters, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("updateParams", parameters)
                .append("options", options)
                .append("token", token);
        Study study;
        try {
            study = resolveId(studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, AuditRecord.Resource.STUDY, studyId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            ParamUtils.checkObj(parameters, "Parameters");

            authorizationManager.checkCanEditStudy(study.getUid(), userId);

            for (String s : parameters.keySet()) {
                if (!s.matches("id|alias|name|type|description|attributes|stats")) {
                    throw new CatalogDBException("Parameter '" + s + "' can't be changed");
                }
            }

            if (parameters.containsKey("id")) {
                ParamUtils.checkAlias(parameters.getString("id"), "id");
            }
            if (parameters.containsKey("alias")) {
                ParamUtils.checkAlias(parameters.getString("alias"), "alias");
            }

            String ownerId = getOwner(study);
            userDBAdaptor.updateUserLastModified(ownerId);
            WriteResult result = studyDBAdaptor.update(study.getUid(), parameters, options);
            auditManager.auditUpdate(userId, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            QueryResult<Study> queryResult = studyDBAdaptor.get(study.getUid(), options);
            queryResult.setDbTime(queryResult.getDbTime() + result.getDbTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<PermissionRule> createPermissionRule(String studyId, Study.Entity entry, PermissionRule permissionRule,
                                                            String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entry", entry)
                .append("permissionRule", permissionRule)
                .append("token", token);
        try {
            ParamUtils.checkObj(entry, "entry");
            ParamUtils.checkObj(permissionRule, "permission rule");

            authorizationManager.checkCanUpdatePermissionRules(study.getUid(), userId);
            validatePermissionRules(study.getUid(), entry, permissionRule);

            studyDBAdaptor.createPermissionRule(study.getUid(), entry, permissionRule);

            auditManager.audit(userId, AuditRecord.Action.ADD_STUDY_PERMISSION_RULE, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new QueryResult<>(study.getFqn(), -1, 1, 1, "", "", Collections.singletonList(permissionRule));
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.ADD_STUDY_PERMISSION_RULE, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public void markDeletedPermissionRule(String studyId, Study.Entity entry, String permissionRuleId,
                                          PermissionRule.DeleteAction deleteAction, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entry", entry)
                .append("permissionRuleId", permissionRuleId)
                .append("deleteAction", deleteAction)
                .append("token", token);
        try {
            ParamUtils.checkObj(entry, "entry");
            ParamUtils.checkObj(deleteAction, "Delete action");
            ParamUtils.checkObj(permissionRuleId, "permission rule id");

            authorizationManager.checkCanUpdatePermissionRules(study.getUid(), userId);
            studyDBAdaptor.markDeletedPermissionRule(study.getUid(), entry, permissionRuleId, deleteAction);

            auditManager.audit(userId, AuditRecord.Action.REMOVE_STUDY_PERMISSION_RULE, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.REMOVE_STUDY_PERMISSION_RULE, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<PermissionRule> getPermissionRules(String studyId, Study.Entity entry, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entry", entry)
                .append("token", token);
        try {
            authorizationManager.checkCanViewStudy(study.getUid(), userId);
            QueryResult<PermissionRule> result = studyDBAdaptor.getPermissionRules(study.getUid(), entry);

            auditManager.audit(userId, AuditRecord.Action.FETCH_STUDY_PERMISSION_RULES, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.FETCH_STUDY_PERMISSION_RULES, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult rank(long projectId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(projectId, "projectId");

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        authorizationManager.checkCanViewProject(projectId, userId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult groupBy(long projectId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        return groupBy(projectId, query, Collections.singletonList(field), options, sessionId);
    }

    public QueryResult groupBy(long projectId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(projectId, "projectId");

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        authorizationManager.checkCanViewProject(projectId, userId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult<StudySummary> getSummary(String studyStr, QueryOptions queryOptions, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();

        Study study = get(studyStr, queryOptions, sessionId).first();

        StudySummary studySummary = new StudySummary()
                .setAlias(study.getId())
                .setAttributes(study.getAttributes())
                .setCipher(study.getCipher())
                .setCreationDate(study.getCreationDate())
                .setDatasets(study.getDatasets().size())
                .setDescription(study.getDescription())
                .setDiskUsage(study.getSize())
                .setExperiments(study.getExperiments())
                .setGroups(study.getGroups())
                .setName(study.getName())
                .setStats(study.getStats())
                .setStatus(study.getStatus())
                .setType(study.getType())
                .setVariableSets(study.getVariableSets());

        Long nFiles = fileDBAdaptor.count(
                new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.FILE)
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setFiles(nFiles);

        Long nSamples = sampleDBAdaptor.count(
                new Query(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setSamples(nSamples);

        Long nJobs = jobDBAdaptor.count(
                new Query(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setJobs(nJobs);

        Long nCohorts = cohortDBAdaptor.count(
                new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setCohorts(nCohorts);

        Long nIndividuals = individualDBAdaptor.count(
                new Query(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(IndividualDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                                + File.FileStatus.DELETED))
                .first();
        studySummary.setIndividuals(nIndividuals);

        return new QueryResult<>("Study summary", (int) (System.currentTimeMillis() - startTime), 1, 1, "", "",
                Collections.singletonList(studySummary));
    }

    public List<QueryResult<StudySummary>> getSummary(List<String> studyList, QueryOptions queryOptions, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<StudySummary>> results = new ArrayList<>(studyList.size());
        for (String aStudyList : studyList) {
            try {
                QueryResult<StudySummary> summaryObj = getSummary(aStudyList, queryOptions, sessionId);
                results.add(summaryObj);
            } catch (CatalogException e) {
                if (silent) {
                    results.add(new QueryResult<>(aStudyList, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return results;
    }

    public QueryResult<Group> createGroup(String studyStr, String groupId, String groupName, String users, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(groupId, "group id");
        String name = StringUtils.isEmpty(groupName) ? groupId : groupName;

        List<String> userList = StringUtils.isNotEmpty(users) ? Arrays.asList(users.split(",")) : Collections.emptyList();
        return createGroup(studyStr, new Group(groupId, name, userList, null), sessionId);
    }

    public QueryResult<Group> createGroup(String studyId, Group group, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("group", group)
                .append("token", token);
        try {
            ParamUtils.checkObj(group, "group");
            ParamUtils.checkGroupId(group.getId());

            if (group.getSyncedFrom() != null) {
                ParamUtils.checkParameter(group.getSyncedFrom().getAuthOrigin(), "Authentication origin");
                ParamUtils.checkParameter(group.getSyncedFrom().getRemoteGroup(), "Remote group id");
            }

            // Fix the group id
            if (!group.getId().startsWith("@")) {
                group.setId("@" + group.getId());
            }

            authorizationManager.checkCreateDeleteGroupPermissions(study.getUid(), userId, group.getId());

            // Check group exists
            if (existsGroup(study.getUid(), group.getId())) {
                throw new CatalogException("The group " + group.getId() + " already exists.");
            }

            List<String> users = group.getUserIds();
            if (ListUtils.isNotEmpty(users)) {
                // We remove possible duplicates
                users = users.stream().collect(Collectors.toSet()).stream().collect(Collectors.toList());
                userDBAdaptor.checkIds(users);
                group.setUserIds(users);
            } else {
                users = Collections.emptyList();
            }

            // Add those users to the members group
            if (ListUtils.isNotEmpty(users)) {
                studyDBAdaptor.addUsersToGroup(study.getUid(), MEMBERS, users);
            }

            // Create the group
            WriteResult result = studyDBAdaptor.createGroup(study.getUid(), group);

            QueryResult<Group> queryResult = studyDBAdaptor.getGroup(study.getUid(), group.getId(), null);
            queryResult.setDbTime(queryResult.getDbTime() + result.getDbTime());

            auditManager.audit(userId, AuditRecord.Action.ADD_STUDY_GROUP, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.ADD_STUDY_GROUP, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<Group> getGroup(String studyId, String groupId, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("token", token);
        try {
            authorizationManager.checkCanViewStudy(study.getUid(), userId);

            // Fix the groupId
            if (groupId != null && !groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            QueryResult<Group> result = studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
            auditManager.audit(userId, AuditRecord.Action.FETCH_STUDY_GROUPS, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.FETCH_STUDY_GROUPS, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public List<QueryResult<Group>> getGroup(List<String> studyList, String groupId, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<Group>> results = new ArrayList<>(studyList.size());
        for (String aStudyList : studyList) {
            try {
                QueryResult<Group> groupObj = getGroup(aStudyList, groupId, sessionId);
                results.add(groupObj);
            } catch (CatalogException e) {
                if (silent) {
                    results.add(new QueryResult<>(aStudyList, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return results;
    }

    public QueryResult<Group> updateGroup(String studyId, String groupId, GroupParams groupParams, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("groupParams", groupParams)
                .append("token", token);
        try {
            ParamUtils.checkObj(groupParams, "Group parameters");
            ParamUtils.checkParameter(groupId, "Group name");
            ParamUtils.checkObj(groupParams.getAction(), "Action");

            // Fix the group name
            if (!groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            authorizationManager.checkUpdateGroupPermissions(study.getUid(), userId, groupId, groupParams);

            List<String> users;
            if (StringUtils.isNotEmpty(groupParams.getUsers())) {
                users = Arrays.asList(groupParams.getUsers().split(","));
                List<String> tmpUsers = users;
                if (groupId.equals(MEMBERS) || groupId.equals(ADMINS)) {
                    // Remove anonymous user if present for the checks.
                    // Anonymous user is only allowed in MEMBERS group, otherwise we keep it as if it is present it should fail.
                    tmpUsers = users.stream().filter(user -> !user.equals(ANONYMOUS)).collect(Collectors.toList());
                }
                if (tmpUsers.size() > 0) {
                    userDBAdaptor.checkIds(tmpUsers);
                }
            } else {
                users = Collections.emptyList();
            }

            switch (groupParams.getAction()) {
                case SET:
                    if (MEMBERS.equals(groupId)) {
                        throw new CatalogException("Operation not valid. Valid actions over the '@members' group are ADD or REMOVE.");
                    }
                    studyDBAdaptor.setUsersToGroup(study.getUid(), groupId, users);
                    studyDBAdaptor.addUsersToGroup(study.getUid(), MEMBERS, users);
                    break;
                case ADD:
                    studyDBAdaptor.addUsersToGroup(study.getUid(), groupId, users);
                    if (!MEMBERS.equals(groupId)) {
                        studyDBAdaptor.addUsersToGroup(study.getUid(), MEMBERS, users);
                    }
                    break;
                case REMOVE:
                    if (MEMBERS.equals(groupId)) {
                        // Check we are not trying to remove the owner of the study from the group
                        String owner = getOwner(study);
                        if (users.contains(owner)) {
                            throw new CatalogException("Cannot remove owner of the study from the '@members' group");
                        }

                        // We remove the users from all the groups and acls
                        authorizationManager.resetPermissionsFromAllEntities(study.getUid(), users);
                        studyDBAdaptor.removeUsersFromAllGroups(study.getUid(), users);
                    } else {
                        studyDBAdaptor.removeUsersFromGroup(study.getUid(), groupId, users);
                    }
                    break;
                default:
                    throw new CatalogException("Unknown action " + groupParams.getAction() + " found.");
            }

            auditManager.audit(userId, AuditRecord.Action.UPDATE_USERS_FROM_STUDY_GROUP, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.UPDATE_USERS_FROM_STUDY_GROUP, AuditRecord.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<Group> syncGroupWith(String studyStr, String externalGroup, String catalogGroup, String authenticationOriginId,
                                            boolean force, String token) throws CatalogException {
        if (!ROOT.equals(catalogManager.getUserManager().getUserId(token))) {
            throw new CatalogAuthorizationException("Only the root of OpenCGA can synchronise groups");
        }

        ParamUtils.checkObj(studyStr, "study");
        ParamUtils.checkObj(externalGroup, "external group");
        ParamUtils.checkObj(catalogGroup, "catalog group");
        ParamUtils.checkObj(authenticationOriginId, "authentication origin");

        AuthenticationOrigin authenticationOrigin = getAuthenticationOrigin(authenticationOriginId);
        if (authenticationOrigin == null) {
            throw new CatalogException("Authentication origin " + authenticationOriginId + " not found");
        }

        try {
            String base = ((String) authenticationOrigin.getOptions().get(AuthenticationOrigin.GROUPS_SEARCH));
            if (!LDAPUtils.existsLDAPGroup(authenticationOrigin.getHost(), externalGroup, base)) {
                throw new CatalogException("Group " + externalGroup + " not found in origin " + authenticationOriginId);
            }
        } catch (NamingException e) {
            logger.error("{}", e.getMessage(), e);
            throw new CatalogException("Unexpected LDAP error: " + e.getMessage());
        }

        Study study = resolveId(studyStr, ROOT);

        // Fix the groupId
        if (!catalogGroup.startsWith("@")) {
            catalogGroup = "@" + catalogGroup;
        }

        QueryResult<Group> group = studyDBAdaptor.getGroup(study.getUid(), catalogGroup, Collections.emptyList());
        if (group.getNumResults() == 1) {
            if (group.first().getSyncedFrom() != null && StringUtils.isNotEmpty(group.first().getSyncedFrom().getAuthOrigin())
                    && StringUtils.isNotEmpty(group.first().getSyncedFrom().getRemoteGroup())) {
                if (authenticationOriginId.equals(group.first().getSyncedFrom().getAuthOrigin())
                        && externalGroup.equals(group.first().getSyncedFrom().getRemoteGroup())) {
                    // It is already synced with that group from that authentication origin
                    return group;
                } else {
                    throw new CatalogException("The group " + catalogGroup + " is already synced with the group " + externalGroup + " "
                            + "from " + authenticationOriginId + ". If you still want to sync the group with the new external group, "
                            + "please use the force parameter.");
                }
            }

            if (!force) {
                throw new CatalogException("Cannot sync the group " + catalogGroup + " because it already exist in Catalog. Please, use "
                        + "force parameter if you still want sync it.");
            }

            // We remove all the users belonging to that group and resync it with the new external group
            studyDBAdaptor.removeUsersFromGroup(study.getUid(), catalogGroup, group.first().getUserIds());
            studyDBAdaptor.syncGroup(study.getUid(), catalogGroup, new Group.Sync(authenticationOriginId, externalGroup));
        } else {
            // We need to create a new group
            Group newGroup = new Group(catalogGroup, catalogGroup, Collections.emptyList(), new Group.Sync(authenticationOriginId,
                    externalGroup));
            studyDBAdaptor.createGroup(study.getUid(), newGroup);
        }

        return studyDBAdaptor.getGroup(study.getUid(), catalogGroup, Collections.emptyList());
    }


    public QueryResult<Group> syncGroupWith(String studyStr, String groupId, Group.Sync syncedFrom, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(syncedFrom, "sync");

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = resolveId(studyStr, userId);

        if (StringUtils.isEmpty(groupId)) {
            throw new CatalogException("Missing group name parameter");
        }

        // Fix the groupId
        if (!groupId.startsWith("@")) {
            groupId = "@" + groupId;
        }

        authorizationManager.checkSyncGroupPermissions(study.getUid(), userId, groupId);

        QueryResult<Group> group = studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
        if (group.first().getSyncedFrom() != null && StringUtils.isNotEmpty(group.first().getSyncedFrom().getAuthOrigin())
                && StringUtils.isNotEmpty(group.first().getSyncedFrom().getRemoteGroup())) {
            throw new CatalogException("Cannot modify already existing sync information.");
        }

        // Check the group exists
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.UID.key(), study.getUid())
                .append(StudyDBAdaptor.QueryParams.GROUP_ID.key(), groupId);
        if (studyDBAdaptor.count(query).first() == 0) {
            throw new CatalogException("The group " + groupId + " does not exist.");
        }

        studyDBAdaptor.syncGroup(study.getUid(), groupId, syncedFrom);

        return studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
    }

    public QueryResult<Group> deleteGroup(String studyId, String groupId, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("token", token);
        try {
            if (StringUtils.isEmpty(groupId)) {
                throw new CatalogException("Missing group id");
            }

            // Fix the groupId
            if (!groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            authorizationManager.checkCreateDeleteGroupPermissions(study.getUid(), userId, groupId);

            QueryResult<Group> group = studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
            group.setId("Delete group");

            // Remove the permissions the group might have had
            Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
            updateAcl(Collections.singletonList(studyId), groupId, aclParams, token);

            studyDBAdaptor.deleteGroup(study.getUid(), groupId);

            auditManager.audit(userId, AuditRecord.Action.REMOVE_STUDY_GROUP, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return group;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.REMOVE_STUDY_GROUP, AuditRecord.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<VariableSetSummary> getVariableSetSummary(String studyStr, String variableSetStr, String sessionId)
            throws CatalogException {
        MyResourceId resource = getVariableSetId(variableSetStr, studyStr, sessionId);

        String userId = resource.getUser();

        QueryResult<VariableSet> variableSet = studyDBAdaptor.getVariableSet(resource.getResourceId(), new QueryOptions(), userId);
        if (variableSet.getNumResults() == 0) {
            logger.error("getVariableSetSummary: Could not find variable set id {}. {} results returned", variableSetStr,
                    variableSet.getNumResults());
            throw new CatalogDBException("Variable set " + variableSetStr + " not found.");
        }

        int dbTime = 0;

        VariableSetSummary variableSetSummary = new VariableSetSummary(resource.getResourceId(), variableSet.first().getId());

        QueryResult<VariableSummary> annotationSummary = sampleDBAdaptor.getAnnotationSummary(resource.getStudyId(),
                resource.getResourceId());
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setSamples(annotationSummary.getResult());

        annotationSummary = cohortDBAdaptor.getAnnotationSummary(resource.getStudyId(), resource.getResourceId());
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setCohorts(annotationSummary.getResult());

        annotationSummary = individualDBAdaptor.getAnnotationSummary(resource.getStudyId(), resource.getResourceId());
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setIndividuals(annotationSummary.getResult());

        annotationSummary = familyDBAdaptor.getAnnotationSummary(resource.getStudyId(), resource.getResourceId());
        dbTime += annotationSummary.getDbTime();
        variableSetSummary.setFamilies(annotationSummary.getResult());

        return new QueryResult<>("Variable set summary", dbTime, 1, 1, "", "", Arrays.asList(variableSetSummary));
    }


    /*
     * Variables Methods
     */
    QueryResult<VariableSet> createVariableSet(Study study, String id, String name, Boolean unique, Boolean confidential,
                                               String description, Map<String, Object> attributes, List<Variable> variables,
                                               List<VariableSet.AnnotableDataModels> entities, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkObj(variables, "Variables from VariableSet");
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        authorizationManager.checkCanCreateUpdateDeleteVariableSets(study.getUid(), userId);

        unique = ParamUtils.defaultObject(unique, true);
        confidential = ParamUtils.defaultObject(confidential, false);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, new HashMap<>());
        entities = ParamUtils.defaultObject(entities, Collections.emptyList());

        for (Variable variable : variables) {
            ParamUtils.checkParameter(variable.getId(), "variable ID");
            ParamUtils.checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(ParamUtils.defaultObject(variable.getAllowedValues(), Collections.emptyList()));
            variable.setAttributes(ParamUtils.defaultObject(variable.getAttributes(), Collections.emptyMap()));
            variable.setCategory(ParamUtils.defaultString(variable.getCategory(), ""));
            variable.setDependsOn(ParamUtils.defaultString(variable.getDependsOn(), ""));
            variable.setDescription(ParamUtils.defaultString(variable.getDescription(), ""));
            variable.setName(ParamUtils.defaultString(variable.getName(), variable.getId()));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }

        Set<Variable> variablesSet = new HashSet<>(variables);
        if (variablesSet.size() < variables.size()) {
            throw new CatalogException("Error. Repeated variables");
        }

        VariableSet variableSet = new VariableSet(id, name, unique, confidential, description, variablesSet, entities,
                getCurrentRelease(study), attributes);
        AnnotationUtils.checkVariableSet(variableSet);

        WriteResult result = studyDBAdaptor.createVariableSet(study.getUid(), variableSet);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.getVariableSet(study.getUid(), variableSet.getId(), QueryOptions.empty());

        queryResult.setDbTime(queryResult.getDbTime() + result.getDbTime());

        return queryResult;
    }

    public QueryResult<VariableSet> createVariableSet(String studyId, String id, String name, Boolean unique, Boolean confidential,
                                                      String description, Map<String, Object> attributes, List<Variable> variables,
                                                      List<VariableSet.AnnotableDataModels> entities, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("id", id)
                .append("name", name)
                .append("unique", unique)
                .append("confidential", confidential)
                .append("description", description)
                .append("attributes", attributes)
                .append("variables", variables)
                .append("entities", entities)
                .append("token", token);
        try {
            QueryResult<VariableSet> queryResult = createVariableSet(study, id, name, unique, confidential, description, attributes,
                    variables, entities, token);
            auditManager.audit(userId, AuditRecord.Action.ADD_VARIABLE_SET, AuditRecord.Resource.STUDY, queryResult.first().getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.ADD_VARIABLE_SET, AuditRecord.Resource.STUDY, id, "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<VariableSet> getVariableSet(String studyId, String variableSetId, QueryOptions options, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("variableSetId", variableSetId)
                .append("options", options)
                .append("token", token);
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            VariableSet variableSet = extractVariableSet(study, variableSetId, userId);
            QueryResult<VariableSet> result = studyDBAdaptor.getVariableSet(variableSet.getUid(), options, userId);

            auditManager.audit(userId, AuditRecord.Action.FETCH_VARIABLE_SET, AuditRecord.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.FETCH_VARIABLE_SET, AuditRecord.Resource.STUDY, variableSetId, "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<VariableSet> searchVariableSets(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = resolveId(studyStr, userId);
//        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);
        if (query.containsKey(StudyDBAdaptor.VariableSetParams.UID.key())) {
            // Id could be either the id or the name
            MyResourceId resource = getVariableSetId(query.getString(StudyDBAdaptor.VariableSetParams.UID.key()), studyStr, sessionId);
            query.put(StudyDBAdaptor.VariableSetParams.UID.key(), resource.getResourceId());
        }
        query.put(StudyDBAdaptor.VariableSetParams.STUDY_UID.key(), study.getUid());
        return studyDBAdaptor.getVariableSets(query, options, userId);
    }

    public QueryResult<VariableSet> deleteVariableSet(String studyId, String variableSetId, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId, StudyManager.INCLUDE_VARIABLE_SET);
        VariableSet variableSet = extractVariableSet(study, variableSetId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("variableSetId", variableSetId)
                .append("token", token);
        try {
            authorizationManager.checkCanCreateUpdateDeleteVariableSets(study.getUid(), userId);
            WriteResult writeResult = studyDBAdaptor.deleteVariableSet(variableSet.getUid(), QueryOptions.empty(), userId);
            auditManager.audit(userId, AuditRecord.Action.DELETE_VARIABLE_SET, AuditRecord.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new QueryResult("", writeResult.getDbTime(), 0, 0, "", "", Collections.emptyList());
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.DELETE_VARIABLE_SET, AuditRecord.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<VariableSet> addFieldToVariableSet(String studyId, String variableSetId, Variable variable, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId, StudyManager.INCLUDE_VARIABLE_SET);
        VariableSet variableSet = extractVariableSet(study, variableSetId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("variableSetId", variableSetId)
                .append("variable", variable)
                .append("token", token);
        try {
            if (StringUtils.isEmpty(variable.getId())) {
                if (StringUtils.isEmpty(variable.getName())) {
                    throw new CatalogException("Missing variable id");
                }
                variable.setId(variable.getName());
            }

            authorizationManager.checkCanCreateUpdateDeleteVariableSets(study.getUid(), userId);
            WriteResult result = studyDBAdaptor.addFieldToVariableSet(variableSet.getUid(), variable, userId);
            auditManager.audit(userId, AuditRecord.Action.ADD_VARIABLE_TO_VARIABLE_SET, AuditRecord.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            QueryResult<VariableSet> queryResult = studyDBAdaptor.getVariableSet(variableSet.getUid(), QueryOptions.empty());
            queryResult.setDbTime(queryResult.getDbTime() + result.getDbTime());
            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.ADD_VARIABLE_TO_VARIABLE_SET, AuditRecord.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public QueryResult<VariableSet> removeFieldFromVariableSet(String studyId, String variableSetId, String variableId, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = resolveId(studyId, userId, StudyManager.INCLUDE_VARIABLE_SET);
        VariableSet variableSet = extractVariableSet(study, variableSetId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("variableSetId", variableSetId)
                .append("variableId", variableId)
                .append("token", token);

        try {
            authorizationManager.checkCanCreateUpdateDeleteVariableSets(study.getUid(), userId);
            WriteResult result = studyDBAdaptor.removeFieldFromVariableSet(variableSet.getUid(), variableId, userId);
            auditManager.audit(userId, AuditRecord.Action.REMOVE_VARIABLE_FROM_VARIABLE_SET, AuditRecord.Resource.STUDY,
                    variableSet.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            QueryResult<VariableSet> queryResult = studyDBAdaptor.getVariableSet(variableSet.getUid(), QueryOptions.empty());
            queryResult.setDbTime(queryResult.getDbTime() + result.getDbTime());
            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.REMOVE_VARIABLE_FROM_VARIABLE_SET, AuditRecord.Resource.STUDY,
                    variableSet.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private VariableSet extractVariableSet(Study study, String variableSetId, String userId) throws CatalogException {
        if (study == null || study.getVariableSets() == null || study.getVariableSets().isEmpty()) {
            throw new CatalogException(variableSetId + " not found.");
        }
        if (StringUtils.isEmpty(variableSetId)) {
            throw new CatalogException("Missing 'variableSetId' variable");
        }

        Query query = new Query(StudyDBAdaptor.VariableSetParams.STUDY_UID.key(), study.getUid())
                .append(StudyDBAdaptor.VariableSetParams.ID.key(), variableSetId);

        QueryResult<VariableSet> queryResult = studyDBAdaptor.getVariableSets(query, new QueryOptions(), userId);
        if (queryResult.getNumResults() == 0) {
            throw new CatalogException(variableSetId + " not found.");
        }
        return queryResult.first();
    }

    public QueryResult<VariableSet> renameFieldFromVariableSet(String studyStr, String variableSetStr, String oldName, String newName,
                                                               String sessionId) throws CatalogException {
        throw new UnsupportedOperationException("Operation not yet supported");

//        MyResourceId resource = getVariableSetId(variableSetStr, studyStr, sessionId);
//        String userId = resource.getUser();
//
//        authorizationManager.checkCanCreateUpdateDeleteVariableSets(resource.getStudyId(), userId);
//        QueryResult<VariableSet> queryResult = studyDBAdaptor.renameFieldVariableSet(resource.getResourceId(), oldName, newName, userId);
//        auditManager.recordDeletion(AuditRecord.Resource.variableSet, resource.getResourceId(), userId, queryResult.first(), null, null);
//        return queryResult;
    }


    // **************************   ACLs  ******************************** //
    public List<QueryResult<StudyAclEntry>> getAcls(List<String> studyIdList, String member, boolean silent, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        List<Study> studyList = resolveIds(studyIdList, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyIdList", studyIdList)
                .append("member", member)
                .append("silent", silent)
                .append("token", token);
        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        List<QueryResult<StudyAclEntry>> studyAclList = new ArrayList<>(studyList.size());

        for (int i = 0; i < studyList.size(); i++) {
            Study study = studyList.get(i);
            long studyId = study.getUid();
            try {
                QueryResult<StudyAclEntry> allStudyAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allStudyAcls = authorizationManager.getStudyAcl(userId, studyId, member);
                } else {
                    allStudyAcls = authorizationManager.getAllStudyAcls(userId, studyId);
                }
                allStudyAcls.setId(study.getFqn());
                studyAclList.add(allStudyAcls);

                auditManager.audit(operationUuid, userId, AuditRecord.Action.FETCH_ACLS, AuditRecord.Resource.STUDY, study.getId(),
                        study.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            } catch (CatalogException e) {
                auditManager.audit(operationUuid, userId, AuditRecord.Action.FETCH_ACLS, AuditRecord.Resource.STUDY, study.getId(),
                        study.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                if (silent) {
                    studyAclList.add(new QueryResult<>(study.getFqn(), 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return studyAclList;
    }

    public List<QueryResult<StudyAclEntry>> updateAcl(List<String> studyIdList, String memberIds, Study.StudyAclParams aclParams,
                                                      String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        List<Study> studies = resolveIds(studyIdList, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyIdList", studyIdList)
                .append("memberIds", memberIds)
                .append("aclParams", aclParams)
                .append("token", token);
        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);
        try {
            if (studyIdList == null || studyIdList.isEmpty()) {
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
                EnumSet<StudyAclEntry.StudyPermissions> studyPermissions;
                switch (aclParams.getTemplate()) {
                    case AuthorizationManager.ROLE_ADMIN:
                        studyPermissions = AuthorizationManager.getAdminAcls();
                        break;
                    case AuthorizationManager.ROLE_ANALYST:
                        studyPermissions = AuthorizationManager.getAnalystAcls();
                        break;
                    case AuthorizationManager.ROLE_VIEW_ONLY:
                        studyPermissions = AuthorizationManager.getViewOnlyAcls();
                        break;
                    default:
                        studyPermissions = null;
                        break;
                }

                if (studyPermissions != null) {
                    // Merge permissions from the template with the ones written
                    Set<String> uniquePermissions = new HashSet<>(permissions);

                    for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
                        uniquePermissions.add(studyPermission.toString());
                    }

                    permissions = new ArrayList<>(uniquePermissions);
                }
            }

            // Check the user has the permissions needed to change permissions
            for (Study study : studies) {
                authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);
            }

            // Validate that the members are actually valid members
            List<String> members;
            if (memberIds != null && !memberIds.isEmpty()) {
                members = Arrays.asList(memberIds.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            for (Study study : studies) {
                checkMembers(study.getUid(), members);
            }

            List<QueryResult<StudyAclEntry>> aclResult;
            switch (aclParams.getAction()) {
                case SET:
                    aclResult = authorizationManager.setStudyAcls(studies.
                                    stream()
                                    .map(Study::getUid)
                                    .collect(Collectors.toList()),
                            members, permissions);
                    break;
                case ADD:
                    aclResult = authorizationManager.addStudyAcls(studies
                                    .stream()
                                    .map(Study::getUid)
                                    .collect(Collectors.toList()),
                            members, permissions);
                    break;
                case REMOVE:
                    aclResult = authorizationManager.removeStudyAcls(studies
                                    .stream()
                                    .map(Study::getUid)
                                    .collect(Collectors.toList()),
                            members, permissions);
                    break;
                case RESET:
                    aclResult = new ArrayList<>(studies.size());
                    for (Study study : studies) {
                        authorizationManager.resetPermissionsFromAllEntities(study.getUid(), members);
                        aclResult.add(authorizationManager.getAllStudyAcls(userId, study.getUid()));
                    }
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Study study : studies) {
                auditManager.audit(operationUuid, userId, AuditRecord.Action.UPDATE_ACLS, AuditRecord.Resource.STUDY, study.getId(),
                        study.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return aclResult;
        } catch (CatalogException e) {
            for (Study study : studies) {
                auditManager.audit(operationUuid, userId, AuditRecord.Action.UPDATE_ACLS, AuditRecord.Resource.STUDY, study.getId(),
                        study.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
            }

            throw e;
        }
    }

    public boolean indexCatalogIntoSolr(String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);

        try {
            if (authorizationManager.checkIsAdmin(userId)) {
                // Get all the studies
                Query query = new Query();
                QueryOptions options = new QueryOptions()
                        .append(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                                StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.FQN.key(),
                                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()))
                        .append(DBAdaptor.INCLUDE_ACLS, true);
                QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, options);
                if (studyQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Could not index catalog into solr. No studies found");
                }

                CatalogSolrManager catalogSolrManager = new CatalogSolrManager(this.catalogManager);
                // Create solr collections if they don't exist
                catalogSolrManager.createSolrCollections();

                ExecutorService threadPool = Executors.newFixedThreadPool(4);
                for (Study study : studyQueryResult.getResult()) {
                    Map<String, Set<String>> studyAcls = SolrConverterUtil
                            .parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
                    // We replace the current studyAcls for the parsed one
                    study.getAttributes().put("OPENCGA_ACL", studyAcls);

                    threadPool.submit(() -> indexCohort(catalogSolrManager, study));
                    threadPool.submit(() -> indexFile(catalogSolrManager, study));
                    threadPool.submit(() -> indexFamily(catalogSolrManager, study));
                    threadPool.submit(() -> indexIndividual(catalogSolrManager, study));
                    threadPool.submit(() -> indexSample(catalogSolrManager, study));
                }

                threadPool.shutdown();

                auditManager.audit(userId, AuditRecord.Action.INDEX, AuditRecord.Resource.CATALOG, "", "", "", "", new ObjectMap(),
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                return true;
            }
            throw new CatalogException("Only the " + ROOT + " user can index in Solr");
        } catch (CatalogException e) {
            auditManager.audit(userId, AuditRecord.Action.INDEX, AuditRecord.Resource.CATALOG, "", "", "", "", new ObjectMap(),
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public Map<String, Object> facet(String studyStr, String fileFields, String sampleFields, String individualFields, String cohortFields,
                                     String familyFields, boolean defaultStats, String sessionId) throws CatalogException, IOException {
        Map<String, Object> result = new HashMap<>();
        result.put("sample", catalogManager.getSampleManager().facet(studyStr, new Query(), setFacetFields(sampleFields), defaultStats,
                sessionId));
        result.put("file", catalogManager.getFileManager().facet(studyStr, new Query(), setFacetFields(fileFields), defaultStats,
                sessionId));
        result.put("individual", catalogManager.getIndividualManager().facet(studyStr, new Query(), setFacetFields(individualFields),
                defaultStats, sessionId));
        result.put("family", catalogManager.getFamilyManager().facet(studyStr, new Query(), setFacetFields(familyFields), defaultStats,
                sessionId));
        result.put("cohort", catalogManager.getCohortManager().facet(studyStr, new Query(), setFacetFields(cohortFields), defaultStats,
                sessionId));

        return result;
    }

    private QueryOptions setFacetFields(String fields) {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.FACET, fields);
        return queryOptions;
    }

    // **************************   Private methods  ******************************** //

    private Boolean indexCohort(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException, IOException {

        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryOptions cohortQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.NAME.key(),
                        CohortDBAdaptor.QueryParams.CREATION_DATE.key(), CohortDBAdaptor.QueryParams.STATUS.key(),
                        CohortDBAdaptor.QueryParams.RELEASE.key(), CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
                        CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), CohortDBAdaptor.QueryParams.TYPE.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(this.cohortDBAdaptor.iterator(query,
                cohortQueryOptions), new CatalogCohortToSolrCohortConverter(study), CatalogSolrManager.COHORT_SOLR_COLLECTION);
        return true;
    }

    private Boolean indexFile(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException, IOException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryOptions fileQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                        FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.TYPE.key(), FileDBAdaptor.QueryParams.FORMAT.key(),
                        FileDBAdaptor.QueryParams.CREATION_DATE.key(), FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                        FileDBAdaptor.QueryParams.RELEASE.key(), FileDBAdaptor.QueryParams.STATUS.key(),
                        FileDBAdaptor.QueryParams.EXTERNAL.key(), FileDBAdaptor.QueryParams.SIZE.key(),
                        FileDBAdaptor.QueryParams.SOFTWARE.key(), FileDBAdaptor.QueryParams.EXPERIMENT_UID.key(),
                        FileDBAdaptor.QueryParams.RELATED_FILES.key(), FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(),
                        FileDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(this.fileDBAdaptor.iterator(query,
                fileQueryOptions), new CatalogFileToSolrFileConverter(study), CatalogSolrManager.FILE_SOLR_COLLECTION);
        return true;
    }


    private Boolean indexFamily(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException, IOException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FamilyDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryOptions familyQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FamilyDBAdaptor.QueryParams.ID.key(),
                        FamilyDBAdaptor.QueryParams.CREATION_DATE.key(), FamilyDBAdaptor.QueryParams.STATUS.key(),
                        FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), FamilyDBAdaptor.QueryParams.RELEASE.key(),
                        FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
                        FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), FamilyDBAdaptor.QueryParams.EXPECTED_SIZE.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(this.familyDBAdaptor.iterator(query,
                familyQueryOptions), new CatalogFamilyToSolrFamilyConverter(study), CatalogSolrManager.FAMILY_SOLR_COLLECTION);
        return true;
    }


    private Boolean indexIndividual(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException, IOException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(IndividualDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryOptions individualQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.ID.key(),
                        IndividualDBAdaptor.QueryParams.FATHER_UID.key(), IndividualDBAdaptor.QueryParams.MOTHER_UID.key(),
                        IndividualDBAdaptor.QueryParams.MULTIPLES.key(), IndividualDBAdaptor.QueryParams.SEX.key(),
                        IndividualDBAdaptor.QueryParams.ETHNICITY.key(), IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                        IndividualDBAdaptor.QueryParams.RELEASE.key(), IndividualDBAdaptor.QueryParams.CREATION_DATE.key(),
                        IndividualDBAdaptor.QueryParams.VERSION.key(),
                        IndividualDBAdaptor.QueryParams.STATUS.key(), IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                        IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                        IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(),
                        IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(), IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(this.individualDBAdaptor.iterator(query,
                individualQueryOptions), new CatalogIndividualToSolrIndividualConverter(study),
                CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
        return true;
    }

    private Boolean indexSample(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException, IOException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryOptions sampleQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.SOURCE.key(),
                        SampleDBAdaptor.QueryParams.RELEASE.key(), SampleDBAdaptor.QueryParams.VERSION.key(),
                        SampleDBAdaptor.QueryParams.PROCESSING.key(), SampleDBAdaptor.QueryParams.COLLECTION.key(),
                        SampleDBAdaptor.QueryParams.CREATION_DATE.key(), SampleDBAdaptor.QueryParams.STATUS.key(),
                        SampleDBAdaptor.QueryParams.TYPE.key(), SampleDBAdaptor.QueryParams.SOMATIC.key(),
                        SampleDBAdaptor.QueryParams.PHENOTYPES.key(), SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
                        SampleDBAdaptor.QueryParams.UID.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(this.sampleDBAdaptor.iterator(query,
                sampleQueryOptions), new CatalogSampleToSolrSampleConverter(study), CatalogSolrManager.SAMPLE_SOLR_COLLECTION);
        return true;
    }


    private int getProjectCurrentRelease(long projectId) throws CatalogException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key());
        QueryResult<Project> projectQueryResult = projectDBAdaptor.get(projectId, options);
        if (projectQueryResult.getNumResults() == 0) {
            throw new CatalogException("Internal error. Cannot retrieve current release from project");
        }
        return projectQueryResult.first().getCurrentRelease();
    }

    private boolean existsGroup(long studyId, String groupId) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.UID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.GROUP_ID.key(), groupId);
        return studyDBAdaptor.count(query).first() > 0;
    }

    private void validatePermissionRules(long studyId, Study.Entity entry, PermissionRule permissionRule) throws CatalogException {
        ParamUtils.checkIdentifier(permissionRule.getId(), "PermissionRules");

        if (permissionRule.getPermissions() == null || permissionRule.getPermissions().isEmpty()) {
            throw new CatalogException("Missing permissions for the Permissions Rule object");
        }

        switch (entry) {
            case SAMPLES:
                validatePermissions(permissionRule.getPermissions(), SampleAclEntry.SamplePermissions::valueOf);
                break;
            case FILES:
                validatePermissions(permissionRule.getPermissions(), FileAclEntry.FilePermissions::valueOf);
                break;
            case COHORTS:
                validatePermissions(permissionRule.getPermissions(), CohortAclEntry.CohortPermissions::valueOf);
                break;
            case INDIVIDUALS:
                validatePermissions(permissionRule.getPermissions(), IndividualAclEntry.IndividualPermissions::valueOf);
                break;
            case FAMILIES:
                validatePermissions(permissionRule.getPermissions(), FamilyAclEntry.FamilyPermissions::valueOf);
                break;
            case JOBS:
                validatePermissions(permissionRule.getPermissions(), JobAclEntry.JobPermissions::valueOf);
                break;
            case CLINICAL_ANALYSES:
                validatePermissions(permissionRule.getPermissions(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::valueOf);
                break;
            default:
                throw new CatalogException("Unexpected entry found");
        }

        checkMembers(studyId, permissionRule.getMembers());
    }

    private void validatePermissions(List<String> permissions, Function<String, Object> valueOf) throws CatalogException {
        for (String permission : permissions) {
            try {
                valueOf.apply(permission);
            } catch (IllegalArgumentException e) {
                logger.error("Detected unsupported " + permission + " permission: {}", e.getMessage(), e);
                throw new CatalogException("Detected unsupported " + permission + " permission.");
            }
        }
    }

    private String getOwner(Study study) throws CatalogDBException {
        if (!StringUtils.isEmpty(study.getFqn())) {
            return StringUtils.split(study.getFqn(), "@")[0];
        }
        return studyDBAdaptor.getOwnerId(study.getUid());
    }

    public String getProjectFqn(String studyFqn) throws CatalogException {
        Matcher matcher = USER_PROJECT_STUDY_PATTERN.matcher(studyFqn);
        if (matcher.find()) {
            // studyStr contains the full path (owner@project:study)
            String owner = matcher.group(1);
            String project = matcher.group(2);
            return owner + '@' + project;
        } else {
            throw new CatalogException("Invalid Study FQN. The accepted pattern is [ownerId@projectId:studyId]");
        }
    }

//    private Map<Long, String> getAllStudiesIdAndUid(List<Study> studies) {
//        Map<Long, String> allStudiesIdAndUids = new HashMap<>();
//
//        for (Study study : studies) {
//            String id = study.getFqn().replace(":", "__");
//            long uid = study.getUid();
//            allStudiesIdAndUids.put(uid, id);
//        }
//        return allStudiesIdAndUids;
//
//    }

}
