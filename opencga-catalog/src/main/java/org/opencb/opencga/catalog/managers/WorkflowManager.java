package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.workflow.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor.QueryParams.*;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class WorkflowManager extends ResourceManager<Workflow> {

    public static final QueryOptions INCLUDE_WORKFLOW_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(ID.key(), UID.key(),
            UUID.key(), VERSION.key(), STUDY_UID.key()));

    private final CatalogIOManager catalogIOManager;
    private final IOManagerFactory ioManagerFactory;

    private final Logger logger;

    WorkflowManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, IOManagerFactory ioManagerFactory, CatalogIOManager catalogIOManager,
                    Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.catalogIOManager = catalogIOManager;
        this.ioManagerFactory = ioManagerFactory;
        this.logger = LoggerFactory.getLogger(WorkflowManager.class);
    }


    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.WORKFLOW;
    }

    @Override
    InternalGetDataResult<Workflow> internalGet(String organizationId, long studyUid, List<String> workflowIdList, @Nullable Query query,
                                                QueryOptions options, String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(workflowIdList)) {
            throw new CatalogException("Missing workflow entries.");
        }
        List<String> uniqueList = ListUtils.unique(workflowIdList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one workflow allowed when requesting multiple versions");
        }

        WorkflowDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Workflow> workflowResult = getWorkflowDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<Workflow, String> workflowStringFunction = Workflow::getId;
        if (idQueryParam.equals(UUID)) {
            workflowStringFunction = Workflow::getUuid;
        }

        if (ignoreException || workflowResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, workflowStringFunction, workflowResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Workflow> resultsNoCheck = getWorkflowDBAdaptor(organizationId).get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == workflowResult.getNumResults()) {
            throw CatalogException.notFound("workflows", getMissingFields(uniqueList, workflowResult.getResults(), workflowStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the workflows.");
        }
    }

    @Override
    public OpenCGAResult<Workflow> create(String studyStr, Workflow workflow, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("workflow", workflow)
                .append("options", options)
                .append("token", token);

        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        String studyId = studyFqn.getStudyId();
        String studyUuid = studyFqn.getStudyUuid();
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
            studyId = study.getId();
            studyUuid = study.getUuid();

            // 1. Check permissions
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), tokenPayload,
                    StudyPermissions.Permissions.WRITE_WORKFLOWS);

            // 2. Validate the workflow parameters
            validateNewWorkflow(workflow, userId);

            // 3. We insert the workflow
            OpenCGAResult<Workflow> insert = getWorkflowDBAdaptor(organizationId).insert(study.getUid(), workflow, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                Query query = new Query()
                        .append(STUDY_UID.key(), study.getUid())
                        .append(UID.key(), workflow.getUid());
                OpenCGAResult<Workflow> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, workflow.getId(), workflow.getUuid(), studyId,
                    studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, workflow.getId(), "", studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Workflow> update(String studyStr, String workflowId, WorkflowUpdateParams updateParams, QueryOptions options,
                                          String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("workflowId", workflowId)
                .append("updateParams", updateParams)
                .append("options", options)
                .append("token", token);

        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        String studyId = studyFqn.getStudyId();
        String studyUuid = studyFqn.getStudyUuid();

        String id = UuidUtils.isOpenCgaUuid(workflowId) ? "" : workflowId;
        String uuid = UuidUtils.isOpenCgaUuid(workflowId) ? workflowId : "";
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
            studyId = study.getId();
            studyUuid = study.getUuid();

            // 1. Check permissions
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            Workflow workflow = internalGet(organizationId, study.getUid(), Collections.singletonList(workflowId), null,
                    INCLUDE_WORKFLOW_IDS, userId, false).first();
            id = workflow.getId();
            uuid = workflow.getUuid();

            if (updateParams == null) {
                throw new CatalogException("Missing parameters to update the workflow.");
            }

            if (updateParams.getManager() != null) {
                if (updateParams.getManager().getId() == null) {
                    throw new CatalogException("Manager id cannot be left empty.");
                }
                if (StringUtils.isEmpty(updateParams.getManager().getVersion())) {
                    throw new CatalogException("Manager version cannot be left empty.");
                }
            }

            ObjectMap updateMap;
            try {
                updateMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams));
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse WorkflowUpdateParams object: " + e.getMessage(), e);
            }

            // 2. Update workflow object
            OpenCGAResult<Workflow> insert = getWorkflowDBAdaptor(organizationId).update(workflow.getUid(), updateMap, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated workflow
                Query query = new Query()
                        .append(UID.key(), workflow.getUid());
                OpenCGAResult<Workflow> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.WORKFLOW, id, uuid, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.WORKFLOW, id, uuid, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public DBIterator<Workflow> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Query finalQuery = new Query(query);
        fixQueryObject(finalQuery);
        finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getWorkflowDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<Workflow> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        String studyId = studyFqn.getStudyId();
        String studyUuid = studyFqn.getStudyUuid();

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);
            studyId = study.getId();
            studyUuid = study.getUuid();

            fixQueryObject(query);
            query.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Workflow> queryResult = getWorkflowDBAdaptor(organizationId).get(study.getUid(), query, options, userId);

            auditManager.auditSearch(organizationId, userId, Enums.Resource.WORKFLOW, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.WORKFLOW, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyStr, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("fields", fields)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(query);

            query.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = getWorkflowDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.WORKFLOW, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.WORKFLOW, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Workflow> count(String studyStr, Query query, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        query = new Query(ParamUtils.defaultObject(query, Query::new));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("query", query)
                .append("token", token);
        try {
            fixQueryObject(query);

            query.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = getWorkflowDBAdaptor(organizationId).count(query, userId);

            auditManager.auditCount(organizationId, userId, Enums.Resource.WORKFLOW, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(organizationId, userId, Enums.Resource.WORKFLOW, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Workflow> delete(String studyStr, List<String> ids, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, ids, options, false, token);
    }

    public OpenCGAResult<Workflow> delete(String studyStr, List<String> ids, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        if (ids == null || ListUtils.isEmpty(ids)) {
            throw new CatalogException("Missing list of workflow ids");
        }

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("workflowIds", ids)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.WORKFLOW, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Workflow> result = OpenCGAResult.empty(Workflow.class);
        for (String id : ids) {
            String workflowId = id;
            String workflowUuid = "";
            try {
                OpenCGAResult<Workflow> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_WORKFLOW_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Workflow '" + id + "' not found");
                }
                Workflow workflow = internalResult.first();

                // We set the proper values for the audit
                workflowId = workflow.getId();
                workflowUuid = workflow.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkWorkflowPermission(organizationId, study.getUid(), workflow.getUid(), userId,
                            WorkflowPermissions.DELETE);
                }

                // Check if the workflow can be deleted
                checkWorkflowCanBeDeleted(organizationId, study.getUid(), workflow, params.getBoolean(Constants.FORCE, false));

                result.append(getWorkflowDBAdaptor(organizationId).delete(workflow));

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.WORKFLOW, workflow.getId(), workflow.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete workflow " + workflowId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, workflowId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.WORKFLOW, workflowId, workflowUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    private void checkWorkflowCanBeDeleted(String organizationId, long uid, Workflow workflow, boolean force) {
        return;
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        OpenCGAResult result = OpenCGAResult.empty();

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the workflows to be deleted
        DBIterator<Workflow> iterator;
        try {
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getWorkflowDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_WORKFLOW_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Workflow workflow = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkWorkflowPermission(organizationId, study.getUid(), workflow.getUid(), userId,
                            WorkflowPermissions.DELETE);
                }

                // Check if the workflow can be deleted
                checkWorkflowCanBeDeleted(organizationId, study.getUid(), workflow, params.getBoolean(Constants.FORCE, false));

                result.append(getWorkflowDBAdaptor(organizationId).delete(workflow));

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, workflow.getId(),
                        workflow.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete workflow " + workflow.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, workflow.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, workflow.getId(),
                        workflow.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationUuid);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        return null;
    }

    private WorkflowDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        WorkflowDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            WorkflowDBAdaptor.QueryParams param = ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = UUID;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        return idQueryParam;
    }

    private void validateNewWorkflow(Workflow workflow, String userId) throws CatalogParameterException {
        ParamUtils.checkIdentifier(workflow.getId(), ID.key());
        ParamUtils.checkObj(workflow.getType(), TYPE.key());
        if (workflow.getManager() == null) {
            workflow.setManager(new WorkflowSystem());
        }
        if (workflow.getManager().getId() == null) {
            workflow.getManager().setId(WorkflowSystem.SystemId.NEXTFLOW);
        }
        if (StringUtils.isEmpty(workflow.getManager().getVersion())) {
            workflow.getManager().setVersion("24.04.4");
        }
        workflow.setTags(workflow.getTags() != null ? workflow.getTags() : Collections.emptyList());
        workflow.setScripts(workflow.getScripts() != null ? workflow.getScripts() : Collections.emptyList());
        boolean main = false;
        for (WorkflowScript script : workflow.getScripts()) {
            ParamUtils.checkIdentifier(script.getFileName(), SCRIPTS.key() + ".id");
            ParamUtils.checkParameter(script.getContent(), SCRIPTS.key() + ".content");
            if (script.isMain()) {
                if (main) {
                    throw new CatalogParameterException("More than one main script found.");
                }
                main = script.isMain();
            }
        }
        if (CollectionUtils.isNotEmpty(workflow.getScripts()) && !main) {
            throw new CatalogParameterException("No main script found.");
        }
        workflow.setRepository(workflow.getRepository() != null ? workflow.getRepository() : new WorkflowRepository(""));
        if (StringUtils.isEmpty(workflow.getRepository().getImage()) && CollectionUtils.isEmpty(workflow.getScripts())) {
            throw new CatalogParameterException("No repository image or scripts found.");
        }
        if (StringUtils.isNotEmpty(workflow.getRepository().getImage()) && CollectionUtils.isNotEmpty(workflow.getScripts())) {
            throw new CatalogParameterException("Both repository image and scripts found. Please, either add scripts or a repository"
                    + " image.");
        }

        workflow.setName(ParamUtils.defaultString(workflow.getName(), workflow.getId()));
        workflow.setDescription(ParamUtils.defaultString(workflow.getDescription(), ""));
        workflow.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.WORKFLOW));
        workflow.setVersion(1);
        workflow.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(workflow.getCreationDate(), CREATION_DATE.key()));
        workflow.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(workflow.getModificationDate(), MODIFICATION_DATE.key()));
        workflow.setAttributes(ParamUtils.defaultObject(workflow.getAttributes(), Collections.emptyMap()));
        workflow.setInternal(new WorkflowInternal(new InternalStatus(InternalStatus.READY), TimeUtils.getTime(), TimeUtils.getTime(),
                userId));
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<WorkflowPermissions>> getAcls(String studyId, List<String> workflowList, String member,
                                                                    boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, workflowList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<WorkflowPermissions>> getAcls(String studyStr, List<String> workflowList, List<String> members,
                                                                  boolean ignoreException, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("workflowList", workflowList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<AclEntryList<WorkflowPermissions>> workflowAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<Workflow> queryResult = internalGet(organizationId, study.getUid(), workflowList, INCLUDE_WORKFLOW_IDS,
                    userId, ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> workflowUids = queryResult.getResults().stream().map(Workflow::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                workflowAcls = authorizationManager.getAcl(organizationId, study.getUid(), workflowUids, members, Enums.Resource.WORKFLOW,
                        WorkflowPermissions.class, userId);
            } else {
                workflowAcls = authorizationManager.getAcl(organizationId, study.getUid(), workflowUids, Enums.Resource.WORKFLOW,
                        WorkflowPermissions.class, userId);
            }

            // Include non-existing samples to the result list
            List<AclEntryList<WorkflowPermissions>> resultList = new ArrayList<>(workflowList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String workflowId : workflowList) {
                if (!missingMap.containsKey(workflowId)) {
                    Workflow workflow = queryResult.getResults().get(counter);
                    resultList.add(workflowAcls.getResults().get(counter));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.WORKFLOW,
                            workflow.getId(), workflow.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, workflowId, missingMap.get(workflowId).getErrorMsg()));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.WORKFLOW, workflowId,
                            "", study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(workflowId).getErrorMsg())), new ObjectMap());
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                workflowAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            workflowAcls.setResults(resultList);
            workflowAcls.setEvents(eventList);
        } catch (CatalogException e) {
            for (String workflowId : workflowList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.WORKFLOW, workflowId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            if (!ignoreException) {
                throw e;
            } else {
                for (String workflowId : workflowList) {
                    Event event = new Event(Event.Type.ERROR, workflowId, e.getMessage());
                    workflowAcls.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                }
            }
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }

        return workflowAcls;
    }

    public OpenCGAResult<AclEntryList<WorkflowPermissions>> updateAcl(String studyStr, String memberList, WorkflowAclUpdateParams params,
                                                                      ParamUtils.AclAction action, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("memberList", memberList)
                .append("params", params)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        List<String> members;
        List<Workflow> workflowList;
        try {
            auditManager.initAuditBatch(operationId);

            ParamUtils.checkObj(params, "WorkflowAclUpdateParams");

            if (CollectionUtils.isEmpty(params.getWorkflowIds())) {
                throw new CatalogException("Update ACL: No workflows provided to be updated.");
            }
            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }
            if (CollectionUtils.isNotEmpty(params.getPermissions())) {
                checkPermissions(params.getPermissions(), WorkflowPermissions::valueOf);
            }

            workflowList = internalGet(organizationId, study.getUid(), params.getWorkflowIds(), INCLUDE_WORKFLOW_IDS, userId, false)
                    .getResults();
            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

            // Validate that the members are actually valid members
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            checkMembers(organizationId, study.getUid(), members);
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        } catch (CatalogException e) {
            if (CollectionUtils.isNotEmpty(params.getWorkflowIds())) {
                for (String workflowId : params.getWorkflowIds()) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW, workflowId,
                            "", study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            auditManager.finishAuditBatch(organizationId, operationId);
            throw e;
        }

        OpenCGAResult<AclEntryList<WorkflowPermissions>> aclResultList = OpenCGAResult.empty();
        int numProcessed = 0;
        do {
            List<Workflow> batchWorkflowList = new ArrayList<>();
            while (numProcessed < Math.min(numProcessed + BATCH_OPERATION_SIZE, workflowList.size())) {
                batchWorkflowList.add(workflowList.get(numProcessed));
                numProcessed += 1;
            }

            List<Long> workflowUids = batchWorkflowList.stream().map(Workflow::getUid).collect(Collectors.toList());
            List<String> workflowIds = batchWorkflowList.stream().map(Workflow::getId).collect(Collectors.toList());
            List<AuthorizationManager.CatalogAclParams> aclParamsList = new ArrayList<>();
            AuthorizationManager.CatalogAclParams.addToList(workflowUids, params.getPermissions(), Enums.Resource.WORKFLOW, aclParamsList);

            try {
                switch (action) {
                    case SET:
                        authorizationManager.setAcls(organizationId, study.getUid(), members, aclParamsList);
                        break;
                    case ADD:
                        authorizationManager.addAcls(organizationId, study.getUid(), members, aclParamsList);
                        break;
                    case REMOVE:
                        authorizationManager.removeAcls(organizationId, members, aclParamsList);
                        break;
                    case RESET:
                        for (AuthorizationManager.CatalogAclParams aclParam : aclParamsList) {
                            aclParam.setPermissions(null);
                        }
                        authorizationManager.removeAcls(organizationId, members, aclParamsList);
                        break;
                    default:
                        throw new CatalogException("Unexpected error occurred. No valid action found.");
                }

                OpenCGAResult<AclEntryList<WorkflowPermissions>> queryResults = authorizationManager.getAcls(organizationId, study.getUid(),
                        workflowUids, members, Enums.Resource.WORKFLOW, WorkflowPermissions.class);

                for (int i = 0; i < queryResults.getResults().size(); i++) {
                    queryResults.getResults().get(i).setId(workflowIds.get(i));
                }
                aclResultList.append(queryResults);

                for (Workflow workflow : batchWorkflowList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW,
                            workflow.getId(), workflow.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                }
            } catch (CatalogException e) {
                // Process current batch
                for (Workflow workflow : batchWorkflowList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW,
                            workflow.getId(), workflow.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                // Process remaining unprocessed batches
                while (numProcessed < workflowList.size()) {
                    Workflow workflow = workflowList.get(numProcessed);
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW,
                            workflow.getId(), workflow.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                auditManager.finishAuditBatch(organizationId, operationId);
                throw e;
            }
        } while (numProcessed < workflowList.size());

        auditManager.finishAuditBatch(organizationId, operationId);
        return aclResultList;
    }

}
