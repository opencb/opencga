package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.workflow.Workflow;
import org.opencb.opencga.core.models.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.models.workflow.WorkflowUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class WorkflowManager extends AbstractManager {

    public static final QueryOptions INCLUDE_WORKFLOW_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            WorkflowDBAdaptor.QueryParams.ID.key(), WorkflowDBAdaptor.QueryParams.UID.key(), WorkflowDBAdaptor.QueryParams.UUID.key(),
            WorkflowDBAdaptor.QueryParams.VERSION.key()));

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


    private InternalGetDataResult<Workflow> internalGet(String organizationId, List<String> workflowList, Query query, QueryOptions options,
                                                        String userId, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(workflowList)) {
            throw new CatalogException("Missing workflow entries.");
        }
        List<String> uniqueList = ListUtils.unique(workflowList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(WorkflowDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one workflow allowed when requesting multiple versions");
        }

        WorkflowDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Workflow> workflowResult = getWorkflowDBAdaptor(organizationId).get(queryCopy, queryOptions);

        Function<Workflow, String> workflowStringFunction = Workflow::getId;
        if (idQueryParam.equals(WorkflowDBAdaptor.QueryParams.UUID)) {
            workflowStringFunction = Workflow::getUuid;
        }

        if (ignoreException || workflowResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, workflowStringFunction, workflowResult, ignoreException, versioned);
        } else {
            throw CatalogException.notFound("workflows", getMissingFields(uniqueList, workflowResult.getResults(), workflowStringFunction));
        }
    }

    private WorkflowDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        WorkflowDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            WorkflowDBAdaptor.QueryParams param = WorkflowDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = WorkflowDBAdaptor.QueryParams.UUID;
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

    public OpenCGAResult<Workflow> create(WorkflowCreateParams createParams, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        ObjectMap auditParams = new ObjectMap()
                .append("workflow", createParams)
                .append("options", options)
                .append("token", token);

        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);

        // 0. Create the workflow object
        Workflow workflow = createParams.toWorkflow();
        try {
            // 1. Check permissions
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            // 2. Validate the workflow parameters
            validateNewWorkflow(workflow);

            // 3. We insert the workflow
            OpenCGAResult<Workflow> insert = getWorkflowDBAdaptor(organizationId).insert(workflow, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                Query query = new Query()
                        .append(WorkflowDBAdaptor.QueryParams.UID.key(), workflow.getUid());
                OpenCGAResult<Workflow> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, workflow.getId(), workflow.getUuid(), "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, workflow.getId(), "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void validateNewWorkflow(Workflow workflow) throws CatalogParameterException {
        ParamUtils.checkIdentifier(workflow.getId(), WorkflowDBAdaptor.QueryParams.ID.key());
        ParamUtils.checkObj(workflow.getType(), WorkflowDBAdaptor.QueryParams.TYPE.key());
        ParamUtils.checkParameter(workflow.getCommandLine(), WorkflowDBAdaptor.QueryParams.COMMAND_LINE.key());
        workflow.setScripts(workflow.getScripts() != null ? workflow.getScripts() : Collections.emptyList());
        for (Workflow.Script script : workflow.getScripts()) {
            ParamUtils.checkIdentifier(script.getId(), WorkflowDBAdaptor.QueryParams.SCRIPTS.key() + ".id");
            ParamUtils.checkParameter(script.getContent(), WorkflowDBAdaptor.QueryParams.SCRIPTS.key() + ".content");
        }
        workflow.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.WORKFLOW));
        workflow.setVersion(1);
        workflow.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(workflow.getCreationDate(),
                WorkflowDBAdaptor.QueryParams.CREATION_DATE.key()));
        workflow.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(workflow.getModificationDate(),
                WorkflowDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
    }

    public OpenCGAResult<Workflow> update(String workflowId, WorkflowUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        ObjectMap updateMap = null;
        try {
            if (updateParams != null) {
                updateMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
            }
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse WorkflowUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("workflowId", workflowId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);

        String id = UuidUtils.isOpenCgaUuid(workflowId) ? "" : workflowId;
        String uuid = UuidUtils.isOpenCgaUuid(workflowId) ? workflowId : "";
        try {
            // 1. Check permissions
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            Workflow workflow = internalGet(organizationId, Collections.singletonList(workflowId), null, INCLUDE_WORKFLOW_IDS, userId,
                    false).first();
            id = workflow.getId();
            uuid = workflow.getUuid();

            // 2. Update workflow object
            OpenCGAResult<Workflow> insert = getWorkflowDBAdaptor(organizationId).update(workflow.getUid(), updateMap, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated workflow
                Query query = new Query()
                        .append(WorkflowDBAdaptor.QueryParams.UID.key(), workflow.getUid());
                OpenCGAResult<Workflow> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.WORKFLOW, id, uuid, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.WORKFLOW, id, uuid, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Workflow> get(String workflow, QueryOptions queryOptions, String token) throws CatalogException {
        return get(Collections.singletonList(workflow), queryOptions, false, token);
    }

    public OpenCGAResult<Workflow> get(List<String> workflowList, QueryOptions queryOptions, boolean ignoreException, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("workflowList", workflowList)
                .append("queryOptions", queryOptions)
                .append("ignoreException", ignoreException)
                .append("token", token);
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        auditManager.initAuditBatch(operationUuid);
        QueryOptions options = queryOptions != null ? new QueryOptions(queryOptions) : new QueryOptions();
        try {
            OpenCGAResult<Workflow> result = OpenCGAResult.empty(Workflow.class);
            options.remove(QueryOptions.LIMIT);
            InternalGetDataResult<Workflow> responseResult = internalGet(organizationId, workflowList, null, options, userId,
                    ignoreException);

            Map<String, InternalGetDataResult<Workflow>.Missing> missingMap = new HashMap<>();
            if (responseResult.getMissing() != null) {
                missingMap = responseResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<List<Workflow>> versionedResults = responseResult.getVersionedResults();
            for (int i = 0; i < versionedResults.size(); i++) {
                String entryId = workflowList.get(i);
                if (versionedResults.get(i).isEmpty()) {
                    Event event = new Event(Event.Type.ERROR, entryId, missingMap.get(entryId).getErrorMsg());
                    // Missing
                    result.getEvents().add(event);
                } else {
                    int size = versionedResults.get(i).size();
                    result.append(new OpenCGAResult<>(0, Collections.emptyList(), size, versionedResults.get(i), size));

                    Workflow entry = versionedResults.get(i).get(0);
                    auditManager.auditInfo(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, entry.getId(), entry.getUuid(),
                            "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                }
            }

            return result;
        } catch (CatalogException e) {
            for (String entryId : workflowList) {
                auditManager.auditInfo(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, entryId, "", "", "",
                        auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationUuid);
        }
    }

}
