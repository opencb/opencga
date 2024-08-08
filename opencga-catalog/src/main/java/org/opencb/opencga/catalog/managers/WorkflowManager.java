package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.workflow.Workflow;
import org.opencb.opencga.core.models.workflow.WorkflowRepository;
import org.opencb.opencga.core.models.workflow.WorkflowScript;
import org.opencb.opencga.core.models.workflow.WorkflowUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor.QueryParams.*;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class WorkflowManager extends ResourceManager<Workflow> {

    public static final QueryOptions INCLUDE_WORKFLOW_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(ID.key(), UID.key(),
            UUID.key(), VERSION.key()));

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
            validateNewWorkflow(workflow);

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
        return null;
    }

    @Override
    public OpenCGAResult<Workflow> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult<Workflow> count(String studyId, Query query, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> ids, QueryOptions options, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return null;
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

    private void validateNewWorkflow(Workflow workflow) throws CatalogParameterException {
        ParamUtils.checkIdentifier(workflow.getId(), ID.key());
        ParamUtils.checkObj(workflow.getType(), TYPE.key());
        workflow.setScripts(workflow.getScripts() != null ? workflow.getScripts() : Collections.emptyList());
        boolean main = false;
        for (WorkflowScript script : workflow.getScripts()) {
            ParamUtils.checkIdentifier(script.getFilename(), SCRIPTS.key() + ".id");
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
        workflow.setDocker(workflow.getDocker() != null ? workflow.getDocker() : new WorkflowRepository(""));
        if (StringUtils.isEmpty(workflow.getDocker().getImage()) && CollectionUtils.isEmpty(workflow.getScripts())) {
            throw new CatalogParameterException("No docker image or scripts found.");
        }

        workflow.setName(ParamUtils.defaultString(workflow.getName(), workflow.getId()));
        workflow.setDescription(ParamUtils.defaultString(workflow.getDescription(), ""));
        workflow.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.WORKFLOW));
        workflow.setVersion(1);
        workflow.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(workflow.getCreationDate(), CREATION_DATE.key()));
        workflow.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(workflow.getModificationDate(), MODIFICATION_DATE.key()));
        workflow.setAttributes(ParamUtils.defaultObject(workflow.getAttributes(), Collections.emptyMap()));
    }

}
