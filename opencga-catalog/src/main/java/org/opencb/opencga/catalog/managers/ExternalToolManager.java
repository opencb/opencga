package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.*;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.externalTool.custom.*;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.variant.VariantWalkerToolParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor.QueryParams.*;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ExternalToolManager extends ResourceManager<ExternalTool> {

    public static final QueryOptions INCLUDE_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(ID.key(), UID.key(),
            UUID.key(), VERSION.key(), DESCRIPTION.key(), STUDY_UID.key(), WORKFLOW.key(), CONTAINER.key()));

    private final Logger logger;

    ExternalToolManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.logger = LoggerFactory.getLogger(ExternalToolManager.class);
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.EXTERNAL_TOOL;
    }

    @Override
    InternalGetDataResult<ExternalTool> internalGet(String organizationId, long studyUid, List<String> externalToolIdList,
                                                    @Nullable Query query, QueryOptions options, String user, boolean ignoreException)
            throws CatalogException {
        if (ListUtils.isEmpty(externalToolIdList)) {
            throw new CatalogException("Missing external tool entries.");
        }
        List<String> uniqueList = ListUtils.unique(externalToolIdList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one external tool allowed when requesting multiple versions");
        }

        ExternalToolDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<ExternalTool> externalToolResult = getWorkflowDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<ExternalTool, String> externalToolStringFunction = ExternalTool::getId;
        if (idQueryParam.equals(UUID)) {
            externalToolStringFunction = ExternalTool::getUuid;
        }

        if (ignoreException || externalToolResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, externalToolStringFunction, externalToolResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<ExternalTool> resultsNoCheck = getWorkflowDBAdaptor(organizationId).get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == externalToolResult.getNumResults()) {
            throw CatalogException.notFound("user tools", getMissingFields(uniqueList, externalToolResult.getResults(),
                    externalToolStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the external"
                    + " tools.");
        }
    }

    @Override
    public OpenCGAResult<ExternalTool> create(String studyStr, ExternalTool externalTool, QueryOptions options, String token)
            throws CatalogException {
        throw new NotImplementedException("Not implemented yet");
    }

    public OpenCGAResult<ExternalTool> createWorkflow(String studyStr, WorkflowCreateParams workflow, QueryOptions options, String token)
            throws CatalogException {
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
                    StudyPermissions.Permissions.WRITE_USER_TOOLS);

            // Convert WorkflowCreateParams to ExternalTool
            ExternalTool externalTool = new ExternalTool(workflow.getId(), workflow.getName(), workflow.getDescription(),
                    ExternalToolType.WORKFLOW, workflow.getScope(), workflow.getWorkflow(), null, workflow.getTags(),
                    workflow.getVariables(), workflow.getMinimumRequirements(), workflow.isDraft(), workflow.getInternal(),
                    workflow.getCreationDate(), workflow.getModificationDate(), workflow.getAttributes());

            // 2. Validate the workflow parameters
            validateNewWorkflow(externalTool, userId);

            // 3. We insert the workflow
            OpenCGAResult<ExternalTool> insert = getWorkflowDBAdaptor(organizationId).insert(study.getUid(), externalTool, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                Query query = new Query()
                        .append(STUDY_UID.key(), study.getUid())
                        .append(UID.key(), externalTool.getUid());
                OpenCGAResult<ExternalTool> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, externalTool.getId(), externalTool.getUuid(),
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, workflow.getId(), "", studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<ExternalTool> createCustomTool(String studyStr, CustomToolCreateParams toolCreateParams, QueryOptions options,
                                                        String token) throws CatalogException {
        return createGenericTool(studyStr, toolCreateParams, ExternalToolType.CUSTOM_TOOL, options, token);
    }

    public OpenCGAResult<ExternalTool> createVariantWalkerTool(String studyStr, CustomToolCreateParams toolCreateParams,
                                                               QueryOptions options, String token) throws CatalogException {
        return createGenericTool(studyStr, toolCreateParams, ExternalToolType.VARIANT_WALKER, options, token);
    }

    private OpenCGAResult<ExternalTool> createGenericTool(String studyStr, CustomToolCreateParams toolCreateParams,
                                                          ExternalToolType toolType, QueryOptions options, String token)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("customTool", toolCreateParams)
                .append("toolType", toolType)
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
                    StudyPermissions.Permissions.WRITE_USER_TOOLS);

            // Convert CustomToolCreateParams to ExternalTool
            ExternalTool externalTool = new ExternalTool(toolCreateParams.getId(), toolCreateParams.getName(),
                    toolCreateParams.getDescription(), toolType, toolCreateParams.getScope(), null,
                    toolCreateParams.getContainer(), toolCreateParams.getTags(), toolCreateParams.getVariables(),
                    toolCreateParams.getMinimumRequirements(), toolCreateParams.isDraft(), toolCreateParams.getInternal(),
                    toolCreateParams.getCreationDate(), toolCreateParams.getModificationDate(), toolCreateParams.getAttributes());

            // 2. Validate the custom tool parameters
            validateNewCustomTool(externalTool, userId);

            // 3. We insert the workflow
            OpenCGAResult<ExternalTool> insert = getWorkflowDBAdaptor(organizationId).insert(study.getUid(), externalTool, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                Query query = new Query()
                        .append(STUDY_UID.key(), study.getUid())
                        .append(UID.key(), externalTool.getUid());
                OpenCGAResult<ExternalTool> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, externalTool.getId(), externalTool.getUuid(),
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, toolCreateParams.getId(), "", studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Job> submitWorkflow(String studyStr, WorkflowParams params, String jobId, String jobDescription,
                                             String jobDependsOnStr, String jobTagsStr, String jobScheduledStartTime, String jobPriority,
                                             Boolean dryRun, String token) throws CatalogException {
        return submit(studyStr, params, jobId, jobDescription, jobDependsOnStr, jobTagsStr, jobScheduledStartTime, jobPriority, dryRun,
                token);
    }

    public OpenCGAResult<Job> submitCustomTool(String studyStr, CustomToolRunParams params, String jobId, String jobDescription,
                                               String jobDependsOnStr, String jobTagsStr, String jobScheduledStartTime, String jobPriority,
                                               Boolean dryRun, String token) throws CatalogException {
        return submit(studyStr, params, jobId, jobDescription, jobDependsOnStr, jobTagsStr, jobScheduledStartTime, jobPriority, dryRun,
                token);
    }

    public OpenCGAResult<Job> submitVariantWalker(String projectStr, String studyStr, VariantWalkerToolParams params, String jobId,
                                                  String jobDescription, String jobDependsOnStr, String jobTagsStr,
                                                  String jobScheduledStartTime, String jobPriority, Boolean dryRun, String token)
            throws CatalogException {
        studyStr = getStudyFromProject(projectStr, studyStr, token);
        return submit(studyStr, params, jobId, jobDescription, jobDependsOnStr, jobTagsStr, jobScheduledStartTime, jobPriority, dryRun,
                token);
    }

    private String getStudyFromProject(String projectStr, String studyStr, String token) throws CatalogException {
        if (StringUtils.isNotEmpty(projectStr) && StringUtils.isEmpty(studyStr)) {
            // Project job
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key());
            // Peek any study. The ExecutionDaemon will take care of filling up the rest of studies.
            List<String> studies = catalogManager.getStudyManager()
                    .search(projectStr, new Query(), options, token)
                    .getResults()
                    .stream()
                    .map(Study::getFqn)
                    .collect(Collectors.toList());
            if (studies.isEmpty()) {
                throw new CatalogException("Project '" + projectStr + "' not found!");
            }
            studyStr = studies.get(0);
        }
        return studyStr;
    }

    public OpenCGAResult<Job> submit(String studyStr, ExternalToolParams params, String jobId, String jobDescription,
                                     String jobDependsOnStr, String jobTagsStr, String jobScheduledStartTime, String jobPriority,
                                     Boolean dryRun, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        ParamUtils.checkParameter(params.getId(), "External tool id");

        String externalToolId = params.getId();
        Integer version = params.getVersion();

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);

        Query query = new Query();
        if (version != null) {
            query.append(VERSION.key(), version);
        }
        ExternalTool externalTool = internalGet(organizationId, study.getUid(), externalToolId, query, QueryOptions.empty(), userId)
                .first();
        ExternalToolType type = externalTool.getType();

        ToolInfo toolInfo = new ToolInfo()
                .setId(externalTool.getId())
                // Set workflow minimum requirements
                .setMinimumRequirements(externalTool.getMinimumRequirements() != null
                        ? externalTool.getMinimumRequirements()
                        : configuration.getAnalysis().getWorkflow().getMinRequirements())
                .setDescription(externalTool.getDescription());

        // Build ExternalToolRunParams
        Map<String, Object> paramsMap = params != null ? params.toParams() : new HashMap<>();
        paramsMap.putIfAbsent(ParamConstants.STUDY_PARAM, study.getFqn());

        Enums.Priority priority = Enums.Priority.MEDIUM;
        if (!StringUtils.isEmpty(jobPriority)) {
            priority = Enums.Priority.getPriority(jobPriority.toUpperCase());
        }

        List<String> jobDependsOn;
        if (StringUtils.isNotEmpty(jobDependsOnStr)) {
            jobDependsOn = Arrays.asList(jobDependsOnStr.split(","));
        } else {
            jobDependsOn = Collections.emptyList();
        }

        List<String> jobTags;
        if (StringUtils.isNotEmpty(jobTagsStr)) {
            jobTags = Arrays.asList(jobTagsStr.split(","));
        } else {
            jobTags = Collections.emptyList();
        }

        JobType jobType = toJobType(type);
        return catalogManager.getJobManager().submit(study.getFqn(), jobType, toolInfo, priority, paramsMap, jobId, jobDescription,
                jobDependsOn, jobTags, null, jobScheduledStartTime, dryRun, Collections.emptyMap(), token);
    }

    private static JobType toJobType(ExternalToolType type) throws CatalogException {
        JobType jobType;
        switch (type) {
            case CUSTOM_TOOL:
                jobType = JobType.CUSTOM_TOOL;
                break;
            case VARIANT_WALKER:
                jobType = JobType.VARIANT_WALKER;
                break;
            case WORKFLOW:
                jobType = JobType.WORKFLOW;
                break;
            default:
                throw new CatalogException("Unknown external tool type: " + type);
        }
        return jobType;
    }

    public OpenCGAResult<ExternalTool> importWorkflow(String studyStr, WorkflowRepositoryParams repository, QueryOptions options,
                                                      String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("repository", repository)
                .append("options", options)
                .append("token", token);

        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        String studyId = studyFqn.getStudyId();
        String studyUuid = studyFqn.getStudyUuid();
        String workflowId = "";
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);
            studyId = study.getId();
            studyUuid = study.getUuid();

            // 1. Check permissions
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), tokenPayload,
                    StudyPermissions.Permissions.WRITE_USER_TOOLS);

            // 2. Download workflow
            ExternalTool externalTool = NextflowUtils.importRepository(repository);
            validateNewWorkflow(externalTool, userId);
            workflowId = externalTool.getId();

            OpenCGAResult<ExternalTool> result;

            Query query = new Query()
                    .append(STUDY_UID.key(), study.getUid())
                    .append(ID.key(), externalTool.getId());
            OpenCGAResult<ExternalTool> tmpResult = getWorkflowDBAdaptor(organizationId).get(query, INCLUDE_IDS);
            if (tmpResult.getNumResults() > 0) {
                logger.warn("Workflow '" + workflowId + "' already exists. Updating with the latest workflow information.");
                try {
                    // Set workflow uid just in case users want to get the final result
                    externalTool.setUid(tmpResult.first().getUid());

                    // Create update map removing the id to avoid the dbAdaptor exception
                    ObjectMap updateMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(externalTool));
                    updateMap.remove("id");
                    result = getWorkflowDBAdaptor(organizationId).update(tmpResult.first().getUid(), updateMap, QueryOptions.empty());
                } catch (JsonProcessingException e) {
                    throw new CatalogException("Internal error. Workflow '" + workflowId + "' already existed but it could not be updated",
                            e);
                }
            } else {
                // 3. We insert the workflow
                result = getWorkflowDBAdaptor(organizationId).insert(study.getUid(), externalTool, options);
            }

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                query = new Query()
                        .append(STUDY_UID.key(), study.getUid())
                        .append(UID.key(), externalTool.getUid());
                OpenCGAResult<ExternalTool> tmpTmpResult = getWorkflowDBAdaptor(organizationId).get(query, options);
                result.setResults(tmpTmpResult.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, externalTool.getId(), externalTool.getUuid(),
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, workflowId, "", studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @FunctionalInterface
    private interface CatalogExceptionThrowingConsumer<T> {
        void execute(T t) throws CatalogException;
    }

    public OpenCGAResult<ExternalTool> updateWorkflow(String studyStr, String externalToolId, WorkflowUpdateParams updateParams,
                                              QueryOptions options, String token) throws CatalogException {
        return privateUpdate(studyStr, externalToolId, updateParams, externalTool -> {
            if (externalTool.getType() != ExternalToolType.WORKFLOW) {
                throw new CatalogException("Only a tool of type " + ExternalToolType.WORKFLOW + " can be updated");
            }
            Workflow workflow = updateParams.getWorkflow();
            if (workflow == null) {
                return;
            }
            if (workflow.getManager() != null) {
                if (workflow.getManager().getId() == null) {
                    throw new CatalogException("Workflow manager id cannot be left empty.");
                }
                if (StringUtils.isEmpty(workflow.getManager().getVersion())) {
                    throw new CatalogException("Workflow manager version cannot be left empty.");
                }
            }
            if (CollectionUtils.isNotEmpty(workflow.getScripts()) && workflow.getRepository() != null) {
                throw new CatalogParameterException("Both workflow repository and scripts objects are provided. Please, choose one.");
            }
            if (CollectionUtils.isEmpty(externalTool.getWorkflow().getScripts())
                    && CollectionUtils.isNotEmpty(workflow.getScripts())) {
                throw new CatalogParameterException("Cannot add scripts to a workflow that was created with a repository.");
            }
            if (externalTool.getWorkflow().getRepository() == null && workflow.getRepository() != null) {
                throw new CatalogParameterException("Cannot add a repository to a workflow that was created with scripts.");
            }
            if (CollectionUtils.isNotEmpty(workflow.getScripts())) {
                boolean main = false;
                for (WorkflowScript script : workflow.getScripts()) {
                    ParamUtils.checkIdentifier(script.getName(), WORKFLOW_SCRIPTS.key() + ".id");
                    ParamUtils.checkParameter(script.getContent(), WORKFLOW_SCRIPTS.key() + ".content");
                    if (script.isMain()) {
                        if (main) {
                            throw new CatalogParameterException("More than one workflow main script found.");
                        }
                        main = script.isMain();
                    }
                }
                if (CollectionUtils.isNotEmpty(workflow.getScripts()) && !main) {
                    throw new CatalogParameterException("No workflow main script found.");
                }
            }
            if (workflow.getRepository() != null) {
                validateWorkflowRepository(workflow.getRepository());
            }
        }, options, token);
    }

    public OpenCGAResult<ExternalTool> updateCustomTool(String studyStr, String externalToolId, CustomToolUpdateParams updateParams,
                                                      QueryOptions options, String token) throws CatalogException {
        return privateUpdate(studyStr, externalToolId, updateParams, externalTool -> {
            if (externalTool.getType() != ExternalToolType.CUSTOM_TOOL) {
                throw new CatalogException("Only a tool of type " + ExternalToolType.CUSTOM_TOOL + " can be updated");
            }
            if (updateParams.getContainer() != null) {
                validateDocker(updateParams.getContainer());
            }
        }, options, token);
    }

    public OpenCGAResult<ExternalTool> updateVariantWalker(String studyStr, String externalToolId, CustomToolUpdateParams updateParams,
                                                        QueryOptions options, String token) throws CatalogException {
        return privateUpdate(studyStr, externalToolId, updateParams, externalTool -> {
            if (externalTool.getType() != ExternalToolType.VARIANT_WALKER) {
                throw new CatalogException("Only a tool of type " + ExternalToolType.VARIANT_WALKER + " can be updated");
            }
            if (updateParams.getContainer() != null) {
                validateDocker(updateParams.getContainer());
            }
        }, options, token);
    }

    public OpenCGAResult<ExternalTool> update(String studyStr, String externalToolId, ExternalToolUpdateParams updateParams,
                                              QueryOptions options, String token) throws CatalogException {
        return privateUpdate(studyStr, externalToolId, updateParams, (et) -> { }, options, token);
    }

    private OpenCGAResult<ExternalTool> privateUpdate(String studyStr, String externalToolId, ExternalToolUpdateParams updateParams,
                                                      CatalogExceptionThrowingConsumer<ExternalTool> validateFunction, QueryOptions options,
                                                      String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("externalToolId", externalToolId)
                .append("updateParams", updateParams)
                .append("options", options)
                .append("token", token);

        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        String studyId = studyFqn.getStudyId();
        String studyUuid = studyFqn.getStudyUuid();

        String id = UuidUtils.isOpenCgaUuid(externalToolId) ? "" : externalToolId;
        String uuid = UuidUtils.isOpenCgaUuid(externalToolId) ? externalToolId : "";
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
            studyId = study.getId();
            studyUuid = study.getUuid();

            ExternalTool externalTool = internalGet(organizationId, study.getUid(), Collections.singletonList(externalToolId), null,
                    QueryOptions.empty(), userId, false).first();
            id = externalTool.getId();
            uuid = externalTool.getUuid();

            // Check permission
            authorizationManager.checkExternalToolPermission(organizationId, study.getUid(), externalTool.getUid(), userId,
                    ExternalToolPermissions.WRITE);

            if (updateParams == null) {
                throw new CatalogException("Missing parameters to update the tool.");
            }
            validateFunction.execute(externalTool);

            if (updateParams.getMinimumRequirements() != null) {
                 validateMinimumRequirements(updateParams.getMinimumRequirements());
            }

            ObjectMap updateMap;
            try {
                updateMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams));
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse the UpdateParams object: " + e.getMessage(), e);
            }

            // 2. Update workflow object
            OpenCGAResult<ExternalTool> insert = getWorkflowDBAdaptor(organizationId).update(externalTool.getUid(), updateMap, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated workflow
                Query query = new Query()
                        .append(UID.key(), externalTool.getUid());
                OpenCGAResult<ExternalTool> result = getWorkflowDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, id, uuid, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, id, uuid, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void validateWorkflowRepository(WorkflowRepository repository) throws CatalogParameterException {
        if (repository == null) {
            return;
        }
        ParamUtils.checkParameter(repository.getName(), "workflow repository name");
        repository.setTag(ParamUtils.defaultString(repository.getTag(), ""));
        repository.setDescription(ParamUtils.defaultString(repository.getDescription(), ""));
        repository.setAuthor(ParamUtils.defaultString(repository.getAuthor(), ""));
        repository.setUser(ParamUtils.defaultString(repository.getUser(), ""));
        repository.setPassword(ParamUtils.defaultString(repository.getPassword(), ""));

        if ((StringUtils.isNotEmpty(repository.getUser()) && StringUtils.isEmpty(repository.getPassword()))
                || (StringUtils.isEmpty(repository.getUser()) && StringUtils.isNotEmpty(repository.getPassword()))) {
            throw new CatalogParameterException("User and password must be provided together.");
        }
    }

    private void validateDocker(Container container) throws CatalogParameterException {
        if (container == null) {
            throw new CatalogParameterException("Docker information is missing.");
        }
        ParamUtils.checkParameter(container.getName(), CONTAINER.key() + ".name");
        ParamUtils.checkParameter(container.getTag(), CONTAINER.key() + ".tag");
        container.setDigest(ParamUtils.defaultString(container.getDigest(), ""));
        container.setCommandLine(ParamUtils.defaultString(container.getCommandLine(), ""));
        container.setUser(ParamUtils.defaultString(container.getUser(), ""));
        container.setPassword(ParamUtils.defaultString(container.getPassword(), ""));
        if ((StringUtils.isEmpty(container.getPassword()) && StringUtils.isNotEmpty(container.getUser()))
                || (StringUtils.isNotEmpty(container.getPassword())
                && StringUtils.isEmpty(container.getUser()))) {
            throw new CatalogParameterException("Docker user and password must be set together.");
        }
    }

    @Override
    public DBIterator<ExternalTool> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Query finalQuery = new Query(query);
        fixQueryObject(finalQuery);
        finalQuery.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getWorkflowDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        Query finalQuery = new Query(query);
        fixQueryObject(finalQuery);
        finalQuery.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getWorkflowDBAdaptor(organizationId).facet(study.getUid(), finalQuery, facet, userId);
    }

    @Override
    public OpenCGAResult<ExternalTool> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
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
            query.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<ExternalTool> queryResult = getWorkflowDBAdaptor(organizationId).get(study.getUid(), query, options, userId);

            auditManager.auditSearch(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, studyId, studyUuid, auditParams,
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

            query.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = getWorkflowDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<ExternalTool> count(String studyStr, Query query, String token) throws CatalogException {
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

            query.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = getWorkflowDBAdaptor(organizationId).count(query, userId);

            auditManager.auditCount(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(organizationId, userId, Enums.Resource.EXTERNAL_TOOL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<ExternalTool> delete(String studyStr, List<String> ids, QueryOptions options, String token)
            throws CatalogException {
        return delete(studyStr, ids, options, false, token);
    }

    public OpenCGAResult<ExternalTool> delete(String studyStr, List<String> ids, ObjectMap params, boolean ignoreException, String token)
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
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.EXTERNAL_TOOL, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<ExternalTool> result = OpenCGAResult.empty(ExternalTool.class);
        for (String id : ids) {
            String workflowId = id;
            String workflowUuid = "";
            try {
                OpenCGAResult<ExternalTool> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Workflow '" + id + "' not found");
                }
                ExternalTool externalTool = internalResult.first();

                // We set the proper values for the audit
                workflowId = externalTool.getId();
                workflowUuid = externalTool.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkExternalToolPermission(organizationId, study.getUid(), externalTool.getUid(), userId,
                            ExternalToolPermissions.DELETE);
                }

                // Check if the workflow can be deleted
                checkWorkflowCanBeDeleted(organizationId, study.getUid(), externalTool, params.getBoolean(Constants.FORCE, false));

                result.append(getWorkflowDBAdaptor(organizationId).delete(externalTool));

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.EXTERNAL_TOOL, externalTool.getId(),
                        externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete workflow " + workflowId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, workflowId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.EXTERNAL_TOOL, workflowId, workflowUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    private void checkWorkflowCanBeDeleted(String organizationId, long uid, ExternalTool externalTool, boolean force) {
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
        DBIterator<ExternalTool> iterator;
        try {
            fixQueryObject(finalQuery);
            finalQuery.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getWorkflowDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.EXTERNAL_TOOL, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            ExternalTool externalTool = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkExternalToolPermission(organizationId, study.getUid(), externalTool.getUid(), userId,
                            ExternalToolPermissions.DELETE);
                }

                // Check if the workflow can be deleted
                checkWorkflowCanBeDeleted(organizationId, study.getUid(), externalTool, params.getBoolean(Constants.FORCE, false));

                result.append(getWorkflowDBAdaptor(organizationId).delete(externalTool));

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.EXTERNAL_TOOL, externalTool.getId(),
                        externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete workflow " + externalTool.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, externalTool.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.EXTERNAL_TOOL, externalTool.getId(),
                        externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
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

    protected void fixQueryObject(Query query) {
        super.fixQueryObject(query);
        if (query.containsKey(ParamConstants.EXTERNAL_TOOL_CONTAINER_NAME_PARAM)) {
            query.put(ExternalToolDBAdaptor.QueryParams.CONTAINER_NAME.key(), query.get(ParamConstants.EXTERNAL_TOOL_CONTAINER_NAME_PARAM));
            query.remove(ParamConstants.EXTERNAL_TOOL_CONTAINER_NAME_PARAM);
        }
        if (query.containsKey(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM)) {
            query.put(WORKFLOW_REPOSITORY_NAME.key(), query.get(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM));
            query.remove(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM);
        }
    }

    private ExternalToolDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        ExternalToolDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            ExternalToolDBAdaptor.QueryParams param = ID;
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

    private void validateNewWorkflow(ExternalTool externalTool, String userId) throws CatalogParameterException {
        ParamUtils.checkObj(externalTool.getWorkflow(), WORKFLOW.key());
        if (externalTool.getWorkflow().getManager() == null) {
            externalTool.getWorkflow().setManager(new WorkflowSystem());
        }
        if (externalTool.getWorkflow().getManager().getId() == null) {
            externalTool.getWorkflow().getManager().setId(WorkflowSystem.SystemId.NEXTFLOW);
        }
        externalTool.getWorkflow().setScripts(externalTool.getWorkflow().getScripts() != null
                ? externalTool.getWorkflow().getScripts()
                : Collections.emptyList());
        boolean main = false;
        for (WorkflowScript script : externalTool.getWorkflow().getScripts()) {
            ParamUtils.checkIdentifier(script.getName(), WORKFLOW_SCRIPTS.key() + ".id");
            ParamUtils.checkParameter(script.getContent(), WORKFLOW_SCRIPTS.key() + ".content");
            if (script.isMain()) {
                if (main) {
                    throw new CatalogParameterException("More than one main script found.");
                }
                main = script.isMain();
            }
        }
        if (CollectionUtils.isNotEmpty(externalTool.getWorkflow().getScripts()) && !main) {
            throw new CatalogParameterException("No main script found.");
        }
        externalTool.getWorkflow().setRepository(externalTool.getWorkflow().getRepository() != null
                ? externalTool.getWorkflow().getRepository()
                : new WorkflowRepository());
        if (CollectionUtils.isEmpty(externalTool.getWorkflow().getScripts())) {
            // If scripts are not provided, repository must be provided
            validateWorkflowRepository(externalTool.getWorkflow().getRepository());
        }
        if (StringUtils.isEmpty(externalTool.getWorkflow().getRepository().getName())
                && CollectionUtils.isEmpty(externalTool.getWorkflow().getScripts())) {
            throw new CatalogParameterException("No repository image or scripts found.");
        }
        if (StringUtils.isNotEmpty(externalTool.getWorkflow().getRepository().getName())
                && CollectionUtils.isNotEmpty(externalTool.getWorkflow().getScripts())) {
            throw new CatalogParameterException("Both repository image and scripts found. Please, either add scripts or a repository"
                    + " image.");
        }

        externalTool.setContainer(null);
        validateNewExternalTool(externalTool, userId);
    }

    private void validateNewCustomTool(ExternalTool externalTool, String userId) throws CatalogParameterException {
        ParamUtils.checkObj(externalTool.getContainer(), CONTAINER.key());
        validateDocker(externalTool.getContainer());
        externalTool.setWorkflow(null);
        validateNewExternalTool(externalTool, userId);
    }

    private void validateNewExternalTool(ExternalTool externalTool, String userId) throws CatalogParameterException {
        ParamUtils.checkIdentifier(externalTool.getId(), ID.key());
        if (externalTool.getWorkflow() == null && externalTool.getContainer() == null) {
            throw new CatalogParameterException("Missing expected workflow or docker object");
        }
        if (externalTool.getWorkflow() != null && externalTool.getContainer() != null) {
            throw new CatalogParameterException("Both workflow and docker objects found. Please, choose one.");
        }
        externalTool.setScope(ParamUtils.defaultObject(externalTool.getScope(), ExternalToolScope.OTHER));
        externalTool.setTags(externalTool.getTags() != null
                ? externalTool.getTags()
                : Collections.emptyList());

        externalTool.setName(ParamUtils.defaultString(externalTool.getName(), externalTool.getId()));
        externalTool.setDescription(ParamUtils.defaultString(externalTool.getDescription(), ""));
        externalTool.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EXTERNAL_TOOL));
        externalTool.setVersion(1);
        externalTool.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(externalTool.getCreationDate(), CREATION_DATE.key()));
        externalTool.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(externalTool.getModificationDate(), MODIFICATION_DATE.key()));
        externalTool.setAttributes(ParamUtils.defaultObject(externalTool.getAttributes(), Collections.emptyMap()));
        externalTool.setInternal(new ExternalToolInternal(new InternalStatus(InternalStatus.READY), TimeUtils.getTime(),
                TimeUtils.getTime(), userId));

        if (externalTool.getMinimumRequirements() != null) {
            validateMinimumRequirements(externalTool.getMinimumRequirements());
        }
    }

    private static void validateMinimumRequirements(MinimumRequirements minimumRequirements) throws CatalogParameterException {
        ParamUtils.checkParameter(minimumRequirements.getCpu(), "minimumRequirements.cpu");
        ParamUtils.checkParameter(minimumRequirements.getMemory(), "minimumRequirements.memory");
        // CPU should be a positive number and should be able to cast to Number
        try {
            double cpu = Double.parseDouble(minimumRequirements.getCpu());
            if (cpu <= 0) {
                throw new CatalogParameterException("Minimum requirements CPU should be a positive number.");
            }
        } catch (NumberFormatException e) {
            throw new CatalogParameterException("Minimum requirements CPU should be a positive number.", e);
        }
        // Memory should be a positive number optionally followed by a unit (MB or GB)
        // Supported formats: "16", "16GB", "16 GB", "16.GB", "16.5", "16.5GB", "16.5 GB"
        // If memory is only a positive number, we will add GB as the unit
        String memory = minimumRequirements.getMemory().trim();
        List<String> validUnits = Arrays.asList("MB", "GB", "MIB", "GIB");

        String numericPart = null;
        String unit = null;

        // First, try to find unit at the end (without separator)
        String upperMemory = memory.toUpperCase();
        for (String validUnit : validUnits) {
            if (upperMemory.endsWith(validUnit)) {
                numericPart = memory.substring(0, memory.length() - validUnit.length());
                unit = validUnit;
                // Check if there's a space or dot separator before the unit
                if (numericPart.endsWith(" ") || numericPart.endsWith(".")) {
                    numericPart = numericPart.substring(0, numericPart.length() - 1).trim();
                }
                break;
            }
        }

        if (numericPart == null) {
            // No valid unit found. Check if there's an invalid unit suffix.
            // Look for a pattern where the string ends with letters
            String potentialNumeric = memory;
            String potentialUnit = null;

            // Find where the numeric part ends and letters start
            int i = memory.length() - 1;
            while (i >= 0 && Character.isLetter(memory.charAt(i))) {
                i--;
            }

            if (i < memory.length() - 1) {
                // Found letters at the end
                potentialNumeric = memory.substring(0, i + 1).trim();
                potentialUnit = memory.substring(i + 1).trim();

                // Remove trailing space or dot from numeric part
                if (potentialNumeric.endsWith(" ") || potentialNumeric.endsWith(".")) {
                    potentialNumeric = potentialNumeric.substring(0, potentialNumeric.length() - 1).trim();
                }

                // Check if the unit part is invalid
                if (!potentialUnit.isEmpty() && !validUnits.contains(potentialUnit.toUpperCase())) {
                    throw new CatalogParameterException("Minimum requirements memory unit '" + potentialUnit + "' is not valid. "
                            + "Supported units are: " + String.join(", ", validUnits) + ".");
                }
            }

            numericPart = potentialNumeric;
        }

        try {
            double memoryValue = Double.parseDouble(numericPart);
            if (memoryValue <= 0) {
                throw new CatalogParameterException("Minimum requirements memory should be a positive number.");
            }
            if (unit == null) {
                // No unit provided, we will add GB
                minimumRequirements.setMemory(memoryValue + "GB");
            }
        } catch (NumberFormatException e) {
            throw new CatalogParameterException("Minimum requirements memory should be a positive number.", e);
        }
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<ExternalToolPermissions>> getAcls(String studyId, List<String> workflowList, String member,
                                                                        boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, workflowList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<ExternalToolPermissions>> getAcls(String studyStr, List<String> workflowList, List<String> members,
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

        OpenCGAResult<AclEntryList<ExternalToolPermissions>> workflowAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<ExternalTool> queryResult = internalGet(organizationId, study.getUid(), workflowList,
                    INCLUDE_IDS, userId, ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> workflowUids = queryResult.getResults().stream().map(ExternalTool::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                workflowAcls = authorizationManager.getAcl(organizationId, study.getUid(), workflowUids, members,
                        Enums.Resource.EXTERNAL_TOOL, ExternalToolPermissions.class, userId);
            } else {
                workflowAcls = authorizationManager.getAcl(organizationId, study.getUid(), workflowUids, Enums.Resource.EXTERNAL_TOOL,
                        ExternalToolPermissions.class, userId);
            }

            // Include non-existing samples to the result list
            List<AclEntryList<ExternalToolPermissions>> resultList = new ArrayList<>(workflowList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String workflowId : workflowList) {
                if (!missingMap.containsKey(workflowId)) {
                    ExternalTool externalTool = queryResult.getResults().get(counter);
                    resultList.add(workflowAcls.getResults().get(counter));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.EXTERNAL_TOOL,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, workflowId, missingMap.get(workflowId).getErrorMsg()));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.EXTERNAL_TOOL,
                            workflowId, "", study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
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
                auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.EXTERNAL_TOOL, workflowId,
                        "", study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
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

    public OpenCGAResult<AclEntryList<ExternalToolPermissions>> updateAcl(String studyStr, String memberList,
                                                                          ExternalToolAclUpdateParams params, ParamUtils.AclAction action,
                                                                          String token) throws CatalogException {
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
        List<ExternalTool> externalToolList;
        try {
            auditManager.initAuditBatch(operationId);

            ParamUtils.checkObj(params, "WorkflowAclUpdateParams");

            if (CollectionUtils.isEmpty(params.getExternalToolIds())) {
                throw new CatalogException("Update ACL: No workflows provided to be updated.");
            }
            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }
            if (CollectionUtils.isNotEmpty(params.getPermissions())) {
                checkPermissions(params.getPermissions(), ExternalToolPermissions::valueOf);
            }

            externalToolList = internalGet(organizationId, study.getUid(), params.getExternalToolIds(), INCLUDE_IDS, userId, false)
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
            if (CollectionUtils.isNotEmpty(params.getExternalToolIds())) {
                for (String workflowId : params.getExternalToolIds()) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.EXTERNAL_TOOL,
                            workflowId, "", study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            auditManager.finishAuditBatch(organizationId, operationId);
            throw e;
        }

        OpenCGAResult<AclEntryList<ExternalToolPermissions>> aclResultList = OpenCGAResult.empty();
        int numProcessed = 0;
        do {
            List<ExternalTool> batchExternalToolList = new ArrayList<>();
            while (numProcessed < Math.min(numProcessed + BATCH_OPERATION_SIZE, externalToolList.size())) {
                batchExternalToolList.add(externalToolList.get(numProcessed));
                numProcessed += 1;
            }

            List<Long> workflowUids = batchExternalToolList.stream().map(ExternalTool::getUid).collect(Collectors.toList());
            List<String> workflowIds = batchExternalToolList.stream().map(ExternalTool::getId).collect(Collectors.toList());
            List<AuthorizationManager.CatalogAclParams> aclParamsList = new ArrayList<>();
            AuthorizationManager.CatalogAclParams.addToList(workflowUids, params.getPermissions(), Enums.Resource.EXTERNAL_TOOL,
                    aclParamsList);

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

                OpenCGAResult<AclEntryList<ExternalToolPermissions>> queryResults = authorizationManager.getAcls(organizationId,
                        study.getUid(),
                        workflowUids, members, Enums.Resource.EXTERNAL_TOOL, ExternalToolPermissions.class);

                for (int i = 0; i < queryResults.getResults().size(); i++) {
                    queryResults.getResults().get(i).setId(workflowIds.get(i));
                }
                aclResultList.append(queryResults);

                for (ExternalTool externalTool : batchExternalToolList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.EXTERNAL_TOOL,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                }
            } catch (CatalogException e) {
                // Process current batch
                for (ExternalTool externalTool : batchExternalToolList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.EXTERNAL_TOOL,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                // Process remaining unprocessed batches
                while (numProcessed < externalToolList.size()) {
                    ExternalTool externalTool = externalToolList.get(numProcessed);
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.EXTERNAL_TOOL,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                auditManager.finishAuditBatch(organizationId, operationId);
                throw e;
            }
        } while (numProcessed < externalToolList.size());

        auditManager.finishAuditBatch(organizationId, operationId);
        return aclResultList;
    }

}
