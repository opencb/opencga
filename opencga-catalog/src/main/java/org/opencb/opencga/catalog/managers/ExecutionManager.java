package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

public class ExecutionManager extends ResourceManager<Execution> {

    protected static Logger logger = LoggerFactory.getLogger(ExecutionManager.class);
    private final UserManager userManager;
    private final StudyManager studyManager;
    private final IOManagerFactory ioManagerFactory;

    // TODO: Point to execution variables
    public static final QueryOptions INCLUDE_EXECUTION_IDS = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(ExecutionDBAdaptor.QueryParams.ID.key(), ExecutionDBAdaptor.QueryParams.UID.key(),
                    ExecutionDBAdaptor.QueryParams.UUID.key(), ExecutionDBAdaptor.QueryParams.STUDY_UID.key(),
                    ExecutionDBAdaptor.QueryParams.INTERNAL.key()));
    public static final QueryOptions INCLUDE_EXECUTION_AND_JOB_IDS = keepFieldsInQueryOptions(INCLUDE_EXECUTION_IDS, Arrays.asList(
            ExecutionDBAdaptor.QueryParams.JOBS.key() + "." + JobDBAdaptor.QueryParams.ID.key(),
            ExecutionDBAdaptor.QueryParams.JOBS.key() + "." + JobDBAdaptor.QueryParams.UID.key()));

    ExecutionManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                     DBAdaptorFactory catalogDBAdaptorFactory, IOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
        this.ioManagerFactory = ioManagerFactory;
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.EXECUTION;
    }

    @Override
    InternalGetDataResult<Execution> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                 String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing execution entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);

        Function<Execution, String> executionStringFunction = Execution::getId;
        ExecutionDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            ExecutionDBAdaptor.QueryParams param = ExecutionDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = ExecutionDBAdaptor.QueryParams.UUID;
                executionStringFunction = Execution::getUuid;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        if (studyUid <= 0) {
            if (!idQueryParam.equals(ExecutionDBAdaptor.QueryParams.UUID)) {
                // studyUid is mandatory except for uuids
                throw new CatalogException("Missing mandatory study");
            }

            // If studyUid has not been provided, we will look for
            OpenCGAResult<Execution> executionDataResult = executionDBAdaptor.get(queryCopy, options);
            for (Execution execution : executionDataResult.getResults()) {
                // TODO: check Execution permissions?
                // Check view permissions
                authorizationManager.checkExecutionPermission(execution.getStudyUid(), execution.getUid(), user,
                        ExecutionAclEntry.ExecutionPermissions.VIEW);
            }
            return keepOriginalOrder(uniqueList, executionStringFunction, executionDataResult, ignoreException, false);
        }

        queryCopy.put(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        OpenCGAResult<Execution> executionDataResult = executionDBAdaptor.get(studyUid, queryCopy, options, user);
        if (ignoreException || executionDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, executionStringFunction, executionDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<Execution> resultsNoCheck = executionDBAdaptor.get(queryCopy, queryOptions);
        if (resultsNoCheck.getNumResults() == executionDataResult.getNumResults()) {
            throw CatalogException.notFound("executions", getMissingFields(uniqueList, executionDataResult.getResults(),
                    executionStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user
                    + " is not allowed to see some or none of the executions.");
        }
    }

    @Override
    public OpenCGAResult<Execution> create(String studyStr, Execution entry, QueryOptions options, String token) throws CatalogException {
        throw new NotImplementedException("Create operation not implemented. Please, use submit instead.");
    }

    public OpenCGAResult<Execution> register(String studyStr, ExecutionCreateParams entry, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("execution", entry)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_EXECUTIONS);

            ParamUtils.checkObj(entry, ExecutionCreateParams.class.getName());
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            String user = StringUtils.isNotEmpty(entry.getUserId()) ? entry.getUserId() : userId;

            // Validate toolId
            ParamUtils.checkObj(entry.getInternal(), "internal");
            ParamUtils.checkParameter(entry.getInternal().getToolId(), "toolId");
            ParamUtils.checkObj(entry.getInternal().getStatus(), ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS.key());
            String toolId = entry.getInternal().getToolId();

            // Validate execution status
            Set<String> allowedStatuses = new HashSet<>(Arrays.asList(Enums.ExecutionStatus.DONE, Enums.ExecutionStatus.ERROR,
                    Enums.ExecutionStatus.ABORTED));
            if (!allowedStatuses.contains(entry.getInternal().getStatus().getId())) {
                throw CatalogException.notValidStatus("Execution", entry.getInternal().getStatus().getId(), allowedStatuses);
            }

            // Validate jobs
            if (CollectionUtils.isEmpty(entry.getJobs())) {
                throw new CatalogException("The executions needs to contain at least 1 job");
            }
            List<Job> jobs = new LinkedList<>();
            for (JobCreateParams jobCreateParam : entry.getJobs()) {
                ParamUtils.checkObj(jobCreateParam.getInternal(), "jobs.internal");
                ParamUtils.checkObj(jobCreateParam.getInternal().getStatus(), "jobs.internal.status");
                Job job = jobCreateParam.toJob();
                job.setUserId(user);
                try {
                    catalogManager.getJobManager().autoCompleteNewJob(study, job);
                } catch (CatalogException e) {
                    throw new CatalogParameterException("Job: " + e.getMessage(), e);
                }
                job.getInternal().setStatus(jobCreateParam.getInternal().getStatus());
                if (!allowedStatuses.contains(job.getInternal().getStatus().getId())) {
                    throw CatalogException.notValidStatus("Job", job.getInternal().getStatus().getId(), allowedStatuses);
                }
                jobs.add(job);
            }

            // Extract pipeline object
            Pipeline pipeline = null;
            if (entry.getPipeline() != null && StringUtils.isNotEmpty(entry.getPipeline().getId())) {
                pipeline = catalogManager.getPipelineManager().get(studyStr, entry.getPipeline().getId(), QueryOptions.empty(), token)
                        .first();
            }

            // Extract dependsOn executions
            List<String> dependsOn = entry.getDependsOn() != null
                    ? entry.getDependsOn().stream().map(EntryParam::getId).collect(Collectors.toList())
                    : Collections.emptyList();

            Execution execution = new Execution(entry.getId(), entry.getDescription(), user, entry.getCreationDate(),
                    entry.getModificationDate(), entry.getPriority(), entry.getInternal(), entry.getParams(), null, entry.getTags(),
                    null, pipeline, pipeline != null, entry.isVisited(), jobs, entry.getAttributes());
            autoCompleteNewExecution(study, execution, toolId, dependsOn, token);
            execution.getInternal().setStatus(entry.getInternal().getStatus());

            OpenCGAResult<Execution> insert = executionDBAdaptor.insert(study.getUid(), execution, new QueryOptions());
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created execution
                OpenCGAResult<Execution> executionResult = executionDBAdaptor.get(execution.getUid(), new QueryOptions());
                insert.setResults(executionResult.getResults());
            }

            auditManager.auditCreate(userId, Enums.Resource.EXECUTION, execution.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return insert;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.REGISTER, Enums.Resource.EXECUTION, entry.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

//
//    public OpenCGAResult<Execution> submitTool(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params,
//                                               String token) throws CatalogException {
//        return submitTool(studyStr, toolId, priority, params, null, null, null, null, token);
//    }
//
//    public OpenCGAResult<Execution> submitProject(String projectStr, String toolId, Enums.Priority priority, Map<String, Object> params,
//                                                  String jobId, String jobDescription, List<String> jobDependsOn, List<String> jobTags,
//                                                  String token) throws CatalogException {
//        // Project job
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key());
//        // Peek any study. The ExecutionDaemon will take care of filling up the rest of studies.
//        List<String> studies = catalogManager.getStudyManager()
//                .search(projectStr, new Query(), options, token)
//                .getResults()
//                .stream()
//                .map(Study::getFqn)
//                .collect(Collectors.toList());
//        if (studies.isEmpty()) {
//            throw new CatalogException("Project '" + projectStr + "' not found!");
//        }
//        return submitTool(studies.get(0), toolId, priority, params, jobId, jobDescription, jobDependsOn, jobTags, token);
//    }

    public OpenCGAResult<Execution> retry(String studyStr, ExecutionRetryParams executionRetry, Enums.Priority priority,
                                          String executionId, String description, List<String> dependsOn, List<String> tags, String token)
            throws CatalogException {
        Execution execution = get(studyStr, executionRetry.getExecution(), new QueryOptions(), token).first();
        if (execution.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ERROR)
                || execution.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ABORTED)) {
            return submit(studyStr, execution.getInternal().getToolId(), priority, execution.getParams(), executionId, description,
                    dependsOn, tags, token);
        } else {
            throw new CatalogException("Unable to retry execution with status " + execution.getInternal().getStatus().getId());
        }
    }

    public OpenCGAResult<Execution> submitProject(String projectStr, String resourceId, Enums.Priority priority, Map<String, Object> params,
                                                  String id, String description, List<String> dependsOn, List<String> tags, String token)
            throws CatalogException {
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
        return submit(studies.get(0), resourceId, priority, params, id, description, dependsOn, tags, token);
    }

    public OpenCGAResult<Execution> submit(String studyStr, String resourceId, Enums.Priority priority, Map<String, Object> params,
                                           String token) throws CatalogException {
        return submit(studyStr, resourceId, priority, params, null, null, null, null, token);
    }

    public OpenCGAResult<Execution> submit(String studyStr, String resourceId, Enums.Priority priority, Map<String, Object> params,
                                           String id, String description, List<String> dependsOn, List<String> tags, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("resourceId", resourceId)
                .append("priority", priority)
                .append("params", params)
                .append("id", id)
                .append("description", description)
                .append("dependsOn", dependsOn)
                .append("tags", tags)
                .append("token", token);

        Execution execution = new Execution(id, description, userId, TimeUtils.getTime(), params, priority, tags, null, false,
                new ObjectMap());
        execution.setStudy(new JobStudyParam(study.getFqn()));
        execution.setUserId(userId);
        execution.setPriority(priority);

        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.EXECUTE_JOBS);
            autoCompleteNewExecution(study, execution, resourceId, dependsOn, token);

            executionDBAdaptor.insert(study.getUid(), execution, new QueryOptions());
            OpenCGAResult<Execution> executionResult = executionDBAdaptor.get(execution.getUid(), new QueryOptions());
            executionResult.setNumInserted(1);

            auditManager.auditCreate(userId, Enums.Resource.EXECUTION, execution.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return executionResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.EXECUTION, execution.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));

            if (execution.getInternal() != null) {
                execution.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED));
            } else {
                execution.setInternal(new ExecutionInternal(resourceId, TimeUtils.getTime(), TimeUtils.getTime(),
                        new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED),
                        new JobInternalWebhook(null, new HashMap<>()), Collections.emptyList()));
            }
            execution.getInternal().getStatus().setDescription(e.toString());
            executionDBAdaptor.insert(study.getUid(), execution, new QueryOptions());

            throw e;
        }
    }

    private void autoCompleteNewExecution(Study study, Execution execution, String toolId, List<String> dependsOn, String token)
            throws CatalogException {
        ParamUtils.checkObj(execution, "Execution");

        // Auto generate id
        if (StringUtils.isEmpty(execution.getId())) {
            if (execution.getPipeline() != null) {
                ParamUtils.checkObj(execution.getPipeline(), "Pipeline");
                execution.setId("p--" + execution.getPipeline().getId() + "." + TimeUtils.getTime() + "."
                        + RandomStringUtils.randomAlphanumeric(6));
            } else {
                ParamUtils.checkParameter(toolId, "toolId");
                execution.setId(toolId + "." + TimeUtils.getTime() + "." + RandomStringUtils.randomAlphanumeric(6));
            }
        }

        // Check params
        ParamUtils.checkObj(execution.getParams(), "params");
        for (Map.Entry<String, Object> entry : execution.getParams().entrySet()) {
            if (entry.getValue() == null) {
                throw new CatalogException("Found '" + entry.getKey() + "' param with null value");
            }
        }

        ParamUtils.checkParameter(execution.getUserId(), "userId");

        execution.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EXECUTION));
        execution.setDescription(ParamUtils.defaultString(execution.getDescription(), ""));
        execution.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(execution.getCreationDate(),
                ExecutionDBAdaptor.QueryParams.CREATION_DATE.key()));
        execution.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(execution.getModificationDate(),
                ExecutionDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        execution.setPriority(ParamUtils.defaultObject(execution.getPriority(), Enums.Priority.MEDIUM));
        execution.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));

        // Set default internal
        execution.setInternal(ExecutionInternal.init());
        execution.getInternal().setToolId(toolId);
        execution.getInternal().setWebhook(new JobInternalWebhook(study.getNotification().getWebhook(), new HashMap<>()));

        execution.setTags(ParamUtils.defaultObject(execution.getTags(), Collections::emptyList));
        execution.setDependsOn(ParamUtils.defaultObject(execution.getDependsOn(), Collections::emptyList));
        execution.setStudy(new JobStudyParam(study.getFqn()));
        execution.setAttributes(ParamUtils.defaultObject(execution.getAttributes(), HashMap::new));

        if (CollectionUtils.isNotEmpty(dependsOn)) {
            boolean uuidProvided = dependsOn.stream().anyMatch(UuidUtils::isOpenCgaUuid);
            try {
                // If uuid is provided, we will remove the study uid from the query so it can be searched across any study
                InternalGetDataResult<Execution> dependsOnResult;
                if (uuidProvided) {
                    dependsOnResult = internalGet(0, dependsOn, null, INCLUDE_EXECUTION_IDS, execution.getUserId(), false);
                } else {
                    dependsOnResult = internalGet(study.getUid(), dependsOn, null, INCLUDE_EXECUTION_IDS, execution.getUserId(), false);
                }
                execution.setDependsOn(dependsOnResult.getResults());
            } catch (CatalogException e) {
                throw new CatalogException("Unable to find the executions this execution depends on. " + e.getMessage(), e);
            }

        }
    }

    public OpenCGAResult count(Query query, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        authorizationManager.isInstallationAdministrator(userId);

        return executionDBAdaptor.count(query);
    }

    /**
     * Update Execution from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param executionIds       List of execution ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
     * @return A OpenCGAResult with the objects updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Execution> update(String studyStr, List<String> executionIds, ExecutionUpdateParams updateParams,
                                           QueryOptions options, String token) throws CatalogException {
        return update(studyStr, executionIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Execution> update(String studyStr, List<String> executionIds, ExecutionUpdateParams updateParams,
                                           boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("executionIds", executionIds)
                .append("updateParams", updateParams)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Execution> result = OpenCGAResult.empty();
        for (String id : executionIds) {
            String executionId = id;
            String executionUuid = "";

            try {
                OpenCGAResult<Execution> internalResult = internalGet(study.getUid(), id, INCLUDE_EXECUTION_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }
                Execution execution = internalResult.first();

                // We set the proper values for the audit
                executionId = execution.getId();
                executionUuid = execution.getUuid();

                OpenCGAResult<Execution> updateResult = update(study, execution, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.EXECUTION, execution.getId(), execution.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, executionId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update execution {}: {}", executionId, e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.EXECUTION, executionId, executionUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    /**
     * Update fields of a Execution.
     *
     * @param studyStr    Study id.
     * @param executionId Execution id.
     * @param params      Update params.
     * @param token       token.
     * @return an OpenCGAResult.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<Execution> update(String studyStr, String executionId, ExecutionUpdateParams params, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("executionId", executionId)
                .append("params", params)
                .append("token", token);
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            Execution execution = internalGet(study.getUid(), executionId, INCLUDE_EXECUTION_IDS, userId).first();

            OpenCGAResult<Execution> update = update(study, execution, params, QueryOptions.empty(), userId);
            auditManager.auditUpdate(userId, Enums.Resource.EXECUTION, execution.getId(), execution.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return update;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.EXECUTION, executionId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private OpenCGAResult<Execution> update(Study study, Execution execution, ExecutionUpdateParams params, QueryOptions options,
                                           String userId) throws CatalogException {
        if (params == null) {
            throw new CatalogException("Missing parameters to update");
        }

        ObjectMap updateMap;
        try {
            String jsonString = JacksonUtils.getDefaultNonNullObjectMapper().writeValueAsString(params);
            updateMap = JacksonUtils.getDefaultNonNullObjectMapper().readValue(jsonString, ObjectMap.class);
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse ExecutionUpdateParams object: " + e.getMessage(), e);
        }
        if (updateMap.isEmpty()) {
            throw new CatalogException("Missing parameters to update");
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        authorizationManager.checkExecutionPermission(study.getUid(), execution.getUid(), userId,
                ExecutionAclEntry.ExecutionPermissions.WRITE);

        OpenCGAResult<Execution> update = executionDBAdaptor.update(execution.getUid(), updateMap, options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated execution
            OpenCGAResult<Execution> result = executionDBAdaptor.get(study.getUid(),
                    new Query(JobDBAdaptor.QueryParams.UID.key(), execution.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
    }

    /**
     * Update Internal field of Execution. Only opencga user is allowed to use this method.
     *
     * @param studyStr    Study id.
     * @param executionId Execution id.
     * @param params      Update params.
     * @param token       token.
     * @return an OpenCGAResult.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<Execution> privateUpdate(String studyStr, String executionId, PrivateExecutionUpdateParams params, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("executionId", executionId)
                .append("params", params)
                .append("token", token);
        try {
            authorizationManager.checkIsInstallationAdministrator(userId);
            ParamUtils.checkObj(params, "PrivateExecutionUpdateParams");

            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            Execution execution = internalGet(study.getUid(), executionId, INCLUDE_EXECUTION_IDS, userId).first();

            ObjectMap updateMap;
            try {
                String jsonString = JacksonUtils.getDefaultNonNullObjectMapper().writeValueAsString(params);
                updateMap = JacksonUtils.getDefaultNonNullObjectMapper().readValue(jsonString, ObjectMap.class);
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse PrivateExecutionUpdateParams object: " + e.getMessage(), e);
            }

            OpenCGAResult<Execution> update = executionDBAdaptor.update(execution.getUid(), updateMap, QueryOptions.empty());
            auditManager.auditUpdate(userId, Enums.Resource.EXECUTION, execution.getId(), execution.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return update;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.EXECUTION, executionId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }


    public OpenCGAResult<ExecutionTop> top(Query baseQuery, int limit, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        List<String> studies = studyManager.search(new Query(StudyDBAdaptor.QueryParams.OWNER.key(), userId),
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UUID.key()), token).getResults()
                .stream()
                .map(Study::getUuid)
                .collect(Collectors.toList());
        return top(studies, baseQuery, limit, token);
    }

    public OpenCGAResult<ExecutionTop> top(String studyStr, Query baseQuery, int limit, String token) throws CatalogException {
        if (StringUtils.isEmpty(studyStr)) {
            return top(baseQuery, limit, token);
        } else {
            return top(Collections.singletonList(studyStr), baseQuery, limit, token);
        }
    }

    public OpenCGAResult<ExecutionTop> top(List<String> studiesStr, Query baseQuery, int limit, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        fixQueryObject(baseQuery);
        List<Study> studies = new ArrayList<>(studiesStr.size());
        for (String studyStr : studiesStr) {
            Study study = studyManager.resolveId(studyStr, userId);
            authorizationManager.checkCanViewStudy(study.getUid(), userId);
            studies.add(study);
        }

        StopWatch stopWatch = StopWatch.createStarted();
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(ExecutionDBAdaptor.QueryParams.ID.key(),
                        ExecutionDBAdaptor.QueryParams.INTERNAL.key(), ExecutionDBAdaptor.QueryParams.DEPENDS_ON.key(),
                        ExecutionDBAdaptor.QueryParams.CREATION_DATE.key(), ExecutionDBAdaptor.QueryParams.PRIORITY.key(),
                        ExecutionDBAdaptor.QueryParams.STUDY.key()))
                .append(QueryOptions.COUNT, false)
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

        int executionsLimit = limit;
        List<Execution> running = new ArrayList<>(executionsLimit);
        for (Study study : studies) {
            if (executionsLimit == 0) {
                break;
            }
            List<Execution> results = executionDBAdaptor.get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.RUNNING),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, executionsLimit)
                            .append(QueryOptions.SORT, "execution.start"),
                    userId).getResults();
            running.addAll(results);
        }
//        running.sort(Comparator.comparing(
//                j -> j.getExecution() == null || j.getExecution().getStart() == null
//                        ? new Date()
//                        : j.getExecution().getStart()));
        if (running.size() > executionsLimit) {
            running = running.subList(0, executionsLimit);
        }
        executionsLimit -= running.size();

        List<Execution> queued = new ArrayList<>(executionsLimit);
        for (Study study : studies) {
            if (executionsLimit == 0) {
                break;
            }
            List<Execution> results = executionDBAdaptor.get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.QUEUED),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, executionsLimit)
                            .append(QueryOptions.SORT, ExecutionDBAdaptor.QueryParams.CREATION_DATE.key()),
                    userId).getResults();
            queued.addAll(results);
        }
        queued.sort(Comparator.comparing(Execution::getCreationDate));
        if (queued.size() > executionsLimit) {
            queued = queued.subList(0, executionsLimit);
        }
        executionsLimit -= queued.size();

        List<Execution> pending = new ArrayList<>(executionsLimit);
        for (Study study : studies) {
            if (executionsLimit == 0) {
                break;
            }
            List<Execution> results = executionDBAdaptor.get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.PENDING),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, executionsLimit)
                            .append(QueryOptions.SORT, ExecutionDBAdaptor.QueryParams.CREATION_DATE.key()),
                    userId).getResults();
            pending.addAll(results);
        }
        pending.sort(Comparator.comparing(Execution::getCreationDate));
        if (pending.size() > executionsLimit) {
            pending = pending.subList(0, executionsLimit);
        }
        executionsLimit -= pending.size();

        List<Execution> finishedExecutions = new ArrayList<>(executionsLimit);
        for (Study study : studies) {
            if (executionsLimit == 0) {
                break;
            }
            List<Execution> results = executionDBAdaptor.get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.DONE + ","
                                    + Enums.ExecutionStatus.ERROR + ","
                                    + Enums.ExecutionStatus.ABORTED),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, executionsLimit)
                            .append(QueryOptions.SORT, "execution.end")
                            .append(QueryOptions.ORDER, QueryOptions.DESCENDING), // Get last n elements,
                    userId).getResults();
            Collections.reverse(results); // Reverse elements
            finishedExecutions.addAll(results);
        }
//        finishedExecutions.sort(Comparator.comparing((Execution j) -> j.getExecution() == null || j.getExecution().getStart() == null
//                ? new Date()
//                : j.getExecution().getStart()).reversed());
        if (finishedExecutions.size() > executionsLimit) {
            finishedExecutions = finishedExecutions.subList(0, executionsLimit);
        }

        List<Execution> allExecutions = new ArrayList<>(finishedExecutions.size() + running.size() + pending.size() + queued.size());
        allExecutions.addAll(running);
        allExecutions.addAll(queued);
        allExecutions.addAll(pending);
        allExecutions.addAll(finishedExecutions);

        JobTopStats stats = new JobTopStats();
        for (Study study : studies) {
            OpenCGAResult result = executionDBAdaptor.groupBy(new Query(baseQuery)
                            .append(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()),
                    Collections.singletonList(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key()),
                    new QueryOptions(QueryOptions.COUNT, true),
                    userId);
            for (Object o : result.getResults()) {
                String status = ((Map) ((Map) o).get("_id")).get(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key()).toString();
                int count = ((Number) ((Map) o).get("count")).intValue();
                switch (status) {
                    case Enums.ExecutionStatus.RUNNING:
                        stats.setRunning(stats.getRunning() + count);
                        break;
                    case Enums.ExecutionStatus.QUEUED:
                        stats.setQueued(stats.getQueued() + count);
                        break;
                    case Enums.ExecutionStatus.PENDING:
                        stats.setPending(stats.getPending() + count);
                        break;
                    case Enums.ExecutionStatus.DONE:
                        stats.setDone(stats.getDone() + count);
                        break;
                    case Enums.ExecutionStatus.ERROR:
                        stats.setError(stats.getError() + count);
                        break;
                    case Enums.ExecutionStatus.ABORTED:
                        stats.setAborted(stats.getAborted() + count);
                        break;
                    default:
                        break;
                }
            }
        }
        ExecutionTop top = new ExecutionTop(Date.from(Instant.now()), stats, allExecutions);
        return new OpenCGAResult<>(((int) stopWatch.getTime()), null, 1, Collections.singletonList(top), 1);
    }


    @Override
    public DBIterator<Execution> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query);
        query.put(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return executionDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    public DBIterator<Execution> iterator(Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        authorizationManager.isInstallationAdministrator(userId);

        fixQueryObject(query);

        return executionDBAdaptor.iterator(query, options);
    }

    @Override
    public OpenCGAResult<Execution> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            fixQueryObject(query);
            query.put(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Execution> queryResult = executionDBAdaptor.get(study.getUid(), query, options, userId);
            auditManager.auditSearch(userId, Enums.Resource.EXECUTION, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.EXECUTION, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        try {
            ExecutionDBAdaptor.QueryParams param = ExecutionDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(query);

            query.append(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = executionDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.EXECUTION, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.EXECUTION, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Execution> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(query);
            query.put(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Long> queryResult = executionDBAdaptor.count(query, userId);
            auditManager.auditCount(userId, Enums.Resource.EXECUTION, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResult.getTime(), queryResult.getEvents(), 0, Collections.emptyList(),
                    queryResult.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.EXECUTION, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> ids, QueryOptions options, String token) throws CatalogException {
        throw new NotImplementedException("Delete operation not implemented");
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        throw new NotImplementedException("Delete operation not implemented");
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        throw new NotImplementedException("Rank operation not implemented");
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        throw new NotImplementedException("GroupBy operation not implemented");
    }

    @Override
    protected void fixQueryObject(Query query) {
        changeQueryId(query, ParamConstants.EXECUTION_TOOL_ID_PARAM, ExecutionDBAdaptor.QueryParams.INTERNAL_TOOL_ID.key());
        super.fixQueryObject(query);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> executionList, String member,
                                                            boolean ignoreException, String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("executionList", executionList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        try {
            OpenCGAResult<Map<String, List<String>>> executionAclList = OpenCGAResult.empty();
            InternalGetDataResult<Execution> queryResult = internalGet(study.getUid(), executionList, INCLUDE_EXECUTION_IDS, user,
                    ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String executionId : executionList) {
                if (!missingMap.containsKey(executionId)) {
                    Execution execution = queryResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allExecutionAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allExecutionAcls = authorizationManager.getExecutionAcl(study.getUid(), execution.getUuid(), user, member);
                        } else {
                            allExecutionAcls = authorizationManager.getAllExecutionAcls(study.getUid(), execution.getUuid(), user);
                        }
                        executionAclList.append(allExecutionAcls);
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.EXECUTION, execution.getId(),
                                execution.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.EXECUTION, execution.getId(),
                                execution.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, executionId, missingMap.get(executionId).getErrorMsg());
                            executionAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, executionId, missingMap.get(executionId).getErrorMsg());
                    executionAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.EXECUTION, executionId, "", study.getId(),
                            study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(executionId).getErrorMsg())), new ObjectMap());
                }
            }
            return executionAclList;
        } catch (CatalogException e) {
            for (String jobId : executionList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.EXECUTION, jobId, "", study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> executionStrList, String memberList,
                                                              AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("executionStrList", executionStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            if (executionStrList == null || executionStrList.isEmpty()) {
                throw new CatalogException("Missing execution parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, ExecutionAclEntry.ExecutionPermissions::valueOf);
            }

            List<Execution> executionList = internalGet(study.getUid(), executionStrList, INCLUDE_EXECUTION_AND_JOB_IDS, userId, false)
                    .getResults();

            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            // Propagate permissions to corresponding jobs
            AuthorizationManager.CatalogAclParams executionAclParams = new AuthorizationManager.CatalogAclParams(new LinkedList<>(),
                    permissions, Enums.Resource.EXECUTION);
            AuthorizationManager.CatalogAclParams jobAclParams = new AuthorizationManager.CatalogAclParams(new LinkedList<>(),
                    permissions, Enums.Resource.JOB);
            for (Execution execution : executionList) {
                executionAclParams.getIds().add(execution.getUid());
                if (CollectionUtils.isNotEmpty(execution.getJobs())) {
                    jobAclParams.getIds().addAll(execution.getJobs().stream().map(Job::getUid).collect(Collectors.toList()));
                }
            }
            List<AuthorizationManager.CatalogAclParams> aclParamsList = Arrays.asList(executionAclParams, jobAclParams);

            OpenCGAResult<Map<String, List<String>>> queryResultList;
            switch (action) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), members, aclParamsList);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), members, aclParamsList);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                case RESET:
                    for (AuthorizationManager.CatalogAclParams catalogAclParams : aclParamsList) {
                        catalogAclParams.setPermissions(null);
                    }
                    queryResultList = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Execution execution : executionList) {
                auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.EXECUTION, execution.getId(),
                        execution.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (executionStrList != null) {
                for (String executionId : executionStrList) {
                    auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.EXECUTION, executionId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

}
