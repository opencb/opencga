package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
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
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.workflow.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor.QueryParams.*;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class WorkflowManager extends ResourceManager<Workflow, WorkflowPermissions> {

    public static final QueryOptions INCLUDE_WORKFLOW_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(ID.key(), UID.key(),
            UUID.key(), VERSION.key(), DESCRIPTION.key(), STUDY_UID.key(), MANAGER.key()));

    private final CatalogIOManager catalogIOManager;
    private final IOManagerFactory ioManagerFactory;

    private static final Pattern MEMORY_PATTERN = Pattern.compile("\\s*memory\\s*=\\s*\\{\\s*[^0-9]*([0-9]+\\.[A-Za-z]+)");
    private static final Pattern CPU_PATTERN = Pattern.compile("\\s*cpus\\s*=\\s*\\{\\s*[^0-9]*([0-9]+)");

    public static final int MAX_CPUS = 15;
    public static final String MAX_MEMORY = "64.GB";  // Format is important for Nextflow. It requires the dot symbol.

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
    Enums.Resource getResource() {
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
        return create(studyStr, workflow, options, token, QueryOptions.empty(), (organizationId, study, userId, entryParam) -> {
            entryParam.setId(workflow.getId());

            // 1. Check permissions
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_WORKFLOWS);

            // 2. Validate the workflow parameters
            validateNewWorkflow(workflow, userId);
            entryParam.setId(workflow.getId());
            entryParam.setUuid(workflow.getUuid());

            // 3. We insert the workflow
            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            OpenCGAResult<Workflow> insert = getWorkflowDBAdaptor(organizationId).insert(study.getUid(), workflow, queryOptions);
            if (queryOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                Query query = new Query()
                        .append(STUDY_UID.key(), study.getUid())
                        .append(UID.key(), workflow.getUid());
                OpenCGAResult<Workflow> result = getWorkflowDBAdaptor(organizationId).get(query, queryOptions);
                insert.setResults(result.getResults());
            }
            return insert;
        });
    }

    public OpenCGAResult<Job> submit(String studyStr, NextFlowRunParams runParams, String jobId, String jobDescription,
                                     String jobDependsOnStr, String jobTagsStr, String jobScheduledStartTime, String jobPriority,
                                     Boolean dryRun, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);
        Query query = new Query();
        if (runParams.getVersion() != null) {
            query.append(VERSION.key(), runParams.getVersion());
        }
        Workflow workflow = internalGet(organizationId, study.getUid(), runParams.getId(), query, QueryOptions.empty(), userId).first();

        ToolInfo toolInfo = new ToolInfo()
                .setId(workflow.getId())
                // Set workflow minimum requirements
                .setMinimumRequirements(workflow.getMinimumRequirements() != null
                        ? workflow.getMinimumRequirements()
                        : configuration.getAnalysis().getWorkflow().getMinRequirements())
                .setDescription(workflow.getDescription());

        Map<String, Object> paramsMap = runParams.toParams();
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

        return catalogManager.getJobManager().submit(study.getFqn(), JobType.WORKFLOW, toolInfo, priority, paramsMap, jobId, jobDescription,
                jobDependsOn, jobTags, null, jobScheduledStartTime, dryRun, Collections.emptyMap(), token);
    }

    public OpenCGAResult<Workflow> importWorkflow(String studyStr, WorkflowRepositoryParams repository, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("repository", repository)
                .append("options", options)
                .append("token", token);

        return runForSingleEntry(params, Enums.Action.IMPORT, studyStr, token, (organizationId, study, userId, entryParam) -> {
            // 1. Check permissions
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_WORKFLOWS);

            // 2. Download workflow
            Workflow workflow = downloadWorkflow(repository);
            validateNewWorkflow(workflow, userId);
            entryParam.setId(workflow.getId());
            entryParam.setUuid(workflow.getUuid());

            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            OpenCGAResult<Workflow> result;

            Query query = new Query()
                    .append(STUDY_UID.key(), study.getUid())
                    .append(ID.key(), workflow.getId());
            OpenCGAResult<Workflow> tmpResult = getWorkflowDBAdaptor(organizationId).get(query, INCLUDE_WORKFLOW_IDS);
            if (tmpResult.getNumResults() > 0) {
                logger.warn("Workflow '" + workflow.getId() + "' already exists. Updating with the latest workflow information.");
                try {
                    // Set workflow uid just in case users want to get the final result
                    workflow.setUid(tmpResult.first().getUid());

                    // Create update map removing the id to avoid the dbAdaptor exception
                    ObjectMap updateMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(workflow));
                    updateMap.remove("id");
                    result = getWorkflowDBAdaptor(organizationId).update(tmpResult.first().getUid(), updateMap, QueryOptions.empty());
                } catch (JsonProcessingException e) {
                    throw new CatalogException("Internal error. Workflow '" + workflow.getId() + "' already existed but it could not"
                            + " be updated", e);
                }
            } else {
                // 3. We insert the workflow
                result = getWorkflowDBAdaptor(organizationId).insert(study.getUid(), workflow, queryOptions);
            }

            if (queryOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created workflow
                query = new Query()
                        .append(STUDY_UID.key(), study.getUid())
                        .append(UID.key(), workflow.getUid());
                OpenCGAResult<Workflow> tmpTmpResult = getWorkflowDBAdaptor(organizationId).get(query, queryOptions);
                result.setResults(tmpTmpResult.getResults());
            }
            return result;
        });
    }

    public OpenCGAResult<Workflow> update(String studyStr, String workflowId, WorkflowUpdateParams updateParams, QueryOptions options,
                                          String token) throws CatalogException {
        return update(studyStr, workflowId, updateParams, options, token, StudyManager.INCLUDE_STUDY_IDS,
                (organizationId, study, userId, entryParam) -> {
                    Workflow workflow = internalGet(organizationId, study.getUid(), Collections.singletonList(workflowId), null,
                            INCLUDE_WORKFLOW_IDS, userId, false).first();
                    entryParam.setId(workflow.getId());
                    entryParam.setUuid(workflow.getUuid());

                    // 1. Check permissions
                    authorizationManager.checkWorkflowPermission(organizationId, study.getUid(), workflow.getUid(), userId,
                            WorkflowPermissions.WRITE);

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
                    QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
                    OpenCGAResult<Workflow> insert = getWorkflowDBAdaptor(organizationId).update(workflow.getUid(), updateMap,
                            queryOptions);
                    if (queryOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                        // Fetch updated workflow
                        Query query = new Query()
                                .append(UID.key(), workflow.getUid());
                        OpenCGAResult<Workflow> result = getWorkflowDBAdaptor(organizationId).get(query, queryOptions);
                        insert.setResults(result.getResults());
                    }
                    return insert;
                });
    }

    @Override
    public DBIterator<Workflow> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return iterator(studyStr, query, options, StudyManager.INCLUDE_STUDY_IDS, token, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getWorkflowDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, finalOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        return facet(studyStr, query, facet, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getWorkflowDBAdaptor(organizationId).facet(study.getUid(), finalQuery, facet, userId);
        });
    }

    @Override
    public OpenCGAResult<Workflow> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return search(studyStr, query, options, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getWorkflowDBAdaptor(organizationId).get(study.getUid(), finalQuery, finalOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<?> distinct(String studyStr, List<String> fields, Query query, String token) throws CatalogException {
        return distinct(studyStr, fields, query, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getWorkflowDBAdaptor(organizationId).distinct(study.getUid(), fields, finalQuery, userId);
        });
    }

    @Override
    public OpenCGAResult<Workflow> count(String studyStr, Query query, String token) throws CatalogException {
        return count(studyStr, query, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> count = getWorkflowDBAdaptor(organizationId).count(finalQuery, userId);
            return new OpenCGAResult<>(count.getTime(), count.getEvents(), 0, Collections.emptyList(), count.getNumMatches());
        });
    }

    @Override
    public OpenCGAResult<Workflow> delete(String studyStr, List<String> ids, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, ids, options, false, token);
    }

    public OpenCGAResult<Workflow> delete(String studyStr, List<String> ids, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, ids, params, ignoreException, token, (organizationId, study, userId, entryParam) -> {
            if (StringUtils.isEmpty(entryParam.getId())) {
                throw new CatalogException("Internal error: Missing workflow id. This workflow id should have been provided internally.");
            }
            String workflowId = entryParam.getId();

            Query query = new Query();
            authorizationManager.buildAclCheckQuery(userId, WorkflowPermissions.DELETE.name(), query);
            OpenCGAResult<Workflow> internalResult = internalGet(organizationId, study.getUid(), workflowId, query, INCLUDE_WORKFLOW_IDS,
                    userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Workflow '" + workflowId + "' not found or user " + userId + " does not have the proper "
                        + "permissions to delete it.");
            }
            Workflow workflow = internalResult.first();

            // We set the proper values for the entry param object
            entryParam.setId(workflow.getId());
            entryParam.setUuid(workflow.getUuid());

            // Check if the workflow can be deleted
            checkWorkflowCanBeDeleted(organizationId, study.getUid(), workflow, params.getBoolean(Constants.FORCE, false));

            return getWorkflowDBAdaptor(organizationId).delete(workflow);
        });
    }

    private void checkWorkflowCanBeDeleted(String organizationId, long uid, Workflow workflow, boolean force) {
        return;
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, query, options, ignoreException, token, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(WorkflowDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            authorizationManager.buildAclCheckQuery(userId, WorkflowPermissions.DELETE.name(), finalQuery);
            return getWorkflowDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_WORKFLOW_IDS, userId);
        }, (organizationId, study, userId, workflow) -> {
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            // Check if the workflow can be deleted
            checkWorkflowCanBeDeleted(organizationId, study.getUid(), workflow, finalOptions.getBoolean(Constants.FORCE, false));
            return getWorkflowDBAdaptor(organizationId).delete(workflow);
        });
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
        if (workflow.getManager() == null) {
            workflow.setManager(new WorkflowSystem());
        }
        if (workflow.getManager().getId() == null) {
            workflow.getManager().setId(WorkflowSystem.SystemId.NEXTFLOW);
        }
        workflow.setType(ParamUtils.defaultObject(workflow.getType(), Workflow.Type.OTHER));
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
        if (StringUtils.isEmpty(workflow.getRepository().getId()) && CollectionUtils.isEmpty(workflow.getScripts())) {
            throw new CatalogParameterException("No repository image or scripts found.");
        }
        if (StringUtils.isNotEmpty(workflow.getRepository().getId()) && CollectionUtils.isNotEmpty(workflow.getScripts())) {
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
        return getAcls(studyStr, workflowList, members, ignoreException, token, (organizationId, study, userId, entryParamList) -> {
            OpenCGAResult<AclEntryList<WorkflowPermissions>> workflowAcls;
            Map<String, InternalGetDataResult<?>.Missing> missingMap = new HashMap<>();

            for (String workflowId : workflowList) {
                entryParamList.add(new EntryParam(workflowId, null));
            }
            InternalGetDataResult<Workflow> queryResult = internalGet(organizationId, study.getUid(), workflowList, INCLUDE_WORKFLOW_IDS,
                    userId, ignoreException);
            entryParamList.clear();
            for (Workflow workflow : queryResult.getResults()) {
                entryParamList.add(new EntryParam(workflow.getId(), workflow.getUuid()));
            }
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
                    resultList.add(workflowAcls.getResults().get(counter));
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, workflowId, missingMap.get(workflowId).getErrorMsg()));
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                workflowAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            workflowAcls.setResults(resultList);
            workflowAcls.setEvents(eventList);

            return workflowAcls;
        });
    }

    public OpenCGAResult<AclEntryList<WorkflowPermissions>> updateAcl(String studyStr, String memberList, WorkflowAclUpdateParams params,
                                                                      ParamUtils.AclAction action, String token) throws CatalogException {
        return updateAcls(studyStr, null, memberList, params, action, token, (organizationId, study, userId, entryParamList) -> {
            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

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

            List<Workflow> workflowList = internalGet(organizationId, study.getUid(), params.getWorkflowIds(), INCLUDE_WORKFLOW_IDS,
                    userId, false).getResults();
            for (Workflow workflow : workflowList) {
                entryParamList.add(new EntryParam(workflow.getId(), workflow.getUuid()));
            }

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            checkMembers(organizationId, study.getUid(), members);
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);

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
                AuthorizationManager.CatalogAclParams.addToList(workflowUids, params.getPermissions(), Enums.Resource.WORKFLOW,
                        aclParamsList);

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
            } while (numProcessed < workflowList.size());

            return aclResultList;
        });
    }

    private Workflow downloadWorkflow(WorkflowRepositoryParams repository) throws CatalogException {
        ParamUtils.checkObj(repository, "Workflow repository parameters");
        if (StringUtils.isEmpty(repository.getId())) {
            throw new CatalogParameterException("Missing 'id' field in workflow import parameters");
        }
        String workflowId = repository.getId().replace("/", ".");
        Workflow workflow = new Workflow("", "", "", null, new WorkflowSystem(WorkflowSystem.SystemId.NEXTFLOW, ""), new LinkedList<>(),
                new LinkedList<>(), new MinimumRequirements(), false, repository.toWorkflowRepository(), new LinkedList<>(),
                new WorkflowInternal(), TimeUtils.getTime(), TimeUtils.getTime(), new HashMap<>());

        try {
            processNextflowConfig(workflow, repository);
            processMemoryRequirements(workflow, repository);
        } catch (CatalogException e) {
            throw new CatalogException("Could not process repository information from workflow '" + workflowId + "'.", e);
        }

        return workflow;
    }

    private void processMemoryRequirements(Workflow workflow, WorkflowRepositoryParams repository) throws CatalogException {
        String urlStr;

        if (StringUtils.isEmpty(repository.getVersion())) {
            urlStr = "https://raw.githubusercontent.com/" + repository.getId() + "/refs/heads/master/conf/base.config";
        } else {
            urlStr = "https://raw.githubusercontent.com/" + repository.getId() + "/refs/tags/" + repository.getVersion()
                    + "/conf/base.config";
        }

        try {
            URL url = new URL(urlStr);
            BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

            int cpus = 0;
            String memory = null;
            String inputLine;
            long maxMemory = IOUtils.fromHumanReadableToByte(MAX_MEMORY);
            while ((inputLine = in.readLine()) != null) {
                Matcher cpuMatcher = CPU_PATTERN.matcher(inputLine);
                Matcher memoryMatcher = MEMORY_PATTERN.matcher(inputLine);
                if (cpuMatcher.find()) {
                    String value = cpuMatcher.group(1);
                    int intValue = Integer.parseInt(value);
                    if (intValue > cpus) {
                        cpus = Math.min(intValue, MAX_CPUS);
                    }
                } else if (memoryMatcher.find()) {
                    String value = memoryMatcher.group(1);
                    if (memory == null) {
                        memory = value;
                    } else {
                        long memoryBytes = IOUtils.fromHumanReadableToByte(value);
                        long currentMemoryBytes = IOUtils.fromHumanReadableToByte(memory);
                        if (memoryBytes > currentMemoryBytes) {
                            if (memoryBytes > maxMemory) {
                                memory = MAX_MEMORY;
                            } else {
                                memory = value;
                            }
                        }
                    }
                }
            }
            if (cpus > 0 && memory != null) {
                workflow.getMinimumRequirements().setCpu(String.valueOf(cpus));
                workflow.getMinimumRequirements().setMemory(memory);
            } else {
                logger.warn("Could not find the minimum requirements for the workflow " + workflow.getId());
            }
            in.close();
        } catch (Exception e) {
            throw new CatalogException("Could not process nextflow.config file.", e);
        }
    }

    private void processNextflowConfig(Workflow workflow, WorkflowRepositoryParams repository) throws CatalogException {
        String urlStr;
        if (StringUtils.isEmpty(repository.getVersion())) {
            urlStr = "https://raw.githubusercontent.com/" + repository.getId() + "/refs/heads/master/nextflow.config";
        } else {
            urlStr = "https://raw.githubusercontent.com/" + repository.getId() + "/refs/tags/" + repository.getVersion()
                    + "/nextflow.config";
        }

        try {
            URL url = new URL(urlStr);
            BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

            // We add the bracket close strings because they are expected to be properly indented. That way, we will be able to know when
            // that section has been properly closed. Otherwise, we may get confused by some other subsections that could be closed before
            // the actual section closure.
            String manifestBracketClose = null;
            String profilesBracketClose = null;
            String gitpodBracketClose = null;
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (manifestBracketClose != null) {
                    if (manifestBracketClose.equals(inputLine)) {
                        manifestBracketClose = null;
                    } else {
                        // Process manifest line
                        fillWithWorkflowManifest(workflow, inputLine);
                    }
                } else if (profilesBracketClose != null) {
                    if (gitpodBracketClose != null) {
                        if (gitpodBracketClose.equals(inputLine)) {
                            gitpodBracketClose = null;
                        } else {
                            // Process gitpod line
                            fillWithGitpodManifest(workflow, inputLine);
                        }
                    } else if (inputLine.trim().startsWith("gitpod {")) {
                        int position = inputLine.indexOf("gitpod {");
                        gitpodBracketClose = StringUtils.repeat(" ", position) + "}";
                    } else if (profilesBracketClose.equals(inputLine)) {
                        profilesBracketClose = null;
                    }
                } else if (inputLine.trim().startsWith("manifest {")) {
                    int position = inputLine.indexOf("profiles {");
                    manifestBracketClose = StringUtils.repeat(" ", position) + "}";
                } else if (inputLine.trim().startsWith("profiles {")) {
                    int position = inputLine.indexOf("profiles {");
                    profilesBracketClose = StringUtils.repeat(" ", position) + "}";

                }
            }
            in.close();
        } catch (Exception e) {
            throw new CatalogException("Could not process nextflow.config file.", e);
        }
    }

    private void fillWithWorkflowManifest(Workflow workflow, String rawline) {
        String[] split = rawline.split("= ");
        if (split.length != 2) {
            return;
        }
        String key = split[0].trim();
//        String key = split[0].replaceAll(" ", "");
        String value = split[1].replace("\"", "").replace("'", "").trim();
        switch (key) {
            case "name":
                workflow.setId(value.replace("/", "."));
                workflow.setName(value.replace("/", " "));
                workflow.getRepository().setId(value);
                break;
            case "author":
                workflow.getRepository().setAuthor(value);
                break;
            case "description":
                workflow.setDescription(value);
                workflow.getRepository().setDescription(value);
                break;
            case "version":
                String version = value.replaceAll("^[^0-9]+|[^0-9.]+$", "");
                workflow.getRepository().setVersion(version);
                break;
            case "nextflowVersion":
                // Nextflow version must start with a number
                version = value.replaceAll("^[^0-9]+|[^0-9.]+$", "");
                workflow.getManager().setVersion(version);
                break;
            default:
                break;
        }
    }

    private void fillWithGitpodManifest(Workflow workflow, String rawline) {
        String[] split = rawline.split("=");
        if (split.length != 2) {
            return;
        }
        String key = split[0].replaceAll(" ", "");
        String value = split[1].replace("\"", "").replace("'", "").trim();
        switch (key) {
            case "executor.cpus":
                workflow.getMinimumRequirements().setCpu(value);
                break;
            case "executor.memory":
                workflow.getMinimumRequirements().setMemory(value);
                break;
            default:
                break;
        }
    }
}
