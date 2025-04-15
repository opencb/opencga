package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor;
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
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.externalTool.*;
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
import static org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor.QueryParams.*;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ExternalToolManager extends ResourceManager<ExternalTool> {

    public static final QueryOptions INCLUDE_WORKFLOW_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(ID.key(), UID.key(),
            UUID.key(), VERSION.key(), DESCRIPTION.key(), STUDY_UID.key(), MANAGER.key()));

    private final CatalogIOManager catalogIOManager;
    private final IOManagerFactory ioManagerFactory;

    private static final Pattern MEMORY_PATTERN = Pattern.compile("\\s*memory\\s*=\\s*\\{\\s*[^0-9]*([0-9]+\\.[A-Za-z]+)");
    private static final Pattern CPU_PATTERN = Pattern.compile("\\s*cpus\\s*=\\s*\\{\\s*[^0-9]*([0-9]+)");

    public static final int MAX_CPUS = 15;
    public static final String MAX_MEMORY = "64.GB";  // Format is important for Nextflow. It requires the dot symbol.

    private final Logger logger;

    ExternalToolManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, IOManagerFactory ioManagerFactory, CatalogIOManager catalogIOManager,
                        Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.catalogIOManager = catalogIOManager;
        this.ioManagerFactory = ioManagerFactory;
        this.logger = LoggerFactory.getLogger(ExternalToolManager.class);
    }


    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.WORKFLOW;
    }

    @Override
    InternalGetDataResult<ExternalTool> internalGet(String organizationId, long studyUid, List<String> workflowIdList,
                                                    @Nullable Query query, QueryOptions options, String user, boolean ignoreException)
            throws CatalogException {
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

        ExternalToolDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<ExternalTool> workflowResult = getWorkflowDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<ExternalTool, String> workflowStringFunction = ExternalTool::getId;
        if (idQueryParam.equals(UUID)) {
            workflowStringFunction = ExternalTool::getUuid;
        }

        if (ignoreException || workflowResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, workflowStringFunction, workflowResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<ExternalTool> resultsNoCheck = getWorkflowDBAdaptor(organizationId).get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == workflowResult.getNumResults()) {
            throw CatalogException.notFound("workflows", getMissingFields(uniqueList, workflowResult.getResults(), workflowStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the workflows.");
        }
    }

    @Override
    public OpenCGAResult<ExternalTool> create(String studyStr, ExternalTool externalTool, QueryOptions options, String token)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("workflow", externalTool)
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
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, externalTool.getId(), externalTool.getUuid(), studyId,
                    studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, externalTool.getId(), "", studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
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
        ExternalTool externalTool = internalGet(organizationId, study.getUid(), runParams.getId(), query, QueryOptions.empty(), userId)
                .first();

        ToolInfo toolInfo = new ToolInfo()
                .setId(externalTool.getId())
                // Set workflow minimum requirements
                .setMinimumRequirements(externalTool.getMinimumRequirements() != null
                        ? externalTool.getMinimumRequirements()
                        : configuration.getAnalysis().getWorkflow().getMinRequirements())
                .setDescription(externalTool.getDescription());

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
                    StudyPermissions.Permissions.WRITE_WORKFLOWS);

            // 2. Download workflow
            ExternalTool externalTool = downloadWorkflow(repository);
            validateNewWorkflow(externalTool, userId);
            workflowId = externalTool.getId();

            OpenCGAResult<ExternalTool> result;

            Query query = new Query()
                    .append(STUDY_UID.key(), study.getUid())
                    .append(ID.key(), externalTool.getId());
            OpenCGAResult<ExternalTool> tmpResult = getWorkflowDBAdaptor(organizationId).get(query, INCLUDE_WORKFLOW_IDS);
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
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, externalTool.getId(), externalTool.getUuid(), studyId,
                    studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.WORKFLOW, workflowId, "", studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private ExternalTool downloadWorkflow(WorkflowRepositoryParams repository) throws CatalogException {
        ParamUtils.checkObj(repository, "Workflow repository parameters");
        if (StringUtils.isEmpty(repository.getId())) {
            throw new CatalogParameterException("Missing 'id' field in workflow import parameters");
        }
        String workflowId = repository.getId().replace("/", ".");
        ExternalTool externalTool = new ExternalTool("", "", "", null, new WorkflowSystem(WorkflowSystem.SystemId.NEXTFLOW, ""),
                new LinkedList<>(), new LinkedList<>(), new MinimumRequirements(), false, repository.toWorkflowRepository(),
                new LinkedList<>(), new ExternalToolInternal(), TimeUtils.getTime(), TimeUtils.getTime(), new HashMap<>());

        try {
            processNextflowConfig(externalTool, repository);
            processMemoryRequirements(externalTool, repository);
        } catch (CatalogException e) {
            throw new CatalogException("Could not process repository information from workflow '" + workflowId + "'.", e);
        }

        return externalTool;
    }

    private void processMemoryRequirements(ExternalTool externalTool, WorkflowRepositoryParams repository) throws CatalogException {
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
                externalTool.getMinimumRequirements().setCpu(String.valueOf(cpus));
                externalTool.getMinimumRequirements().setMemory(memory);
            } else {
                logger.warn("Could not find the minimum requirements for the workflow " + externalTool.getId());
            }
            in.close();
        } catch (Exception e) {
            throw new CatalogException("Could not process nextflow.config file.", e);
        }
    }

    private void processNextflowConfig(ExternalTool externalTool, WorkflowRepositoryParams repository) throws CatalogException {
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
                        fillWithWorkflowManifest(externalTool, inputLine);
                    }
                } else if (profilesBracketClose != null) {
                    if (gitpodBracketClose != null) {
                        if (gitpodBracketClose.equals(inputLine)) {
                            gitpodBracketClose = null;
                        } else {
                            // Process gitpod line
                            fillWithGitpodManifest(externalTool, inputLine);
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

    private void fillWithWorkflowManifest(ExternalTool externalTool, String rawline) {
        String[] split = rawline.split("= ");
        if (split.length != 2) {
            return;
        }
        String key = split[0].trim();
//        String key = split[0].replaceAll(" ", "");
        String value = split[1].replace("\"", "").replace("'", "").trim();
        switch (key) {
            case "name":
                externalTool.setId(value.replace("/", "."));
                externalTool.setName(value.replace("/", " "));
                externalTool.getRepository().setId(value);
                break;
            case "author":
                externalTool.getRepository().setAuthor(value);
                break;
            case "description":
                externalTool.setDescription(value);
                externalTool.getRepository().setDescription(value);
                break;
            case "version":
                String version = value.replaceAll("^[^0-9]+|[^0-9.]+$", "");
                externalTool.getRepository().setVersion(version);
                break;
            case "nextflowVersion":
                // Nextflow version must start with a number
                version = value.replaceAll("^[^0-9]+|[^0-9.]+$", "");
                externalTool.getManager().setVersion(version);
                break;
            default:
                break;
        }
    }

    private void fillWithGitpodManifest(ExternalTool externalTool, String rawline) {
        String[] split = rawline.split("=");
        if (split.length != 2) {
            return;
        }
        String key = split[0].replaceAll(" ", "");
        String value = split[1].replace("\"", "").replace("'", "").trim();
        switch (key) {
            case "executor.cpus":
                externalTool.getMinimumRequirements().setCpu(value);
                break;
            case "executor.memory":
                externalTool.getMinimumRequirements().setMemory(value);
                break;
            default:
                break;
        }
    }


    public OpenCGAResult<ExternalTool> update(String studyStr, String workflowId, ExternalToolUpdateParams updateParams,
                                              QueryOptions options, String token) throws CatalogException {
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

            ExternalTool externalTool = internalGet(organizationId, study.getUid(), Collections.singletonList(workflowId), null,
                    INCLUDE_WORKFLOW_IDS, userId, false).first();
            id = externalTool.getId();
            uuid = externalTool.getUuid();

            // Check permission
            authorizationManager.checkWorkflowPermission(organizationId, study.getUid(), externalTool.getUid(), userId,
                    ExternalToolPermissions.WRITE);

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
            OpenCGAResult<ExternalTool> insert = getWorkflowDBAdaptor(organizationId).update(externalTool.getUid(), updateMap, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated workflow
                Query query = new Query()
                        .append(UID.key(), externalTool.getUid());
                OpenCGAResult<ExternalTool> result = getWorkflowDBAdaptor(organizationId).get(query, options);
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

            query.append(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
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
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.WORKFLOW, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<ExternalTool> result = OpenCGAResult.empty(ExternalTool.class);
        for (String id : ids) {
            String workflowId = id;
            String workflowUuid = "";
            try {
                OpenCGAResult<ExternalTool> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_WORKFLOW_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Workflow '" + id + "' not found");
                }
                ExternalTool externalTool = internalResult.first();

                // We set the proper values for the audit
                workflowId = externalTool.getId();
                workflowUuid = externalTool.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkWorkflowPermission(organizationId, study.getUid(), externalTool.getUid(), userId,
                            ExternalToolPermissions.DELETE);
                }

                // Check if the workflow can be deleted
                checkWorkflowCanBeDeleted(organizationId, study.getUid(), externalTool, params.getBoolean(Constants.FORCE, false));

                result.append(getWorkflowDBAdaptor(organizationId).delete(externalTool));

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.WORKFLOW, externalTool.getId(),
                        externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
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
            ExternalTool externalTool = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkWorkflowPermission(organizationId, study.getUid(), externalTool.getUid(), userId,
                            ExternalToolPermissions.DELETE);
                }

                // Check if the workflow can be deleted
                checkWorkflowCanBeDeleted(organizationId, study.getUid(), externalTool, params.getBoolean(Constants.FORCE, false));

                result.append(getWorkflowDBAdaptor(organizationId).delete(externalTool));

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, externalTool.getId(),
                        externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete workflow " + externalTool.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, externalTool.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.WORKFLOW, externalTool.getId(),
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
        ParamUtils.checkIdentifier(externalTool.getId(), ID.key());
        if (externalTool.getManager() == null) {
            externalTool.setManager(new WorkflowSystem());
        }
        if (externalTool.getManager().getId() == null) {
            externalTool.getManager().setId(WorkflowSystem.SystemId.NEXTFLOW);
        }
        externalTool.setScope(ParamUtils.defaultObject(externalTool.getScope(), ExternalTool.Scope.OTHER));
        externalTool.setTags(externalTool.getTags() != null ? externalTool.getTags() : Collections.emptyList());
        externalTool.setScripts(externalTool.getScripts() != null ? externalTool.getScripts() : Collections.emptyList());
        boolean main = false;
        for (WorkflowScript script : externalTool.getScripts()) {
            ParamUtils.checkIdentifier(script.getFileName(), SCRIPTS.key() + ".id");
            ParamUtils.checkParameter(script.getContent(), SCRIPTS.key() + ".content");
            if (script.isMain()) {
                if (main) {
                    throw new CatalogParameterException("More than one main script found.");
                }
                main = script.isMain();
            }
        }
        if (CollectionUtils.isNotEmpty(externalTool.getScripts()) && !main) {
            throw new CatalogParameterException("No main script found.");
        }
        externalTool.setRepository(externalTool.getRepository() != null ? externalTool.getRepository() : new WorkflowRepository(""));
        if (StringUtils.isEmpty(externalTool.getRepository().getId()) && CollectionUtils.isEmpty(externalTool.getScripts())) {
            throw new CatalogParameterException("No repository image or scripts found.");
        }
        if (StringUtils.isNotEmpty(externalTool.getRepository().getId()) && CollectionUtils.isNotEmpty(externalTool.getScripts())) {
            throw new CatalogParameterException("Both repository image and scripts found. Please, either add scripts or a repository"
                    + " image.");
        }

        externalTool.setName(ParamUtils.defaultString(externalTool.getName(), externalTool.getId()));
        externalTool.setDescription(ParamUtils.defaultString(externalTool.getDescription(), ""));
        externalTool.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.WORKFLOW));
        externalTool.setVersion(1);
        externalTool.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(externalTool.getCreationDate(), CREATION_DATE.key()));
        externalTool.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(externalTool.getModificationDate(), MODIFICATION_DATE.key()));
        externalTool.setAttributes(ParamUtils.defaultObject(externalTool.getAttributes(), Collections.emptyMap()));
        externalTool.setInternal(new ExternalToolInternal(new InternalStatus(InternalStatus.READY), TimeUtils.getTime(),
                TimeUtils.getTime(), userId));
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
                    INCLUDE_WORKFLOW_IDS, userId, ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> workflowUids = queryResult.getResults().stream().map(ExternalTool::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                workflowAcls = authorizationManager.getAcl(organizationId, study.getUid(), workflowUids, members, Enums.Resource.WORKFLOW,
                        ExternalToolPermissions.class, userId);
            } else {
                workflowAcls = authorizationManager.getAcl(organizationId, study.getUid(), workflowUids, Enums.Resource.WORKFLOW,
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
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.WORKFLOW,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
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

            externalToolList = internalGet(organizationId, study.getUid(), params.getExternalToolIds(), INCLUDE_WORKFLOW_IDS, userId, false)
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
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW, workflowId,
                            "", study.getId(), study.getUuid(), auditParams,
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

                OpenCGAResult<AclEntryList<ExternalToolPermissions>> queryResults = authorizationManager.getAcls(organizationId,
                        study.getUid(),
                        workflowUids, members, Enums.Resource.WORKFLOW, ExternalToolPermissions.class);

                for (int i = 0; i < queryResults.getResults().size(); i++) {
                    queryResults.getResults().get(i).setId(workflowIds.get(i));
                }
                aclResultList.append(queryResults);

                for (ExternalTool externalTool : batchExternalToolList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                }
            } catch (CatalogException e) {
                // Process current batch
                for (ExternalTool externalTool : batchExternalToolList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW,
                            externalTool.getId(), externalTool.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                // Process remaining unprocessed batches
                while (numProcessed < externalToolList.size()) {
                    ExternalTool externalTool = externalToolList.get(numProcessed);
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.WORKFLOW,
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
