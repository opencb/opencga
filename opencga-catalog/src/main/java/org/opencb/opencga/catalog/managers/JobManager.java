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
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.models.update.JobUpdateParams;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobManager extends ResourceManager<Job> {

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    public static final QueryOptions INCLUDE_JOB_IDS = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.UUID.key(),
                    JobDBAdaptor.QueryParams.STUDY_UID.key()));

    JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
               DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.JOB;
    }

    @Override
    OpenCGAResult<Job> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(JobDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(JobDBAdaptor.QueryParams.ID.key(), entry);
        }
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                JobDBAdaptor.QueryParams.UUID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.STUDY_UID.key(),
//                JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.STATUS.key()));
        OpenCGAResult<Job> jobDataResult = jobDBAdaptor.get(studyUid, queryCopy, options, user);
        if (jobDataResult.getNumResults() == 0) {
            jobDataResult = jobDBAdaptor.get(queryCopy, options);
            if (jobDataResult.getNumResults() == 0) {
                throw new CatalogException("Job " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the job " + entry);
            }
        } else if (jobDataResult.getNumResults() > 1) {
            throw new CatalogException("More than one job found based on " + entry);
        } else {
            return jobDataResult;
        }
    }

    @Override
    InternalGetDataResult<Job> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options, String user,
                                           boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing job entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Job, String> jobStringFunction = Job::getId;
        JobDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            JobDBAdaptor.QueryParams param = JobDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
                param = JobDBAdaptor.QueryParams.UUID;
                jobStringFunction = Job::getUuid;
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

        OpenCGAResult<Job> jobDataResult = jobDBAdaptor.get(studyUid, queryCopy, options, user);
        if (ignoreException || jobDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, jobStringFunction, jobDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<Job> resultsNoCheck = jobDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == jobDataResult.getNumResults()) {
            throw CatalogException.notFound("jobs", getMissingFields(uniqueList, jobDataResult.getResults(), jobStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the jobs.");
        }
    }

    private OpenCGAResult<Job> getJob(long studyUid, String jobUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(JobDBAdaptor.QueryParams.UUID.key(), jobUuid);
        return jobDBAdaptor.get(query, options);
    }

    public Long getStudyId(long jobId) throws CatalogException {
        return jobDBAdaptor.getStudyId(jobId);
    }

    public Study getStudy(Job job, String sessionId) throws CatalogException {
        ParamUtils.checkObj(job, "job");
        ParamUtils.checkObj(sessionId, "session id");

        if (job.getStudyUid() <= 0) {
            throw new CatalogException("Missing study uid field in job");
        }

        String user = catalogManager.getUserManager().getUserId(sessionId);

        Query query = new Query(StudyDBAdaptor.QueryParams.UID.key(), job.getStudyUid());
        OpenCGAResult<Study> studyDataResult = studyDBAdaptor.get(query, QueryOptions.empty(), user);
        if (studyDataResult.getNumResults() == 1) {
            return studyDataResult.first();
        } else {
            authorizationManager.checkCanViewStudy(job.getStudyUid(), user);
            throw new CatalogException("Incorrect study uid");
        }
    }

    public OpenCGAResult<Job> visit(String studyId, String jobId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobId", jobId)
                .append("token", token);
        try {
            JobUpdateParams updateParams = new JobUpdateParams().setVisited(true);
            Job job = internalGet(study.getUid(), jobId, INCLUDE_JOB_IDS, userId).first();

            OpenCGAResult result = update(study, job, updateParams, QueryOptions.empty(), userId);
            auditManager.audit(userId, Enums.Action.VISIT, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.VISIT, Enums.Resource.JOB, jobId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Job> create(String studyStr, Job job, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("job", job)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

            ParamUtils.checkObj(job, "Job");
            ParamUtils.checkAlias(job.getId(), "job id");
            job.setName(ParamUtils.defaultString(job.getName(), ""));
            job.setDescription(ParamUtils.defaultString(job.getDescription(), ""));
            job.setCommandLine(ParamUtils.defaultString(job.getCommandLine(), ""));
            job.setCreationDate(ParamUtils.defaultString(job.getCreationDate(), TimeUtils.getTime()));
            job.setStatus(ParamUtils.defaultObject(job.getStatus(), new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE)));
            job.setPriority(ParamUtils.defaultObject(job.getPriority(), Enums.Priority.MEDIUM));
            job.setInput(ParamUtils.defaultObject(job.getInput(), Collections.emptyList()));
            job.setOutput(ParamUtils.defaultObject(job.getOutput(), Collections.emptyList()));
            job.setParams(ParamUtils.defaultObject(job.getParams(), HashMap::new));
            job.setAttributes(ParamUtils.defaultObject(job.getAttributes(), HashMap::new));
            job.setUserId(userId);
            job.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            job.setOutDir(job.getOutDir() != null && StringUtils.isNotEmpty(job.getOutDir().getPath()) ? job.getOutDir() : null);
            job.setStudyUuid(study.getUuid());

            if (!Arrays.asList(Enums.ExecutionStatus.ABORTED, Enums.ExecutionStatus.DONE, Enums.ExecutionStatus.UNREGISTERED,
                    Enums.ExecutionStatus.ERROR).contains(job.getStatus().getName())) {
                throw new CatalogException("Cannot create a job in a status different from one of the final ones.");
            }

            if (ListUtils.isNotEmpty(job.getInput())) {
                List<File> inputFiles = new ArrayList<>(job.getInput().size());
                for (File file : job.getInput()) {
                    inputFiles.add(getFile(study.getUid(), file.getPath(), userId));
                }
                job.setInput(inputFiles);
            }
            if (ListUtils.isNotEmpty(job.getOutput())) {
                List<File> outputFiles = new ArrayList<>(job.getOutput().size());
                for (File file : job.getOutput()) {
                    outputFiles.add(getFile(study.getUid(), file.getPath(), userId));
                }
                job.setOutput(outputFiles);
            }
            if (job.getOutDir() != null && StringUtils.isNotEmpty(job.getOutDir().getPath())) {
                job.setOutDir(getFile(study.getUid(), job.getOutDir().getPath(), userId));
                if (job.getOutDir().getType() != File.Type.DIRECTORY) {
                    throw new CatalogException("Unexpected outDir type. Expected " + File.Type.DIRECTORY);
                }
            }
            if (job.getStdout() != null && StringUtils.isNotEmpty(job.getStdout().getPath())) {
                job.setStdout(getFile(study.getUid(), job.getStdout().getPath(), userId));
            }
            if (job.getStderr() != null && StringUtils.isNotEmpty(job.getStderr().getPath())) {
                job.setStderr(getFile(study.getUid(), job.getStderr().getPath(), userId));
            }

            job.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.JOB));
            jobDBAdaptor.insert(study.getUid(), job, options);
            OpenCGAResult<Job> queryResult = getJob(study.getUid(), job.getUuid(), options);
            auditManager.auditCreate(userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.JOB, job.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void autoCompleteNewJob(Study study, Job job, String token) throws CatalogException {
        ParamUtils.checkObj(job, "Job");

        // Auto generate id
        if (StringUtils.isEmpty(job.getId())) {
            job.setId(job.getTool().getId() + "." + TimeUtils.getTime() + "." + org.opencb.commons.utils.StringUtils.randomString(6));
        }
        job.setPriority(ParamUtils.defaultObject(job.getPriority(), Enums.Priority.MEDIUM));
        job.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.JOB));
        job.setCreationDate(ParamUtils.defaultString(job.getCreationDate(), TimeUtils.getTime()));
        job.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));

        if (job.getStatus() == null || StringUtils.isEmpty(job.getStatus().getName())) {
            job.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.PENDING));
        }

        // Look for input files
        String fileParamSuffix = "file";
        List<File> inputFiles = new ArrayList<>();
        if (job.getParams() != null) {
            Map<String, Object> dynamicParams = null;
            for (Map.Entry<String, Object> entry : job.getParams().entrySet()) {
                // We assume that every variable ending in 'file' corresponds to input files that need to be accessible in catalog
                if (entry.getKey().toLowerCase().endsWith(fileParamSuffix)) {
                    for (String fileStr : StringUtils.split((String) entry.getValue(), ',')) {
                        try {
                            // Validate the user has access to the file
                            File file = catalogManager.getFileManager().get(study.getFqn(), fileStr,
                                    FileManager.INCLUDE_FILE_URI_PATH, token).first();
                            inputFiles.add(file);
                        } catch (CatalogException e) {
                            throw new CatalogException("Cannot find file '" + entry.getValue() + "' "
                                    + "from job param '" + entry.getKey() + "'; (study = " + study.getName() + ", token = " + token + ") :"
                                    + e.getMessage(), e);
                        }
                    }
                } else if (entry.getValue() instanceof Map) {
                    if (dynamicParams != null) {
                        List<String> dynamicParamKeys = job.getParams()
                                .entrySet()
                                .stream()
                                .filter(e -> e.getValue() instanceof Map)
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toList());
                        throw new CatalogException("Found multiple dynamic param maps in job params: " + dynamicParamKeys);
                    }
                    // If we have found a map for further dynamic params...
                    dynamicParams = (Map<String, Object>) entry.getValue();
                }
            }
            if (dynamicParams != null) {
                // We look for files in the dynamic params
                for (Map.Entry<String, Object> entry : dynamicParams.entrySet()) {
                    if (entry.getKey().toLowerCase().endsWith(fileParamSuffix)) {
                        // We assume that every variable ending in 'file' corresponds to input files that need to be accessible in catalog
                        try {
                            // Validate the user has access to the file
                            File file = catalogManager.getFileManager().get(study.getFqn(), (String) entry.getValue(),
                                    FileManager.INCLUDE_FILE_URI_PATH, token).first();
                            inputFiles.add(file);
                        } catch (CatalogException e) {
                            throw new CatalogException("Cannot find file '" + entry.getValue() + "' from variable '" + entry.getKey()
                                    + "'. ", e);
                        }
                    }
                }
            }
        }
        job.setInput(inputFiles);

        job.setAttributes(ParamUtils.defaultObject(job.getAttributes(), HashMap::new));
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String token)
            throws CatalogException {
        return submit(studyStr, toolId, priority, params, null, null, null, null, token);
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String jobId,
                                     String jobName, String jobDescription, List<String> jobTags, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("toolId", toolId)
                .append("priority", priority)
                .append("params", params)
                .append("token", token);

        Job job = new Job();
        job.setId(jobId);
        job.setName(jobName);
        job.setDescription(jobDescription);
        job.setTool(new ToolInfo().setId(toolId));
        job.setTags(jobTags);

        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.EXECUTION);

            job.setStudyUuid(study.getUuid());
            job.setUserId(userId);
            job.setParams(params);
            job.setPriority(priority);

            autoCompleteNewJob(study, job, token);

            jobDBAdaptor.insert(study.getUid(), job, new QueryOptions());
            OpenCGAResult<Job> jobResult = jobDBAdaptor.get(job.getUid(), new QueryOptions());

            auditManager.auditCreate(userId, Enums.Resource.JOB, job.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return jobResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.JOB, job.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));

            job.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED));
            jobDBAdaptor.insert(study.getUid(), job, new QueryOptions());

            throw e;
        }
    }

    public OpenCGAResult count(Query query, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        authorizationManager.checkIsAdmin(userId);

        return jobDBAdaptor.count(query);
    }

    public DBIterator<Job> iterator(Query query, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        authorizationManager.checkIsAdmin(userId);

        return jobDBAdaptor.iterator(query, options);
    }

    public OpenCGAResult<Job> get(long jobId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(jobId), options, sessionId);
    }

    public OpenCGAResult<Job> get(List<String> jobIds, QueryOptions options, boolean ignoreException, String sessionId)
            throws CatalogException {
        return get(null, jobIds, options, ignoreException, sessionId);
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        if (query.containsKey("inputFiles")) {
            List<File> inputFiles = catalogManager.getFileManager().internalGet(study.getUid(), query.getAsStringList("inputFiles"),
                    FileManager.INCLUDE_FILE_IDS, userId, true).getResults();
            if (ListUtils.isNotEmpty(inputFiles)) {
                query.put(JobDBAdaptor.QueryParams.INPUT_UID.key(), inputFiles.stream().map(File::getUid).collect(Collectors.toList()));
            } else {
                // We add 0 so the query returns no results
                query.put(JobDBAdaptor.QueryParams.INPUT_UID.key(), 0);
            }
            query.remove("inputFiles");
        }
        if (query.containsKey("outputFiles")) {
            List<File> inputFiles = catalogManager.getFileManager().internalGet(study.getUid(), query.getAsStringList("outputFiles"),
                    FileManager.INCLUDE_FILE_IDS, userId, true).getResults();
            if (ListUtils.isNotEmpty(inputFiles)) {
                query.put(JobDBAdaptor.QueryParams.OUTPUT_UID.key(), inputFiles.stream().map(File::getUid).collect(Collectors.toList()));
            } else {
                // We add 0 so the query returns no results
                query.put(JobDBAdaptor.QueryParams.OUTPUT_UID.key(), 0);
            }
            query.remove("outputFiles");
        }
    }

    @Override
    public OpenCGAResult<Job> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
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
            fixQueryObject(study, query, userId);
            query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            Future<OpenCGAResult<Long>> countFuture = null;
            if (options.getBoolean(QueryOptions.COUNT)) {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Query finalQuery = query;
                countFuture = executor.submit(() -> jobDBAdaptor.count(study.getUid(), finalQuery, userId,
                        StudyAclEntry.StudyPermissions.VIEW_JOBS));
            }
            OpenCGAResult<Job> queryResult = OpenCGAResult.empty();
            if (options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT) > 0) {
                queryResult = jobDBAdaptor.get(study.getUid(), query, options, userId);
            }
            if (countFuture != null) {
                mergeCount(queryResult, countFuture);
            }
            auditManager.auditSearch(userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        } catch (InterruptedException | ExecutionException e) {
            auditManager.auditSearch(userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(-1, "", e.getMessage())));
            throw new CatalogException("Unexpected error", e);
        }
    }

    @Override
    public DBIterator<Job> iterator(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        fixQueryObject(study, query, userId);

        return jobDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<Job> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(study, query, userId);

            query.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = jobDBAdaptor.count(study.getUid(), query, userId,
                    StudyAclEntry.StudyPermissions.VIEW_JOBS);

            auditManager.auditCount(userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> jobIds, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, jobIds, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> jobIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobIds", jobIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        for (String id : jobIds) {
            String jobId = id;
            String jobUuid = "";

            try {
                OpenCGAResult<Job> internalResult = internalGet(study.getUid(), id, INCLUDE_JOB_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }

                Job job = internalResult.first();
                // We set the proper values for the audit
                jobId = job.getId();
                jobUuid = job.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.DELETE);
                }

                // Check if the job can be deleted
                checkJobCanBeDeleted(job);

                result.append(jobDBAdaptor.delete(job));

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot delete job {}: {}", jobId, e.getMessage(), e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, jobId, jobUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyId, Query query, ObjectMap params, String token) throws CatalogException {
        return delete(studyId, query, params, false, token);
    }

    public OpenCGAResult delete(String studyId, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        OpenCGAResult result = OpenCGAResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the jobs to be deleted
        DBIterator<Job> iterator;
        try {
            fixQueryObject(study, query, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = jobDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        while (iterator.hasNext()) {
            Job job = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.DELETE);
                }

                // Check if the job can be deleted
                checkJobCanBeDeleted(job);

                result.append(jobDBAdaptor.delete(job));

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete job " + job.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg, e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        return endResult(result, ignoreException);
    }

    private void checkJobCanBeDeleted(Job job) throws CatalogException {
        switch (job.getStatus().getName()) {
            case Enums.ExecutionStatus.DELETED:
                throw new CatalogException("Job already deleted.");
            case Enums.ExecutionStatus.PENDING:
            case Enums.ExecutionStatus.RUNNING:
            case Enums.ExecutionStatus.QUEUED:
                throw new CatalogException("The status of the job is " + job.getStatus().getName()
                        + ". Please, stop the job before deleting it.");
            default:
                break;
        }
    }

    public OpenCGAResult<Job> update(String studyStr, Query query, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyStr, Query query, JobUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        DBIterator<Job> iterator;
        try {
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = jobDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            try {
                OpenCGAResult<Job> updateResult = update(study, job, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update job {}: {}", job.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        return endResult(result, ignoreException);
    }

    /**
     * Update Job from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param jobIds  List of Job ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult with the objects updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Job> update(String studyStr, List<String> jobIds, JobUpdateParams updateParams, QueryOptions options,
                                     String token) throws CatalogException {
        return update(studyStr, jobIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyStr, List<String> jobIds, JobUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobIds", jobIds)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        for (String id : jobIds) {
            String jobId = id;
            String jobUuid = "";

            try {
                OpenCGAResult<Job> internalResult = internalGet(study.getUid(), id, INCLUDE_JOB_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }
                Job job = internalResult.first();

                // We set the proper values for the audit
                jobId = job.getId();
                jobUuid = job.getUuid();

                OpenCGAResult<Job> updateResult = update(study, job, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update job {}: {}", jobId, e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Job> update(String studyStr, String jobId, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobId", jobId)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        String jobUuid = "";
        try {
            OpenCGAResult<Job> internalResult = internalGet(study.getUid(), jobId, INCLUDE_JOB_IDS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Job '" + jobId + "' not found");
            }
            Job job = internalResult.first();

            // We set the proper values for the audit
            jobId = job.getId();
            jobUuid = job.getUuid();

            OpenCGAResult updateResult = update(study, job, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Could not update job {}: {}", jobId, e.getMessage(), e);
            auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    private OpenCGAResult<Job> update(Study study, Job job, JobUpdateParams updateParams, QueryOptions options, String userId)
            throws CatalogException {
        if (updateParams == null) {
            throw new CatalogException("Missing parameters to update");
        }
        if (updateParams.getUpdateMap().isEmpty()) {
            throw new CatalogException("Missing parameters to update");
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);

//        if (StringUtils.isNotEmpty(updateParams.getId())) {
//            ParamUtils.checkAlias(updateParams.getId(), JobDBAdaptor.QueryParams.ID.key());
//        }
//
//        if (updateParams.getOutDir() != null && updateParams.getOutDir().getUid() <= 0) {
//            updateParams.setOutDir(getFile(study.getUid(), updateParams.getOutDir().getPath(), userId));
//        }
//
//        if (ListUtils.isNotEmpty(updateParams.getInput())) {
//            List<File> inputFiles = new ArrayList<>(updateParams.getInput().size());
//            for (File file : updateParams.getInput()) {
//                if (file.getUid() <= 0) {
//                    inputFiles.add(getFile(study.getUid(), file.getPath(), userId));
//                } else {
//                    inputFiles.add(file);
//                }
//            }
//            updateParams.setInput(inputFiles);
//        }
//
//        if (ListUtils.isNotEmpty(updateParams.getOutput())) {
//            List<File> outputFiles = new ArrayList<>(updateParams.getOutput().size());
//            for (File file : updateParams.getOutput()) {
//                if (file.getUid() <= 0) {
//                    outputFiles.add(getFile(study.getUid(), file.getPath(), userId));
//                } else {
//                    outputFiles.add(file);
//                }
//            }
//            updateParams.setOutput(outputFiles);
//        }
//
//        if (updateParams.getLog() != null && updateParams.getLog().getUid() <= 0) {
//            updateParams.setLog(getFile(study.getUid(), updateParams.getLog().getPath(), userId));
//        }
//
//        if (updateParams.getErrorLog() != null && updateParams.getErrorLog().getUid() <= 0) {
//            updateParams.setErrorLog(getFile(study.getUid(), updateParams.getErrorLog().getPath(), userId));
//        }

        return jobDBAdaptor.update(job.getUid(), updateParams.getUpdateMap(), options);
    }

    private File getFile(long studyUid, String path, String userId) throws CatalogException {
        if (StringUtils.isEmpty(path)) {
            throw new CatalogException("Missing file path");
        }

        OpenCGAResult<File> fileResult = catalogManager.getFileManager().internalGet(studyUid, path, FileManager.INCLUDE_FILE_URI_PATH,
                userId);
        if (fileResult.getNumResults() == 0) {
            throw new CatalogException("File/Folder '" + path + "' not found");
        }
        return fileResult.first();
    }

    public OpenCGAResult<Job> update(String studyId, Query query, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        return update(studyId, query, parameters, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyId, Query query, ObjectMap parameters, boolean ignoreException, QueryOptions options,
                                     String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("query", query)
                .append("parameters", parameters)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        ParamUtils.checkObj(parameters, "parameters");

        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        DBIterator<Job> iterator;
        try {
            fixQueryObject(study, finalQuery, token);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = jobDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            try {
                options = ParamUtils.defaultObject(options, QueryOptions::new);

                authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);

                OpenCGAResult updateResult = jobDBAdaptor.update(job.getUid(), parameters, options);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot update job {}: {}", job.getId(), e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Job> update(String studyId, String jobId, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("jobId", jobId)
                .append("parameters", parameters)
                .append("options", options)
                .append("token", token);

        ParamUtils.checkObj(parameters, "parameters");

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        String jobUuid = "";
        try {
            OpenCGAResult<Job> internalResult = internalGet(study.getUid(), jobId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Job '" + jobId + "' not found");
            }
            Job job = internalResult.first();

            // We set the proper values for the audit
            jobId = job.getId();
            jobUuid = job.getUuid();

            options = ParamUtils.defaultObject(options, QueryOptions::new);

            authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);

            OpenCGAResult updateResult = jobDBAdaptor.update(job.getUid(), parameters, options);
            result.append(updateResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Cannot update job {}: {}", jobId, e.getMessage());
            auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    public OpenCGAResult<Job> update(String studyId, List<String> jobIds, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        return update(studyId, jobIds, parameters, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyId, List<String> jobIds, ObjectMap parameters, boolean ignoreException,
                                     QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("jobIds", jobIds)
                .append("parameters", parameters)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        ParamUtils.checkObj(parameters, "parameters");

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        for (String id : jobIds) {
            String jobId = id;
            String jobUuid = "";

            try {
                OpenCGAResult<Job> internalResult = internalGet(study.getUid(), id, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }
                Job job = internalResult.first();

                // We set the proper values for the audit
                jobId = job.getId();
                jobUuid = job.getUuid();

                options = ParamUtils.defaultObject(options, QueryOptions::new);

                authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);

                OpenCGAResult updateResult = jobDBAdaptor.update(job.getUid(), parameters, options);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot update job {}: {}", jobId, e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        return endResult(result, ignoreException);
    }

//    public void setStatus(String studyId, String jobId, String status, String message, String token) throws CatalogException {
//        String userId = userManager.getUserId(token);
//        Study study = studyManager.resolveId(studyId, userId);
//
//        ObjectMap auditParams = new ObjectMap()
//                .append("study", studyId)
//                .append("jobId", jobId)
//                .append("status", status)
//                .append("message", message)
//                .append("token", token);
//        Job job;
//        try {
//            job = internalGet(study.getUid(), jobId, INCLUDE_JOB_IDS, userId).first();
//        } catch (CatalogException e) {
//            auditManager.auditUpdate(userId, Enums.Resource.JOB, jobId, "", study.getId(), study.getUuid(), auditParams,
//                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
//            throw e;
//        }
//
//        try {
//            authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);
//
//            if (status != null && !Job.JobStatus.isValid(status)) {
//                throw new CatalogException("The status " + status + " is not valid job status.");
//            }
//
//            ObjectMap parameters = new ObjectMap();
//            parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
//            parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_MSG.key(), message);
//
//            jobDBAdaptor.update(job.getUid(), parameters, QueryOptions.empty());
//            auditManager.auditUpdate(userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(), study.getUuid(),
//                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
//        } catch (CatalogException e) {
//            auditManager.auditUpdate(userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(), study.getUuid(),
//                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
//            throw e;
//        }
//    }

    public OpenCGAResult<JobsTop> top(String study, Query baseQuery, int limit, String token) throws CatalogException {
        StopWatch stopWatch = StopWatch.createStarted();
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, "id,name,status,execution,creationDate")
                .append(QueryOptions.COUNT, false)
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

        int jobsLimit = limit;
        OpenCGAResult<Job> running = catalogManager.getJobManager().search(
                study,
                new Query(baseQuery)
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.RUNNING),
                new QueryOptions(queryOptions)
                        .append(QueryOptions.LIMIT, jobsLimit)
                        .append(QueryOptions.SORT, "execution.start"),
                token);
        jobsLimit -= running.getResults().size();

        OpenCGAResult<Job> queued = catalogManager.getJobManager().search(
                study,
                new Query(baseQuery)
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED),
                new QueryOptions(queryOptions)
                        .append(QueryOptions.LIMIT, jobsLimit)
                        .append(QueryOptions.SORT, "creationDate"),
                token);
        jobsLimit -= queued.getResults().size();

        OpenCGAResult<Job> pending = catalogManager.getJobManager().search(
                study,
                new Query(baseQuery)
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.PENDING),
                new QueryOptions(queryOptions)
                        .append(QueryOptions.LIMIT, jobsLimit)
                        .append(QueryOptions.SORT, "creationDate"),
                token);
        jobsLimit -= pending.getResults().size();

        List<Job> finishedJobs = catalogManager.getJobManager().search(
                study,
                new Query(baseQuery)
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.DONE + ","
                                + Enums.ExecutionStatus.ERROR + ","
                                + Enums.ExecutionStatus.ABORTED),
                new QueryOptions(queryOptions)
                        .append(QueryOptions.LIMIT, Math.max(1, jobsLimit))
                        .append(QueryOptions.SORT, "execution.end")
                        .append(QueryOptions.ORDER, QueryOptions.DESCENDING), // Get last n elements,
                token).getResults();
        Collections.reverse(finishedJobs); // Reverse elements

        List<Job> allJobs = new ArrayList<>(running.getResults().size() + pending.getResults().size() + queued.getResults().size());
        allJobs.addAll(finishedJobs);
        allJobs.addAll(running.getResults());
        allJobs.addAll(queued.getResults());
        allJobs.addAll(pending.getResults());
        Map<String, Long> jobStatusCount = new HashMap<>();

        OpenCGAResult result = catalogManager.getJobManager().groupBy(study, new Query(baseQuery), JobDBAdaptor.QueryParams.STATUS_NAME.key(),
                new QueryOptions(QueryOptions.COUNT, true), token);
        for (Object o : result.getResults()) {
            String status = ((Map) ((Map) o).get("_id")).get("status.name").toString();
            long count = ((Number) ((Map) o).get("count")).longValue();
            jobStatusCount.put(status, count);
        }
        JobsTop top = new JobsTop(Date.from(Instant.now()), jobStatusCount, allJobs);
        return new OpenCGAResult<>(((int) stopWatch.getTime()), null, 1, Collections.singletonList(top), 1);
    }

    @Override
    public OpenCGAResult rank(String studyId, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(token, "sessionId");

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_JOBS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogJobDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = jobDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyId, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = jobDBAdaptor.groupBy(study.getUid(), query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> jobList, String member, boolean ignoreException,
                                                            String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobList", jobList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        try {
            OpenCGAResult<Map<String, List<String>>> jobAclList = OpenCGAResult.empty();
            InternalGetDataResult<Job> queryResult = internalGet(study.getUid(), jobList, INCLUDE_JOB_IDS, user, ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String jobId : jobList) {
                if (!missingMap.containsKey(jobId)) {
                    Job job = queryResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allJobAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allJobAcls = authorizationManager.getJobAcl(study.getUid(), job.getUid(), user, member);
                        } else {
                            allJobAcls = authorizationManager.getAllJobAcls(study.getUid(), job.getUid(), user);
                        }
                        jobAclList.append(allJobAcls);
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, job.getId(),
                                job.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, job.getId(),
                                job.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, jobId, missingMap.get(jobId).getErrorMsg());
                            jobAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, jobId, missingMap.get(jobId).getErrorMsg());
                    jobAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, jobId, "", study.getId(),
                            study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(jobId).getErrorMsg())), new ObjectMap());
                }
            }
            return jobAclList;
        } catch (CatalogException e) {
            for (String jobId : jobList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, jobId, "", study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> jobStrList, String memberList,
                                                              AclParams aclParams, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobStrList", jobStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("token", token);
        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        try {
            if (jobStrList == null || jobStrList.isEmpty()) {
                throw new CatalogException("Missing job parameter");
            }

            if (aclParams.getAction() == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, JobAclEntry.JobPermissions::valueOf);
            }

            List<Job> jobList = internalGet(study.getUid(), jobStrList, INCLUDE_JOB_IDS, userId, false).getResults();

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

            OpenCGAResult<Map<String, List<String>>> queryResultList;
            switch (aclParams.getAction()) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), jobList.stream().map(Job::getUid)
                            .collect(Collectors.toList()), members, permissions, Enums.Resource.JOB);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), jobList.stream().map(Job::getUid)
                            .collect(Collectors.toList()), members, permissions, Enums.Resource.JOB);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(jobList.stream().map(Job::getUid).collect(Collectors.toList()),
                            members, permissions, Enums.Resource.JOB);
                    break;
                case RESET:
                    queryResultList = authorizationManager.removeAcls(jobList.stream().map(Job::getUid).collect(Collectors.toList()),
                            members, null, Enums.Resource.JOB);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Job job : jobList) {
                auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.JOB, job.getId(),
                        job.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (jobStrList != null) {
                for (String jobId : jobStrList) {
                    auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.JOB, jobId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

}
