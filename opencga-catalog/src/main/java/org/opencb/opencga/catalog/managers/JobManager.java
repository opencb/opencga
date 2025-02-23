/*
 * Copyright 2015-2020 OpenCB
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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobManager extends ResourceManager<Job> {

    public static final QueryOptions INCLUDE_JOB_IDS = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.UUID.key(),
                    JobDBAdaptor.QueryParams.STUDY_UID.key(), JobDBAdaptor.QueryParams.INTERNAL.key()));
    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);
    private final String defaultFacet = "creationYear>>creationMonth;toolId>>executorId";
    private UserManager userManager;
    private StudyManager studyManager;
    private IOManagerFactory ioManagerFactory;

    JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
               DBAdaptorFactory catalogDBAdaptorFactory, IOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
        this.ioManagerFactory = ioManagerFactory;
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.JOB;
    }

//    @Override
//    OpenCGAResult<Job> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
//            throws CatalogException {
//        ParamUtils.checkIsSingleID(entry);
//        Query queryCopy = query == null ? new Query() : new Query(query);
//        queryCopy.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
//
//        if (UuidUtils.isOpenCgaUuid(entry)) {
//            queryCopy.put(JobDBAdaptor.QueryParams.UUID.key(), entry);
//        } else {
//            queryCopy.put(JobDBAdaptor.QueryParams.ID.key(), entry);
//        }
////        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
////                JobDBAdaptor.QueryParams.UUID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.STUDY_UID.key(),
////                JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.STATUS.key()));
//        OpenCGAResult<Job> jobDataResult = getJobDBAdaptor(organizationId).get(studyUid, queryCopy, options, user);
//        if (jobDataResult.getNumResults() == 0) {
//            jobDataResult = getJobDBAdaptor(organizationId).get(queryCopy, options);
//            if (jobDataResult.getNumResults() == 0) {
//                throw new CatalogException("Job " + entry + " not found");
//            } else {
//                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the job " + entry);
//            }
//        } else if (jobDataResult.getNumResults() > 1) {
//            throw new CatalogException("More than one job found based on " + entry);
//        } else {
//            return jobDataResult;
//        }
//    }

    @Override
    InternalGetDataResult<Job> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                           QueryOptions options, String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing job entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);

        Function<Job, String> jobStringFunction = Job::getId;
        JobDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            JobDBAdaptor.QueryParams param = JobDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
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

        if (studyUid <= 0) {
            if (!idQueryParam.equals(JobDBAdaptor.QueryParams.UUID)) {
                // studyUid is mandatory except for uuids
                throw new CatalogException("Missing mandatory study");
            }

            // If studyUid has not been provided, we will look for
            OpenCGAResult<Job> jobDataResult = getJobDBAdaptor(organizationId).get(queryCopy, options);
            for (Job job : jobDataResult.getResults()) {
                // Check view permissions
                authorizationManager.checkJobPermission(organizationId, job.getStudyUid(), job.getUid(), user, JobPermissions.VIEW);
            }
            return keepOriginalOrder(uniqueList, jobStringFunction, jobDataResult, ignoreException, false);
        }

        queryCopy.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        OpenCGAResult<Job> jobDataResult = getJobDBAdaptor(organizationId).get(studyUid, queryCopy, options, user);
        if (ignoreException || jobDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, jobStringFunction, jobDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<Job> resultsNoCheck = getJobDBAdaptor(organizationId).get(queryCopy, queryOptions);
        if (resultsNoCheck.getNumResults() == jobDataResult.getNumResults()) {
            throw CatalogException.notFound("jobs", getMissingFields(uniqueList, jobDataResult.getResults(), jobStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the jobs.");
        }
    }

    private OpenCGAResult<Job> getJob(String organizationId, long studyUid, String jobUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(JobDBAdaptor.QueryParams.UUID.key(), jobUuid);
        return getJobDBAdaptor(organizationId).get(query, options);
    }

    public Long getStudyId(String organizationId, long jobId) throws CatalogException {
        return getJobDBAdaptor(organizationId).getStudyId(jobId);
    }

    public OpenCGAResult<Job> visit(String studyId, String jobId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobId", jobId)
                .append("token", token);
        try {
            JobUpdateParams updateParams = new JobUpdateParams().setVisited(true);
            Job job = internalGet(organizationId, study.getUid(), jobId, INCLUDE_JOB_IDS, userId).first();

            OpenCGAResult result = update(organizationId, study, job, updateParams, QueryOptions.empty(), userId);
            auditManager.audit(organizationId, userId, Enums.Action.VISIT, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.VISIT, Enums.Resource.JOB, jobId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Job> create(String studyStr, Job job, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("job", job)
                .append("options", options)
                .append("token", token);
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_JOBS);

            ParamUtils.checkObj(job, "Job");
            ParamUtils.checkIdentifier(job.getId(), "job id");
            job.setDescription(ParamUtils.defaultString(job.getDescription(), ""));
            job.setCommandLine(ParamUtils.defaultString(job.getCommandLine(), ""));
            job.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(job.getCreationDate(),
                    JobDBAdaptor.QueryParams.CREATION_DATE.key()));
            job.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(job.getModificationDate(),
                    JobDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
            job.setInternal(ParamUtils.defaultObject(job.getInternal(), new JobInternal()));
            job.getInternal().setStatus(ParamUtils.defaultObject(job.getInternal().getStatus(),
                    new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE)));
            job.getInternal().setWebhook(ParamUtils.defaultObject(job.getInternal().getWebhook(),
                    new JobInternalWebhook(null, new HashMap<>())));
            job.getInternal().setEvents(ParamUtils.defaultObject(job.getInternal().getEvents(), new LinkedList<>()));
            job.setPriority(ParamUtils.defaultObject(job.getPriority(), Enums.Priority.MEDIUM));
            job.setInput(ParamUtils.defaultObject(job.getInput(), Collections.emptyList()));
            job.setOutput(ParamUtils.defaultObject(job.getOutput(), Collections.emptyList()));
            job.setParams(ParamUtils.defaultObject(job.getParams(), HashMap::new));
            job.setAttributes(ParamUtils.defaultObject(job.getAttributes(), HashMap::new));
            job.setUserId(userId);
            job.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            job.setOutDir(job.getOutDir() != null && StringUtils.isNotEmpty(job.getOutDir().getPath()) ? job.getOutDir() : null);
            job.setStudy(new JobStudyParam(study.getFqn()));

            if (!Arrays.asList(Enums.ExecutionStatus.ABORTED, Enums.ExecutionStatus.DONE, Enums.ExecutionStatus.UNREGISTERED,
                    Enums.ExecutionStatus.ERROR).contains(job.getInternal().getStatus().getId())) {
                throw new CatalogException("Cannot create a job in a status different from one of the final ones.");
            }

            if (ListUtils.isNotEmpty(job.getInput())) {
                List<File> inputFiles = new ArrayList<>(job.getInput().size());
                for (File file : job.getInput()) {
                    inputFiles.add(getFile(organizationId, study.getUid(), file.getPath(), userId));
                }
                job.setInput(inputFiles);
            }
            if (ListUtils.isNotEmpty(job.getOutput())) {
                List<File> outputFiles = new ArrayList<>(job.getOutput().size());
                for (File file : job.getOutput()) {
                    outputFiles.add(getFile(organizationId, study.getUid(), file.getPath(), userId));
                }
                job.setOutput(outputFiles);
            }
            if (job.getOutDir() != null && StringUtils.isNotEmpty(job.getOutDir().getPath())) {
                job.setOutDir(getFile(organizationId, study.getUid(), job.getOutDir().getPath(), userId));
                if (job.getOutDir().getType() != File.Type.DIRECTORY) {
                    throw new CatalogException("Unexpected outDir type. Expected " + File.Type.DIRECTORY);
                }
            }
            if (job.getStdout() != null && StringUtils.isNotEmpty(job.getStdout().getPath())) {
                job.setStdout(getFile(organizationId, study.getUid(), job.getStdout().getPath(), userId));
            }
            if (job.getStderr() != null && StringUtils.isNotEmpty(job.getStderr().getPath())) {
                job.setStderr(getFile(organizationId, study.getUid(), job.getStderr().getPath(), userId));
            }

            job.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.JOB));
            OpenCGAResult<Job> insert = getJobDBAdaptor(organizationId).insert(study.getUid(), job, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created job
                OpenCGAResult<Job> queryResult = getJob(organizationId, study.getUid(), job.getUuid(), options);
                insert.setResults(queryResult.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return insert;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.JOB, job.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    private void autoCompleteNewJob(String organizationId, Study study, Job job, JwtPayload tokenPayload) throws CatalogException {
        ParamUtils.checkObj(job, "Job");

        // Auto generate id
        if (StringUtils.isEmpty(job.getId())) {
            job.setId(job.getTool().getId() + "." + TimeUtils.getTime() + "." + RandomStringUtils.randomAlphanumeric(6));
        }
        job.setPriority(ParamUtils.defaultObject(job.getPriority(), Enums.Priority.MEDIUM));
        job.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.JOB));
        job.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(job.getCreationDate(), JobDBAdaptor.QueryParams.CREATION_DATE.key()));
        job.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(job.getModificationDate(),
                JobDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        job.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));

        // Set default internal
        job.setInternal(JobInternal.init());
        job.getInternal().setWebhook(new JobInternalWebhook(study.getNotification().getWebhook(), new HashMap<>()));

        if (StringUtils.isNotEmpty(job.getParentId())) {
            // Check parent job exists
            try {
                Job tmpJob = internalGet(organizationId, study.getUid(), job.getParentId(), INCLUDE_JOB_IDS,
                        tokenPayload.getUserId(organizationId)).first();
                job.setParentId(tmpJob.getId());
            } catch (CatalogException e) {
                throw new CatalogException("Parent job '" + job.getParentId() + "' not found", e);
            }
        } else {
            job.setParentId("");
        }

        if (StringUtils.isNotEmpty(job.getScheduledStartTime())) {
            ParamUtils.checkDateFormat(job.getScheduledStartTime(), FieldConstants.JOB_SCHEDULED_START_TIME);
            Date date = TimeUtils.toDate(job.getScheduledStartTime());
            if (date.before(new Date())) {
                throw new CatalogException("'" + FieldConstants.JOB_SCHEDULED_START_TIME + "' must be a future date");
            }
        }

        if (job.getDependsOn() != null && !job.getDependsOn().isEmpty()) {
            boolean uuidProvided = job.getDependsOn().stream().map(Job::getId).anyMatch(UuidUtils::isOpenCgaUuid);

            try {
                // If uuid is provided, we will remove the study uid from the query so it can be searched across any study
                InternalGetDataResult<Job> dependsOnResult;
                if (uuidProvided) {
                    dependsOnResult = internalGet(organizationId, 0,
                            job.getDependsOn().stream().map(Job::getId).collect(Collectors.toList()),
                            null, INCLUDE_JOB_IDS, job.getUserId(), false);
                } else {
                    dependsOnResult = internalGet(organizationId, study.getUid(),
                            job.getDependsOn().stream().map(Job::getId).collect(Collectors.toList()),
                            null, INCLUDE_JOB_IDS, job.getUserId(), false);
                }
                job.setDependsOn(dependsOnResult.getResults());
            } catch (CatalogException e) {
                throw new CatalogException("Unable to find the jobs this job depends on. " + e.getMessage(), e);
            }

            job.setInput(Collections.emptyList());
        } else {
            // We only check input files if the job does not depend on other job that might be creating the necessary file.

            List<File> inputFiles = getJobInputFilesFromParams(study.getFqn(), job, tokenPayload.getToken());
            job.setInput(inputFiles);
        }

        job.setAttributes(ParamUtils.defaultObject(job.getAttributes(), HashMap::new));
    }

    public OpenCGAResult<Job> kill(String studyStr, String jobId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobId", jobId)
                .append("token", token);
        String jobUuid = "";
        try {
            QueryOptions options = keepFieldInQueryOptions(INCLUDE_JOB_IDS, JobDBAdaptor.QueryParams.USER_ID.key());
            Job job = internalGet(organizationId, study.getUid(), jobId, options, userId).first();
            jobId = job.getId();
            jobUuid = job.getUuid();

            try {
                if (!job.getUserId().equals(userId)) {
                    // Check if the user is a study administrator
                    authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
                }
            } catch (CatalogException e) {
                throw CatalogAuthorizationException.deny(userId, "job", jobId, CatalogAuthorizationException.ErrorCode.KILL_JOB, e);
            }

            if (!job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.PENDING)
                    && !job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.QUEUED)
                    && !job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.RUNNING)) {
                throw new CatalogException("Cannot kill job '" + jobId + "' in status " + job.getInternal().getStatus().getId());
            }

            ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.INTERNAL_KILL_JOB_REQUESTED.key(), true);
            OpenCGAResult<Job> update = getCatalogDBAdaptorFactory().getCatalogJobDBAdaptor(organizationId).update(job.getUid(), params,
                    QueryOptions.empty());

            auditManager.audit(organizationId, userId, Enums.Action.KILL_JOB, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return update;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.KILL_JOB, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }


    public List<File> getJobInputFilesFromParams(String study, Job job, String token) throws CatalogException {
        // Look for input files
        String fileParamSuffix = "file";
        List<File> inputFiles = new ArrayList<>();
        if (job.getParams() != null) {
            for (Map.Entry<String, Object> entry : job.getParams().entrySet()) {
                // We assume that every variable ending in 'file' corresponds to input files that need to be accessible in catalog
                if (entry.getKey().toLowerCase().endsWith(fileParamSuffix)) {
                    for (String fileStr : StringUtils.split((String) entry.getValue(), ',')) {
                        try {
                            // Validate the user has access to the file
                            File file = catalogManager.getFileManager().get(study, fileStr,
                                    FileManager.INCLUDE_FILE_URI_PATH, token).first();
                            inputFiles.add(file);
                        } catch (CatalogException e) {
                            throw new CatalogException("Cannot find file '" + entry.getValue() + "' "
                                    + "from job param '" + entry.getKey() + "'; (study = " + study + ") :" + e.getMessage(), e);
                        }
                    }
                } else if (entry.getValue() instanceof Map) {
                    // We look for files in the dynamic params
                    Map<String, Object> dynamicParams = (Map<String, Object>) entry.getValue();
                    for (Map.Entry<String, Object> subEntry : dynamicParams.entrySet()) {
                        if (subEntry.getKey().toLowerCase().endsWith(fileParamSuffix)) {
                            // We assume that every variable ending in 'file' corresponds to input files that need to be accessible in
                            // catalog
                            try {
                                // Validate the user has access to the file
                                File file = catalogManager.getFileManager().get(study, (String) subEntry.getValue(),
                                        FileManager.INCLUDE_FILE_URI_PATH, token).first();
                                inputFiles.add(file);
                            } catch (CatalogException e) {
                                throw new CatalogException("Cannot find file '" + subEntry.getValue() + "' from variable '"
                                        + entry.getKey() + "." + subEntry.getKey() + "'. ", e);
                            }
                        }
                    }
                }
            }
        }
        return inputFiles;
    }

    public OpenCGAResult<Job> retry(String studyStr, JobRetryParams jobRetry, Enums.Priority priority, String jobId, String jobDescription,
                                    List<String> jobDependsOn, List<String> jobTags, String jobScheduledStartTime, String token)
            throws CatalogException {
        Job job = get(studyStr, jobRetry.getJob(), new QueryOptions(), token).first();
        if (jobRetry.isForce()
                || job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ERROR)
                || job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ABORTED)) {
            Map<String, Object> params = new ObjectMap(job.getParams());
            if (jobRetry.getParams() != null) {
                params.putAll(jobRetry.getParams());
            }
            HashMap<String, Object> attributes = new HashMap<>();
            attributes.put("retry_from", jobRetry.getJob());
            if (StringUtils.isEmpty(jobDescription)) {
                jobDescription = "Retry from job '" + jobRetry.getJob() + "'";
            }
            return submit(studyStr, job.getTool().getId(), priority, params, jobId, jobDescription, jobDependsOn, jobTags, job.getId(),
                    jobScheduledStartTime, job.isDryRun(), attributes, token);
        } else {
            throw new CatalogException("Unable to retry job with status " + job.getInternal().getStatus().getId());
        }
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String token)
            throws CatalogException {
        return submit(studyStr, toolId, priority, params, null, null, null, null, null, null, false, token);
    }

    public OpenCGAResult<Job> submitProject(String projectStr, String toolId, Enums.Priority priority,
                                            Map<String, Object> params, String jobId, String jobDescription, List<String> jobDependsOn,
                                            List<String> jobTags, String token) throws CatalogException {
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
        return submit(studies.get(0), toolId, priority, params, jobId, jobDescription, jobDependsOn, jobTags, null, null, false, token);
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String jobId,
                                     String jobDescription, List<String> jobDependsOn, List<String> jobTags, @Nullable String jobParentId,
                                     @Nullable String scheduledStartTime, Boolean dryRun, String token) throws CatalogException {
        return submit(studyStr, toolId, priority, params, jobId, jobDescription, jobDependsOn, jobTags, jobParentId, scheduledStartTime,
                dryRun, null, token);
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String jobId,
                                     String jobDescription, List<String> jobDependsOn, List<String> jobTags,
                                     @Nullable String jobParentId, @Nullable String scheduledStartTime, Boolean dryRun,
                                     Map<String, Object> attributes, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("toolId", toolId)
                .append("jobPriority", priority)
                .append("params", params)
                .append("jobId", jobId)
                .append("jobDescription", jobDescription)
                .append("jobDependsOn", jobDependsOn)
                .append("jobTags", jobTags)
                .append("jobParentId", jobParentId)
                .append("jobScheduledStartTime", scheduledStartTime)
                .append("jobDryRun", dryRun)
                .append("token", token);
        Job job = new Job();
        job.setId(jobId);
        job.setDescription(jobDescription);
        job.setTool(new ToolInfo().setId(toolId));
        job.setTags(jobTags);
        job.setStudy(new JobStudyParam(study.getFqn()));
        job.setUserId(userId);
        job.setParams(params);
        job.setParentId(jobParentId);
        job.setScheduledStartTime(scheduledStartTime);
        job.setPriority(priority);
        job.setDryRun(dryRun != null && dryRun);
        job.setDependsOn(jobDependsOn != null
                ? jobDependsOn.stream().map(j -> new Job().setId(j)).collect(Collectors.toList())
                : Collections.emptyList());
        job.setAttributes(attributes);
        try {
            autoCompleteNewJob(organizationId, study, job, tokenPayload);
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.EXECUTE_JOBS);

            // Check if we have already reached the limit of job hours in the Organisation
            checkExecutionLimitQuota(organizationId);

            // Check params
            ParamUtils.checkObj(params, "params");
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                if (entry.getValue() == null) {
                    throw new CatalogException("Found '" + entry.getKey() + "' param with null value");
                }
            }

            OpenCGAResult<Job> reuseJob = getJobToReuse(organizationId, study, jobId, job, token);
            if (reuseJob != null) {
                job = reuseJob.first();
                auditManager.audit(organizationId, userId, Enums.Action.REUSE, Enums.Resource.JOB, job.getId(), job.getUuid(),
                        study.getId(),
                        study.getUuid(),
                        auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                return reuseJob;
            } else {
                getJobDBAdaptor(organizationId).insert(study.getUid(), job, new QueryOptions());
                OpenCGAResult<Job> jobResult = getJobDBAdaptor(organizationId).get(job.getUid(), new QueryOptions());

                auditManager.auditCreate(organizationId, userId, Enums.Resource.JOB, job.getId(), "", study.getId(), study.getUuid(),
                        auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return jobResult;
            }
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.JOB, job.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));

            job.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED));
            job.getInternal().getStatus().setDescription(e.toString());
            getJobDBAdaptor(organizationId).insert(study.getUid(), job, new QueryOptions());

            throw e;
        }
    }

    /**
     * Method to reschedule a list of jobs. Only intended for "Opencga" administrators.
     * @param studyStr study where the list of jobs belong.
     * @param jobUids  List of job uids to be rescheduled.
     * @param scheduledStartTime New scheduled start time.
     * @param token OpenCGA token.
     * @return OpenCGAResult with the list of jobs that have been rescheduled.
     * @throws CatalogException if there is any error.
     */
    public OpenCGAResult<Job> rescheduleJobs(String studyStr, List<Long> jobUids, String scheduledStartTime, String token)
            throws CatalogException {
        return rescheduleJobs(studyStr, jobUids, scheduledStartTime, null, token);
    }

    /**
     * Method to reschedule a list of jobs. Only intended for "Opencga" administrators.
     * @param studyStr study where the list of jobs belong.
     * @param jobUids  List of job uids to be rescheduled.
     * @param scheduledStartTime New scheduled start time.
     * @param eventMessage Event message to be added to the rescheduled jobs.
     * @param token OpenCGA token.
     * @return OpenCGAResult with the list of jobs that have been rescheduled.
     * @throws CatalogException if there is any error.
     */
    public OpenCGAResult<Job> rescheduleJobs(String studyStr, List<Long> jobUids, String scheduledStartTime, String eventMessage,
                                             String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobUids", jobUids)
                .append("scheduledStartTime", scheduledStartTime)
                .append("eventMessage", eventMessage)
                .append("token", token);
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);
            authorizationManager.checkIsOpencgaAdministrator(tokenPayload);

            if (CollectionUtils.isEmpty(jobUids)) {
                throw new CatalogException("Missing job uids");
            }
            if (StringUtils.isEmpty(scheduledStartTime)) {
                throw new CatalogException("Missing scheduled start time");
            }
            ParamUtils.checkDateIsNotExpired(scheduledStartTime, JobDBAdaptor.QueryParams.SCHEDULED_START_TIME.key());

            ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.SCHEDULED_START_TIME.key(), scheduledStartTime);
            QueryOptions queryOptions = new QueryOptions();

            if (StringUtils.isNotEmpty(eventMessage)) {
                // Add event message
                List<Event> eventList = Collections.singletonList(new Event(Event.Type.INFO, "reschedule", eventMessage));
                params.append(JobDBAdaptor.QueryParams.INTERNAL_EVENTS.key(), eventList);

                Map<String, Object> actionMap = new HashMap<>();
                actionMap.put(JobDBAdaptor.QueryParams.INTERNAL_EVENTS.key(), ParamUtils.BasicUpdateAction.ADD);
                queryOptions.getMap(Constants.ACTIONS, actionMap);
            }

            Query query = new Query()
                    .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(JobDBAdaptor.QueryParams.UID.key(), jobUids);
            OpenCGAResult<Job> update = getJobDBAdaptor(organizationId).update(query, params, queryOptions);

            auditManager.audit(organizationId, userId, Enums.Action.RESCHEDULE_JOB, Enums.Resource.JOB, "", "", "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return update;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.RESCHEDULE_JOB, Enums.Resource.JOB, "", "", "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    public OpenCGAResult<ExecutionTime> getExecutionTimeByMonth(String organizationId, Query query, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        ParamUtils.checkParameter(organizationId, "organizationId");
        authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId(organizationId));
        return getJobDBAdaptor(organizationId).executionTimeByMonth(query);
    }

    private void checkExecutionLimitQuota(String organizationId) throws CatalogException {
        if (exceedsExecutionLimitQuota(organizationId)) {
            throw new CatalogException("The organization '" + organizationId + "' has reached the maximum quota of execution hours ("
                    + configuration.getQuota().getMaxNumJobHours() + ") for the current month.");
        }
    }

    public boolean exceedsExecutionLimitQuota(String organizationId) throws CatalogException {
        if (configuration.getQuota().getMaxNumJobHours() <= 0) {
            return false;
        }
        // Get current year/month
        String time = TimeUtils.getTime(TimeUtils.getDate(), TimeUtils.yyyyMM);
        Query query = new Query(JobDBAdaptor.QueryParams.EXECUTION_END.key(), time);
        OpenCGAResult<ExecutionTime> result = getJobDBAdaptor(organizationId).executionTimeByMonth(query);
        if (result.getNumResults() > 0) {
            ExecutionTime executionTime = result.first();
            if (executionTime.getTime().getHours() >= configuration.getQuota().getMaxNumJobHours()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the job is eligible to be reused, and if so, try to find an equivalent job.
     * Eligible job:
     * - Not enforced jobId
     * - Tool.id within list of "tools-to-reuse"
     * Equivalent job:
     * - Internal.status.id = PENDING | QUEUED
     * - Params -> Same as the input job
     * - Same job dependencies
     *
     * @param organizationId Organization id.
     * @param study          Study
     * @param inputJobId     Enforced JobId
     * @param job            Job to be created
     * @param token          User token
     * @return A job to be reused (if any)
     * @throws CatalogException on error
     */
    private OpenCGAResult<Job> getJobToReuse(String organizationId, Study study, String inputJobId, Job job, String token)
            throws CatalogException {

        // First check if the job can be reused
        if (!jobEligibleToReuse(inputJobId, job)) {
            return null;
        }

        // Get candidates
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), job.getTool().getId())
                .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(),
                        Arrays.asList(Enums.ExecutionStatus.PENDING, Enums.ExecutionStatus.QUEUED));

        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 10)
                .append(QueryOptions.INCLUDE, Arrays.asList(JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.PARAMS.key()));

        DBIterator<Job> it = iterator(study.getFqn(), query, options, token);
        while (it.hasNext()) {
            Job candidateJob = it.next();
            // Compare params orderless
            if (new HashMap<>(job.getParams()).equals(new HashMap<>(candidateJob.getParams()))) {
                // This is a valid candidate!
                OpenCGAResult<Job> result = getJobDBAdaptor(organizationId).get(candidateJob.getUid(), new QueryOptions());
                result.addEvent(new Event(Event.Type.WARNING, "reuse",
                        "Another job which is pending execution was already created using the exact same parameters. "
                                + "Returning that job instead of creating a new one."));
                return result;
            }
        }

        return null;
    }

    private boolean jobEligibleToReuse(String inputJobId, Job job) {
        boolean enabled = configuration.getAnalysis().getExecution().getOptions()
                .getBoolean(Execution.JOBS_REUSE_ENABLED, Execution.JOBS_REUSE_ENABLED_DEFAULT);
        if (!enabled) {
            return false;
        }

        if (StringUtils.isNotEmpty(inputJobId)) {
            // Do not allow reusing a job if a `jobId` is provided
            return false;
        }

        List<String> availableTools = configuration.getAnalysis().getExecution().getOptions()
                .getAsStringList(Execution.JOBS_REUSE_TOOLS);
        if (availableTools.isEmpty()) {
            availableTools = Execution.JOBS_REUSE_TOOLS_DEFAULT;
        }
        String toolId = job.getTool().getId();
        boolean validTool = false;
        for (String availableTool : availableTools) {
            if (availableTool.equals(toolId) || toolId.matches(availableTool)) {
                validTool = true;
                break;
            }
        }
        return validTool;
    }

    private void fixQueryObject(String organizationId, Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.JOB_TOOL_ID_PARAM, JobDBAdaptor.QueryParams.TOOL_ID.key());
        changeQueryId(query, ParamConstants.JOB_TOOL_TYPE_PARAM, JobDBAdaptor.QueryParams.TOOL_TYPE.key());
        changeQueryId(query, ParamConstants.JOB_INTERNAL_STATUS_PARAM, JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());
        changeQueryId(query, ParamConstants.JOB_STATUS_PARAM, JobDBAdaptor.QueryParams.STATUS_ID.key());

        if (query.containsKey(ParamConstants.JOB_INPUT_FILES_PARAM)) {
            List<File> inputFiles = catalogManager.getFileManager().internalGet(organizationId, study.getUid(),
                    query.getAsStringList(ParamConstants.JOB_INPUT_FILES_PARAM), FileManager.INCLUDE_FILE_IDS, userId, true).getResults();
            if (ListUtils.isNotEmpty(inputFiles)) {
                query.put(JobDBAdaptor.QueryParams.INPUT_UID.key(), inputFiles.stream().map(File::getUid).collect(Collectors.toList()));
            } else {
                // We add 0 so the query returns no results
                query.put(JobDBAdaptor.QueryParams.INPUT_UID.key(), 0);
            }
            query.remove(ParamConstants.JOB_INPUT_FILES_PARAM);
        }
        if (query.containsKey(ParamConstants.JOB_OUTPUT_FILES_PARAM)) {
            List<File> inputFiles = catalogManager.getFileManager().internalGet(organizationId, study.getUid(),
                    query.getAsStringList(ParamConstants.JOB_OUTPUT_FILES_PARAM), FileManager.INCLUDE_FILE_IDS, userId, true).getResults();
            if (ListUtils.isNotEmpty(inputFiles)) {
                query.put(JobDBAdaptor.QueryParams.OUTPUT_UID.key(), inputFiles.stream().map(File::getUid).collect(Collectors.toList()));
            } else {
                // We add 0 so the query returns no results
                query.put(JobDBAdaptor.QueryParams.OUTPUT_UID.key(), 0);
            }
            query.remove(ParamConstants.JOB_OUTPUT_FILES_PARAM);
        }
    }

    @Override
    public OpenCGAResult<Job> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            fixQueryObject(organizationId, study, query, userId);
            query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Job> queryResult = getJobDBAdaptor(organizationId).get(study.getUid(), query, options, userId);
            auditManager.auditSearch(organizationId, userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (Exception e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fields", fields)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(organizationId, study, query, userId);

            query.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = getJobDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (Exception e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    public DBIterator<Job> iteratorInOrganization(String organizationId, Query query, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        authorizationManager.isOpencgaAdministrator(tokenPayload);
        return getJobDBAdaptor(organizationId).iterator(query, options);
    }

    @Override
    public DBIterator<Job> iterator(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        fixQueryObject(organizationId, study, query, userId);
        query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getJobDBAdaptor(organizationId).iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        fixQueryObject(organizationId, study, query, userId);
        query.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getJobDBAdaptor(organizationId).facet(study.getUid(), query, facet, userId);
    }

    public OpenCGAResult countInOrganization(String organizationId, Query query, String token) throws CatalogException {
        JwtPayload jwtPayload = userManager.validateToken(token);
        authorizationManager.checkIsOpencgaAdministrator(jwtPayload);
        return getCatalogDBAdaptorFactory().getCatalogJobDBAdaptor(organizationId).count(query);
    }

    @Override
    public OpenCGAResult<Job> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(organizationId, study, query, userId);

            query.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = getJobDBAdaptor(organizationId).count(query, userId);

            auditManager.auditCount(organizationId, userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (Exception e) {
            auditManager.auditCount(organizationId, userId, Enums.Resource.JOB, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> jobIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, jobIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> jobIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobIds", jobIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (Exception e) {
            auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        OpenCGAResult<Job> result = OpenCGAResult.empty();
        for (String id : jobIds) {
            String jobId = id;
            String jobUuid = "";

            try {
                OpenCGAResult<Job> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_JOB_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }

                Job job = internalResult.first();
                // We set the proper values for the audit
                jobId = job.getId();
                jobUuid = job.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkJobPermission(organizationId, study.getUid(), job.getUid(), userId, JobPermissions.DELETE);
                }

                // Check if the job can be deleted
                checkJobCanBeDeleted(job);

                result.append(getJobDBAdaptor(organizationId).delete(job));

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.JOB, job.getId(), job.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (Exception e) {
                Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot delete job {}: {}", jobId, e.getMessage(), e);
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.FAMILY, jobId, jobUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationUuid);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyId, query, options, false, token);
    }

    public OpenCGAResult delete(String studyId, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        OpenCGAResult result = OpenCGAResult.empty();

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

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
            fixQueryObject(organizationId, study, query, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getJobDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId1 = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId1, userId);
        } catch (Exception e) {
            auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Job job = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkJobPermission(organizationId, study.getUid(), job.getUid(), userId, JobPermissions.DELETE);
                }

                // Check if the job can be deleted
                checkJobCanBeDeleted(job);

                result.append(getJobDBAdaptor(organizationId).delete(job));

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.JOB, job.getId(), job.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (Exception e) {
                String errorMsg = "Cannot delete job " + job.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg, e);
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.JOB, job.getId(), job.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationUuid);

        return endResult(result, ignoreException);
    }

    private void checkJobCanBeDeleted(Job job) throws CatalogException {
        switch (job.getInternal().getStatus().getId()) {
            case Enums.ExecutionStatus.DELETED:
                throw new CatalogException("Job already deleted.");
            case Enums.ExecutionStatus.PENDING:
            case Enums.ExecutionStatus.RUNNING:
            case Enums.ExecutionStatus.QUEUED:
                throw new CatalogException("The status of the job is " + job.getInternal().getStatus().getId()
                        + ". Please, stop the job before deleting it.");
            default:
                break;
        }
    }

    public OpenCGAResult<FileContent> log(String studyId, String jobId, long offset, int lines, String type, boolean tail, String token)
            throws CatalogException {
        long startTime = System.currentTimeMillis();

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobId", jobId)
                .append("offset", offset)
                .append("lines", lines)
                .append("type", type)
                .append("tail", tail)
                .append("token", token);
        try {
            if (StringUtils.isEmpty(type)) {
                type = "stderr";
            }
            if (!"stderr".equalsIgnoreCase(type) && !"stdout".equalsIgnoreCase(type)) {
                throw new CatalogException("Incorrect log type. It must be 'stdout' or 'stderr'");
            }

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.UUID.key(),
                            JobDBAdaptor.QueryParams.INTERNAL_STATUS.key(), JobDBAdaptor.QueryParams.STDOUT.key(),
                            JobDBAdaptor.QueryParams.OUT_DIR.key()));
            Job job = internalGet(organizationId, study.getUid(), jobId, options, userId).first();

            Path logFile;
            if ("stderr".equalsIgnoreCase(type)) {
                if (job.getStderr() != null && job.getStderr().getUri() != null) {
                    logFile = Paths.get(job.getStderr().getUri());
                } else {
                    // The log file hasn't yet been registered
                    logFile = Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".err");
                }
            } else {
                if (job.getStdout() != null && job.getStdout().getUri() != null) {
                    logFile = Paths.get(job.getStdout().getUri());
                } else {
                    if (job.getOutDir() != null && job.getOutDir().getUri() != null) {
                        // The log file hasn't yet been registered
                        logFile = Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log");
                    } else {
                        throw CatalogAuthorizationException.deny(userId, "view log file");
                    }
                }
            }

            IOManager ioManager;
            try {
                ioManager = ioManagerFactory.get(logFile.toUri());
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(logFile.toUri(), e);
            }

            if (!ioManager.exists(logFile.toUri())) {
                String status = job.getInternal().getStatus().getId();
                throw new CatalogException("Job '" + jobId + "' in status '" + status + "'. Log file '" + type + "' not found");
            }

            FileContent fileContent;
            if (tail) {
                fileContent = ioManager.tail(logFile, lines);
            } else {
                fileContent = ioManager.head(logFile, offset, lines);
            }

            auditManager.audit(organizationId, userId, Enums.Action.VIEW_LOG, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 1,
                    Collections.singletonList(fileContent), 1);
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.VIEW_LOG, Enums.Resource.JOB, jobId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    public OpenCGAResult<Job> update(String studyStr, Query query, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyStr, Query query, JobUpdateParams updateParams, boolean ignoreException,
                                     QueryOptions options, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse JobUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        DBIterator<Job> iterator;
        try {
            fixQueryObject(organizationId, study, finalQuery, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getJobDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId);
        } catch (Exception e) {
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Job> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            try {
                OpenCGAResult<Job> updateResult = update(organizationId, study, job, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (Exception e) {
                Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update job {}: {}", job.getId(), e.getMessage(), e);
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Job> update(String studyStr, List<String> jobIds, JobUpdateParams updateParams, boolean ignoreException,
                                     QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse JobUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobIds", jobIds)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Job> result = OpenCGAResult.empty();
        for (String id : jobIds) {
            String jobId = id;
            String jobUuid = "";

            try {
                OpenCGAResult<Job> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_JOB_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }
                Job job = internalResult.first();

                // We set the proper values for the audit
                jobId = job.getId();
                jobUuid = job.getUuid();

                OpenCGAResult<Job> updateResult = update(organizationId, study, job, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (Exception e) {
                Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update job {}: {}", jobId, e.getMessage(), e);
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    /**
     * Update Job from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of
     *                     [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param jobIds       List of Job ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
     * @return A OpenCGAResult with the objects updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Job> update(String studyStr, List<String> jobIds, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, jobIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyStr, String jobId, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse JobUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobId", jobId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        String jobUuid = "";
        try {
            OpenCGAResult<Job> internalResult = internalGet(organizationId, study.getUid(), jobId, INCLUDE_JOB_IDS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Job '" + jobId + "' not found");
            }
            Job job = internalResult.first();

            // We set the proper values for the audit
            jobId = job.getId();
            jobUuid = job.getUuid();

            OpenCGAResult updateResult = update(organizationId, study, job, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (Exception e) {
            Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Could not update job {}: {}", jobId, e.getMessage(), e);
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }

        return result;
    }

    private OpenCGAResult<Job> update(String organizationId, Study study, Job job, JobUpdateParams updateParams, QueryOptions options,
                                      String userId) throws CatalogException {
        if (updateParams == null) {
            throw new CatalogException("Missing parameters to update");
        }

        ObjectMap updateMap;
        try {
            updateMap = updateParams.getUpdateMap();
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse JobUpdateParams object: " + e.getMessage(), e);
        }
        if (updateMap.isEmpty()) {
            throw new CatalogException("Missing parameters to update");
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        authorizationManager.checkJobPermission(organizationId, study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

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

        OpenCGAResult<Job> update = getJobDBAdaptor(organizationId).update(job.getUid(), updateMap, options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated job
            OpenCGAResult<Job> result = getJobDBAdaptor(organizationId).get(study.getUid(), new Query(JobDBAdaptor.QueryParams.UID.key(),
                            job.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
    }

    private File getFile(String organizationId, long studyUid, String path, String userId) throws CatalogException {
        if (StringUtils.isEmpty(path)) {
            throw new CatalogException("Missing file path");
        }

        OpenCGAResult<File> fileResult = catalogManager.getFileManager().internalGet(organizationId, studyUid, path,
                FileManager.INCLUDE_FILE_URI_PATH, userId);
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
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

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
            fixQueryObject(organizationId, study, finalQuery, token);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getJobDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId);
        } catch (Exception e) {
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            try {
                options = ParamUtils.defaultObject(options, QueryOptions::new);

                authorizationManager.checkJobPermission(organizationId, study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

                OpenCGAResult updateResult = getJobDBAdaptor(organizationId).update(job.getUid(), parameters, options);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (Exception e) {
                Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot update job {}: {}", job.getId(), e.getMessage());
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            }
        }

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Job> update(String studyId, String jobId, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

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
            OpenCGAResult<Job> internalResult = internalGet(organizationId, study.getUid(), jobId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Job '" + jobId + "' not found");
            }
            Job job = internalResult.first();

            // We set the proper values for the audit
            jobId = job.getId();
            jobUuid = job.getUuid();

            options = ParamUtils.defaultObject(options, QueryOptions::new);

            authorizationManager.checkJobPermission(organizationId, study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

            OpenCGAResult updateResult = getJobDBAdaptor(organizationId).update(job.getUid(), parameters, options);
            result.append(updateResult);

            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (Exception e) {
            Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Cannot update job {}: {}", jobId, e.getMessage());
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
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
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

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
                OpenCGAResult<Job> internalResult = internalGet(organizationId, study.getUid(), id, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Job '" + id + "' not found");
                }
                Job job = internalResult.first();

                // We set the proper values for the audit
                jobId = job.getId();
                jobUuid = job.getUuid();

                options = ParamUtils.defaultObject(options, QueryOptions::new);

                authorizationManager.checkJobPermission(organizationId, study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

                OpenCGAResult updateResult = getJobDBAdaptor(organizationId).update(job.getUid(), parameters, options);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, job.getId(), job.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (Exception e) {
                Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot update job {}: {}", jobId, e.getMessage());
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.JOB, jobId, jobUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            }
        }

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<JobTop> top(String organizationId, Query baseQuery, int limit, String token) throws CatalogException {
        JwtPayload jwtPayload = userManager.validateToken(token);
        String userId = jwtPayload.getUserId(organizationId);
        authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
        List<String> studies = studyManager.searchInOrganization(organizationId, new Query(), StudyManager.INCLUDE_STUDY_IDS, token)
                .getResults()
                .stream()
                .map(Study::getFqn)
                .collect(Collectors.toList());
        return top(studies, baseQuery, limit, token);
    }

    public OpenCGAResult<JobTop> top(String organizationId, String studyStr, Query baseQuery, int limit, String token)
            throws CatalogException {
        if (StringUtils.isEmpty(studyStr)) {
            return top(organizationId, baseQuery, limit, token);
        } else {
            return top(Collections.singletonList(studyStr), baseQuery, limit, token);
        }
    }

    public OpenCGAResult<JobTop> top(List<String> studiesStr, Query baseQuery, int limit, String token) throws CatalogException {
        JwtPayload payload = userManager.validateToken(token);
        List<Study> studies = new ArrayList<>(studiesStr.size());
        String organizationId = null;
        for (String studyStr : studiesStr) {
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, payload);
            if (organizationId == null) {
                organizationId = studyFqn.getOrganizationId();
            } else if (!organizationId.equals(studyFqn.getOrganizationId())) {
                throw new CatalogException("Organization id should be the same for all the studies.");
            }
            Study study = studyManager.resolveId(studyFqn, QueryOptions.empty(), payload);
            studies.add(study);
        }

        String userId = payload.getUserId(organizationId);
        fixQueryObject(organizationId, null, baseQuery, userId);

        StopWatch stopWatch = StopWatch.createStarted();
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.TOOL.key(),
                        JobDBAdaptor.QueryParams.INTERNAL.key(), JobDBAdaptor.QueryParams.EXECUTION.key(),
                        JobDBAdaptor.QueryParams.DEPENDS_ON.key(), JobDBAdaptor.QueryParams.CREATION_DATE.key(),
                        JobDBAdaptor.QueryParams.PRIORITY.key(), JobDBAdaptor.QueryParams.STUDY.key()))
                .append(QueryOptions.COUNT, false)
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);


        int jobsLimit = limit;
        List<Job> running = new ArrayList<>(jobsLimit);
        for (Study study : studies) {
            if (jobsLimit == 0) {
                break;
            }
            List<Job> results = getJobDBAdaptor(organizationId).get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.RUNNING),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, jobsLimit)
                            .append(QueryOptions.SORT, "execution.start"),
                    userId).getResults();
            running.addAll(results);
        }
        running.sort(Comparator.comparing(
                j -> j.getExecution() == null || j.getExecution().getStart() == null
                        ? new Date()
                        : j.getExecution().getStart()));
        if (running.size() > jobsLimit) {
            running = running.subList(0, jobsLimit);
        }
        jobsLimit -= running.size();

        List<Job> queued = new ArrayList<>(jobsLimit);
        for (Study study : studies) {
            if (jobsLimit == 0) {
                break;
            }
            List<Job> results = getJobDBAdaptor(organizationId).get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.QUEUED),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, jobsLimit)
                            .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key()),
                    userId).getResults();
            queued.addAll(results);
        }
        queued.sort(Comparator.comparing(Job::getCreationDate));
        if (queued.size() > jobsLimit) {
            queued = queued.subList(0, jobsLimit);
        }
        jobsLimit -= queued.size();

        List<Job> pending = new ArrayList<>(jobsLimit);
        for (Study study : studies) {
            if (jobsLimit == 0) {
                break;
            }
            List<Job> results = getJobDBAdaptor(organizationId).get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.PENDING),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, jobsLimit)
                            .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key()),
                    userId).getResults();
            pending.addAll(results);
        }
        pending.sort(Comparator.comparing(Job::getCreationDate));
        if (pending.size() > jobsLimit) {
            pending = pending.subList(0, jobsLimit);
        }
        jobsLimit -= pending.size();

        List<Job> finishedJobs = new ArrayList<>(jobsLimit);
        for (Study study : studies) {
            if (jobsLimit == 0) {
                break;
            }
            List<Job> results = getJobDBAdaptor(organizationId).get(
                    study.getUid(),
                    new Query(baseQuery)
                            .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.DONE + ","
                                    + Enums.ExecutionStatus.ERROR + ","
                                    + Enums.ExecutionStatus.ABORTED),
                    new QueryOptions(queryOptions)
                            .append(QueryOptions.LIMIT, jobsLimit)
                            .append(QueryOptions.SORT, "execution.end")
                            .append(QueryOptions.ORDER, QueryOptions.DESCENDING), // Get last n elements,
                    userId).getResults();
            Collections.reverse(results); // Reverse elements
            finishedJobs.addAll(results);
        }
        finishedJobs.sort(Comparator.comparing((Job j) -> j.getExecution() == null || j.getExecution().getStart() == null
                ? new Date()
                : j.getExecution().getStart()).reversed());
        if (finishedJobs.size() > jobsLimit) {
            finishedJobs = finishedJobs.subList(0, jobsLimit);
        }

        List<Job> allJobs = new ArrayList<>(finishedJobs.size() + running.size() + pending.size() + queued.size());
        allJobs.addAll(running);
        allJobs.addAll(queued);
        allJobs.addAll(pending);
        allJobs.addAll(finishedJobs);

        JobTopStats stats = new JobTopStats();
        for (Study study : studies) {
            OpenCGAResult result = getJobDBAdaptor(organizationId).groupBy(new Query(baseQuery)
                            .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()),
                    Collections.singletonList(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key()),
                    new QueryOptions(QueryOptions.COUNT, true),
                    userId);
            for (Object o : result.getResults()) {
                String status = ((Map) ((Map) o).get("_id")).get(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key()).toString();
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
        JobTop top = new JobTop(Date.from(Instant.now()), stats, allJobs);
        return new OpenCGAResult<>(((int) stopWatch.getTime()), null, 1, Collections.singletonList(top), 1);
    }

    @Override
    public OpenCGAResult rank(String studyId, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(token, "token");

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);
        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_JOBS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogJobDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getJobDBAdaptor(organizationId).rank(query, field, numResults, asc);
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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = getJobDBAdaptor(organizationId).groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<JobPermissions>> getAcls(String studyId, List<String> jobList, String member, boolean ignoreException,
                                                               String token) throws CatalogException {
        return getAcls(studyId, jobList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(),
                ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<JobPermissions>> getAcls(String studyId, List<String> jobList, List<String> members,
                                                               boolean ignoreException, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobList", jobList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<AclEntryList<JobPermissions>> jobAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<Job> queryResult = internalGet(organizationId, study.getUid(), jobList, INCLUDE_JOB_IDS, userId,
                    ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> jobUids = queryResult.getResults().stream().map(Job::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                jobAcls = authorizationManager.getAcl(organizationId, study.getUid(), jobUids, members, Enums.Resource.JOB,
                        JobPermissions.class, userId);
            } else {
                jobAcls = authorizationManager.getAcl(organizationId, study.getUid(), jobUids, Enums.Resource.JOB, JobPermissions.class,
                        userId);
            }

            // Include non-existing jobs to the result list
            List<AclEntryList<JobPermissions>> resultList = new ArrayList<>(jobList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String jobId : jobList) {
                if (!missingMap.containsKey(jobId)) {
                    Job job = queryResult.getResults().get(counter);
                    resultList.add(jobAcls.getResults().get(counter));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, job.getId(),
                            job.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, jobId, missingMap.get(jobId).getErrorMsg()));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, jobId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", missingMap.get(jobId).getErrorMsg())),
                            new ObjectMap());
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                jobAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            jobAcls.setResults(resultList);
            jobAcls.setEvents(eventList);
        } catch (Exception e) {
            for (String jobId : jobList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.JOB, jobId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e),
                        new ObjectMap());
            }
            if (!ignoreException) {
                throw e;
            } else {
                for (String jobId : jobList) {
                    Event event = new Event(Event.Type.ERROR, jobId, e.getMessage());
                    jobAcls.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                }
            }
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }

        return jobAcls;
    }

    public OpenCGAResult<AclEntryList<JobPermissions>> updateAcl(String studyId, List<String> jobStrList, String memberList,
                                                                 AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyFqn, StudyManager.INCLUDE_CONFIGURATION, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobStrList", jobStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            auditManager.initAuditBatch(operationId);

            if (jobStrList == null || jobStrList.isEmpty()) {
                throw new CatalogException("Missing job parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, JobPermissions::valueOf);
            }

            List<Job> jobList = internalGet(organizationId, study.getUid(), jobStrList, INCLUDE_JOB_IDS, userId, false).getResults();

            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(organizationId, study.getUid(), members);
            if (study.getInternal().isFederated()) {
                try {
                    checkIsNotAFederatedUser(organizationId, members);
                } catch (CatalogException e) {
                    throw new CatalogException("Cannot provide access to federated users to a federated study.", e);
                }
            }

            List<Long> jobUids = jobList.stream().map(Job::getUid).collect(Collectors.toList());
            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(jobUids, permissions,
                    Enums.Resource.JOB);

            switch (action) {
                case SET:
                    authorizationManager.setAcls(organizationId, study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    authorizationManager.addAcls(organizationId, study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(organizationId, members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    authorizationManager.removeAcls(organizationId, members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }
            OpenCGAResult<AclEntryList<JobPermissions>> queryResultList = authorizationManager.getAcls(organizationId, study.getUid(),
                    jobUids, members, Enums.Resource.JOB, JobPermissions.class);
            for (int i = 0; i < queryResultList.getResults().size(); i++) {
                queryResultList.getResults().get(i).setId(jobList.get(i).getId());
            }
            for (Job job : jobList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.JOB, job.getId(),
                        job.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (Exception e) {
            if (jobStrList != null) {
                for (String jobId : jobStrList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.JOB, jobId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e),
                            new ObjectMap());
                }
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }
    }

}
