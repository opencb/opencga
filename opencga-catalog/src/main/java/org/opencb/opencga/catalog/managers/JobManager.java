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
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.core.models.common.Enums.Resource.JOB;

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
    Enums.Resource getResource() {
        return JOB;
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
//        OpenCGAResult<Job> jobDataResult = jobDBAdaptor.get(studyUid, queryCopy, options, user);
//        if (jobDataResult.getNumResults() == 0) {
//            jobDataResult = jobDBAdaptor.get(queryCopy, options);
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
    InternalGetDataResult<Job> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options, String user,
                                           boolean ignoreException) throws CatalogException {
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
            OpenCGAResult<Job> jobDataResult = jobDBAdaptor.get(queryCopy, options);
            for (Job job : jobDataResult.getResults()) {
                // Check view permissions
                authorizationManager.checkJobPermission(job.getStudyUid(), job.getUid(), user, JobPermissions.VIEW);
            }
            return keepOriginalOrder(uniqueList, jobStringFunction, jobDataResult, ignoreException, false);
        }

        queryCopy.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
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

    private OpenCGAResult<Job> getJob(long studyUid, String jobUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(JobDBAdaptor.QueryParams.UUID.key(), jobUuid);
        return jobDBAdaptor.get(query, options);
    }

    public Long getStudyId(long jobId) throws CatalogException {
        return jobDBAdaptor.getStudyId(jobId);
    }

    public Study getStudy(Job job, String token) throws CatalogException {
        ParamUtils.checkObj(job, "job");
        ParamUtils.checkObj(token, "token");

        if (job.getStudyUid() <= 0) {
            throw new CatalogException("Missing study uid field in job");
        }

        String user = catalogManager.getUserManager().getUserId(token);

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
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobId", jobId)
                .append("token", token);
        return run(auditParams, Enums.Action.VISIT, JOB, studyId, token, null, (study, userId, rp, qOptions) -> {
            rp.setId(jobId);
            JobUpdateParams updateParams = new JobUpdateParams().setVisited(true);
            Job job = internalGet(study.getUid(), jobId, INCLUDE_JOB_IDS, userId).first();
            rp.setId(job.getId());
            rp.setUuid(job.getUuid());
            return update(study, job, updateParams, QueryOptions.empty(), userId);
        });
    }

    @Override
    public OpenCGAResult<Job> create(String studyStr, Job job, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("job", job)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.CREATE, JOB, studyStr, token, options, (study, userId, rp, qOptions) -> {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.WRITE_JOBS);

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
            job.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.JOB));
            rp.setId(job.getId());
            rp.setUuid(job.getUuid());

            if (!Arrays.asList(Enums.ExecutionStatus.ABORTED, Enums.ExecutionStatus.DONE, Enums.ExecutionStatus.UNREGISTERED,
                    Enums.ExecutionStatus.ERROR).contains(job.getInternal().getStatus().getId())) {
                throw new CatalogException("Cannot create a job in a status different from one of the final ones.");
            }

            if (CollectionUtils.isNotEmpty(job.getInput())) {
                List<File> inputFiles = new ArrayList<>(job.getInput().size());
                for (File file : job.getInput()) {
                    inputFiles.add(getFile(study.getUid(), file.getPath(), userId));
                }
                job.setInput(inputFiles);
            }
            if (CollectionUtils.isNotEmpty(job.getOutput())) {
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

            OpenCGAResult<Job> insert = jobDBAdaptor.insert(study.getUid(), job, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created job
                OpenCGAResult<Job> queryResult = getJob(study.getUid(), job.getUuid(), options);
                insert.setResults(queryResult.getResults());
            }

            return insert;
        });
    }

    private void autoCompleteNewJob(Study study, Job job, String token) throws CatalogException {
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

        if (job.getDependsOn() != null && !job.getDependsOn().isEmpty()) {
            boolean uuidProvided = job.getDependsOn().stream().map(Job::getId).anyMatch(UuidUtils::isOpenCgaUuid);

            try {
                // If uuid is provided, we will remove the study uid from the query so it can be searched across any study
                InternalGetDataResult<Job> dependsOnResult;
                if (uuidProvided) {
                    dependsOnResult = internalGet(0, job.getDependsOn().stream().map(Job::getId).collect(Collectors.toList()), null,
                            INCLUDE_JOB_IDS, job.getUserId(), false);
                } else {
                    dependsOnResult = internalGet(study.getUid(), job.getDependsOn().stream().map(Job::getId).collect(Collectors.toList()),
                            null, INCLUDE_JOB_IDS, job.getUserId(), false);
                }
                job.setDependsOn(dependsOnResult.getResults());
            } catch (CatalogException e) {
                throw new CatalogException("Unable to find the jobs this job depends on. " + e.getMessage(), e);
            }

            job.setInput(Collections.emptyList());
        } else {
            // We only check input files if the job does not depend on other job that might be creating the necessary file.

            List<File> inputFiles = getJobInputFilesFromParams(study.getFqn(), job, token);
            job.setInput(inputFiles);
        }

        job.setAttributes(ParamUtils.defaultObject(job.getAttributes(), HashMap::new));
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

    public OpenCGAResult<Job> retry(String studyStr, JobRetryParams jobRetry, Enums.Priority priority,
                                    String jobId, String jobDescription, List<String> jobDependsOn, List<String> jobTags, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("jobRetry", jobRetry)
                .append("priority", priority)
                .append("jobId", jobId)
                .append("jobDescription", jobDescription)
                .append("jobDependsOn", jobDependsOn)
                .append("jobTags", jobTags)
                .append("token", token);

        return run(auditParams, Enums.Action.CREATE, JOB, studyStr, token, null, (study, userId, rp, qOptions) -> {
            rp.setId(jobId);
            Job job = get(studyStr, jobRetry.getJob(), new QueryOptions(), token).first();
            rp.setId(job.getId());
            rp.setUuid(job.getUuid());
            if (jobRetry.isForce()
                    || job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ERROR)
                    || job.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ABORTED)) {
                Map<String, Object> params = new ObjectMap(job.getParams());
                if (jobRetry.getParams() != null) {
                    params.putAll(jobRetry.getParams());
                }
                HashMap<String, Object> attributes = new HashMap<>();
                attributes.put("retry_from", jobRetry.getJob());
                String finalDescription = StringUtils.isNotEmpty(jobDescription)
                        ? jobDescription
                        : "Retry from job '" + jobRetry.getJob() + "'";
                return submit(studyStr, job.getTool().getId(), priority, params, jobId, finalDescription, jobDependsOn, jobTags, attributes,
                        token);
            } else {
                throw new CatalogException("Unable to retry job with status " + job.getInternal().getStatus().getId());
            }
        });
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String token)
            throws CatalogException {
        return submit(studyStr, toolId, priority, params, null, null, null, null, token);
    }

    public OpenCGAResult<Job> submitProject(String projectStr, String toolId, Enums.Priority priority, Map<String, Object> params,
                                            String jobId, String jobDescription, List<String> jobDependsOn, List<String> jobTags,
                                            String token) throws CatalogException {
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
        return submit(studies.get(0), toolId, priority, params, jobId, jobDescription, jobDependsOn, jobTags, token);
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String jobId,
                                     String jobDescription, List<String> jobDependsOn, List<String> jobTags, String token)
            throws CatalogException {
        return submit(studyStr, toolId, priority, params, jobId, jobDescription, jobDependsOn, jobTags, null, token);
    }

    public OpenCGAResult<Job> submit(String studyStr, String toolId, Enums.Priority priority, Map<String, Object> params, String jobId,
                                     String jobDescription, List<String> jobDependsOn, List<String> jobTags,
                                     Map<String, Object> attributes, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("toolId", toolId)
                .append("priority", priority)
                .append("params", params)
                .append("jobId", jobId)
                .append("jobDescription", jobDescription)
                .append("jobDependsOn", jobDependsOn)
                .append("jobTags", jobTags)
                .append("token", token);

        Job job = new Job();
        job.setId(jobId);
        job.setDescription(jobDescription);
        job.setTool(new ToolInfo().setId(toolId));
        job.setTags(jobTags);
        job.setParams(params);
        job.setPriority(priority);
        job.setDependsOn(jobDependsOn != null
                ? jobDependsOn.stream().map(j -> new Job().setId(j)).collect(Collectors.toList())
                : Collections.emptyList());
        job.setAttributes(attributes);

        try {
            return run(auditParams, Enums.Action.CREATE, JOB, studyStr, token, null, (study, userId, rp, qOptions) -> {
                job.setStudyUid(study.getUid());
                job.setStudy(new JobStudyParam(study.getFqn()));
                job.setUserId(userId);
                authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.EXECUTE_JOBS);

                autoCompleteNewJob(study, job, token);

                // Check params
                ParamUtils.checkObj(params, "params");
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    if (entry.getValue() == null) {
                        throw new CatalogException("Found '" + entry.getKey() + "' param with null value");
                    }
                }

                jobDBAdaptor.insert(study.getUid(), job, new QueryOptions());
                return jobDBAdaptor.get(job.getUid(), new QueryOptions());
            });
        } catch (CatalogException e) {
            job.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED));
            job.getInternal().getStatus().setDescription(e.toString());
            jobDBAdaptor.insert(job.getStudyUid(), job, new QueryOptions());
            throw e;
        }
    }

    public OpenCGAResult count(Query query, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("token", token);

        return run(auditParams, Enums.Action.COUNT, JOB, null, token, null, (study, userId, rp, qOptions) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            authorizationManager.isInstallationAdministrator(userId);
            return jobDBAdaptor.count(finalQuery);
        });
    }

    public DBIterator<Job> iterator(Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.ITERATE, JOB, null, token, options, (study, userId, rp, queryOptions) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            authorizationManager.isInstallationAdministrator(userId);
            return jobDBAdaptor.iterator(finalQuery, queryOptions);
        });
    }

    public OpenCGAResult<Job> get(long jobId, QueryOptions options, String token) throws CatalogException {
        return get(null, String.valueOf(jobId), options, token);
    }

//    public OpenCGAResult<Job> get(List<String> jobIds, QueryOptions options, boolean ignoreException, String sessionId)
//            throws CatalogException {
//        return get(null, jobIds, options, ignoreException, sessionId);
//    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.JOB_TOOL_ID_PARAM, JobDBAdaptor.QueryParams.TOOL_ID.key());
        changeQueryId(query, ParamConstants.JOB_TOOL_TYPE_PARAM, JobDBAdaptor.QueryParams.TOOL_TYPE.key());
        changeQueryId(query, ParamConstants.JOB_INTERNAL_STATUS_PARAM, JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());
        changeQueryId(query, ParamConstants.JOB_STATUS_PARAM, JobDBAdaptor.QueryParams.STATUS_ID.key());

        if (query.containsKey(ParamConstants.JOB_INPUT_FILES_PARAM)) {
            List<File> inputFiles = catalogManager.getFileManager().internalGet(study.getUid(),
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
            List<File> inputFiles = catalogManager.getFileManager().internalGet(study.getUid(),
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
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.SEARCH, JOB, studyId, token, options, (study, userId, rp, qOptions) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(study, finalQuery, userId);
            finalQuery.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return jobDBAdaptor.get(study.getUid(), finalQuery, qOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", field)
                .append("query", query)
                .append("token", token);

        return run(auditParams, Enums.Action.DISTINCT, JOB, studyId, token, null, (study, userId, rp, queryOptions) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return jobDBAdaptor.distinct(study.getUid(), field, finalQuery, userId);
        });
    }

    @Override
    public DBIterator<Job> iterator(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.ITERATE, JOB, studyId, token, options, (study, userId, rp, qOptions) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(study, finalQuery, userId);
            finalQuery.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return jobDBAdaptor.iterator(study.getUid(), finalQuery, qOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<Job> count(String studyId, Query query, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("token", token);

        return run(auditParams, Enums.Action.COUNT, JOB, studyId, token, null, (study, userId, rp, qOptions) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return jobDBAdaptor.count(finalQuery, userId);
        });
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> jobIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, jobIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> jobIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("jobIds", jobIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        return runBatch(auditParams, Enums.Action.DELETE, JOB, studyStr, token, null, (study, userId, qOptions, operationUuid) -> {

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            boolean checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

            OpenCGAResult<Job> result = OpenCGAResult.empty(Job.class);
            for (String id : jobIds) {
                try {
                    run(auditParams, Enums.Action.DELETE, JOB, operationUuid, study, userId, null, (s, u, rp, qo) -> {
                        OpenCGAResult<Job> internalResult = internalGet(study.getUid(), id, INCLUDE_JOB_IDS, userId);
                        if (internalResult.getNumResults() == 0) {
                            throw new CatalogException("Job '" + id + "' not found");
                        }

                        Job job = internalResult.first();
                        // We set the proper values for the audit
                        rp.setId(job.getId());
                        rp.setUuid(job.getUuid());

                        if (checkPermissions) {
                            authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobPermissions.DELETE);
                        }

                        // Check if the job can be deleted
                        checkJobCanBeDeleted(job);
                        result.append(jobDBAdaptor.delete(job));
                        return null;
                    });
                } catch (CatalogException e) {
                    Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                    result.getEvents().add(event);
                    result.setNumErrors(result.getNumErrors() + 1);

                    logger.error("Cannot delete job {}: {}", id, e.getMessage(), e);
                }
            }

            return endResult(result, ignoreException);
        });
    }

    @Override
    public OpenCGAResult delete(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyId, query, options, false, token);
    }

    public OpenCGAResult delete(String studyId, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("query", query)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        return runBatch(auditParams, Enums.Action.DELETE, JOB, studyId, token, null, (study, userId, qOptions, operationUuid) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            OpenCGAResult<Job> result = OpenCGAResult.empty(Job.class);

            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            boolean checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

            // We try to get an iterator containing all the jobs to be deleted
            try (DBIterator<Job> iterator = jobDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId)) {
                while (iterator.hasNext()) {
                    Job job = iterator.next();
                    try {
                        run(auditParams, Enums.Action.DELETE, JOB, operationUuid, study, userId, null, (s, u, rp, qo) -> {
                            rp.setId(job.getId());
                            rp.setUuid(job.getUuid());
                            if (checkPermissions) {
                                authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobPermissions.DELETE);
                            }

                            // Check if the job can be deleted
                            checkJobCanBeDeleted(job);
                            result.append(jobDBAdaptor.delete(job));
                            return null;
                        });
                    } catch (CatalogException e) {
                        String errorMsg = "Cannot delete job " + job.getId() + ": " + e.getMessage();

                        Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                        result.getEvents().add(event);
                        result.setNumErrors(result.getNumErrors() + 1);

                        logger.error(errorMsg, e);
                    }
                }
            }

            return endResult(result, ignoreException);
        });
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
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobId", jobId)
                .append("offset", offset)
                .append("lines", lines)
                .append("type", type)
                .append("tail", tail)
                .append("token", token);

        StopWatch stopWatch = StopWatch.createStarted();
        return run(auditParams, Enums.Action.VIEW_LOG, JOB, studyId, token, null, (study, userId, rp, qOptions) -> {
            rp.setId(jobId);
            String finalType = StringUtils.isNotEmpty(type) ? type : "stderr";
            if (!"stderr".equalsIgnoreCase(finalType) && !"stdout".equalsIgnoreCase(finalType)) {
                throw new CatalogException("Incorrect log type. It must be 'stdout' or 'stderr'");
            }

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.UUID.key(),
                            JobDBAdaptor.QueryParams.INTERNAL_STATUS.key(), JobDBAdaptor.QueryParams.STDOUT.key(),
                            JobDBAdaptor.QueryParams.OUT_DIR.key()));
            Job job = internalGet(study.getUid(), jobId, options, userId).first();
            rp.setId(job.getId());
            rp.setUuid(job.getUuid());

            Path logFile;
            if ("stderr".equalsIgnoreCase(finalType)) {
                if (job.getStderr() != null && job.getStderr().getUri() != null) {
                    logFile = Paths.get(job.getStderr().getUri());
                } else {
                    // The log file hasn't yet been registered
                    if (!Arrays.asList(Enums.ExecutionStatus.PENDING, Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.ABORTED)
                            .contains(job.getInternal().getStatus().getId()) && job.getOutDir() != null) {
                        logFile = Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".err");
                    } else {
                        throw CatalogAuthorizationException.deny(userId, "see stderr log file of job '" + jobId + "'");
                    }
                }
            } else {
                if (job.getStdout() != null && job.getStdout().getUri() != null) {
                    logFile = Paths.get(job.getStdout().getUri());
                } else {
                    // The log file hasn't yet been registered
                    if (!Arrays.asList(Enums.ExecutionStatus.PENDING, Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.ABORTED)
                            .contains(job.getInternal().getStatus().getId()) && job.getOutDir() != null) {
                        logFile = Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log");
                    } else {
                        throw CatalogAuthorizationException.deny(userId, "see stdout log file of job '" + jobId + "'");
                    }
                }
            }

            IOManager ioManager;
            try {
                ioManager = ioManagerFactory.get(logFile.toUri());
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(logFile.toUri(), e);
            }
            FileContent fileContent;
            if (tail) {
                fileContent = ioManager.tail(logFile, lines);
            } else {
                fileContent = ioManager.head(logFile, offset, lines);
            }

            return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), 1,
                    Collections.singletonList(fileContent), 1);
        });
    }

    public OpenCGAResult<Job> update(String studyStr, Query query, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyStr, Query query, JobUpdateParams updateParams, boolean ignoreException,
                                     QueryOptions options, String token) throws CatalogException {
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

        return runBatch(auditParams, Enums.Action.UPDATE, JOB, studyStr, token, options, (study, userId, qOptions, operationUuid) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            try (DBIterator<Job> iterator = jobDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId)) {
                OpenCGAResult<Job> result = OpenCGAResult.empty(Job.class);
                while (iterator.hasNext()) {
                    Job job = iterator.next();
                    try {
                        run(auditParams, Enums.Action.UPDATE, JOB, operationUuid, study, userId, null, (s, u, rp, qo) -> {
                            rp.setId(job.getId());
                            rp.setUuid(job.getUuid());
                            OpenCGAResult<Job> updateResult = update(study, job, updateParams, options, userId);
                            result.append(updateResult);
                            return null;
                        });
                    } catch (CatalogException e) {
                        Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                        result.getEvents().add(event);
                        result.setNumErrors(result.getNumErrors() + 1);

                        logger.error("Could not update job {}: {}", job.getId(), e.getMessage(), e);
                    }
                }

                return endResult(result, ignoreException);
            }
        });
    }

    /**
     * Update Job from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param jobIds       List of Job ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
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

        return runBatch(auditParams, Enums.Action.UPDATE, JOB, studyStr, token, options, (study, userId, qOptions, operationUuid) -> {
            OpenCGAResult<Job> result = OpenCGAResult.empty(Job.class);
            for (String id : jobIds) {
                try {
                    run(auditParams, Enums.Action.UPDATE, JOB, operationUuid, study, userId, null, (s, u, rp, qo) -> {
                        OpenCGAResult<Job> internalResult = internalGet(study.getUid(), id, INCLUDE_JOB_IDS, userId);
                        if (internalResult.getNumResults() == 0) {
                            throw new CatalogException("Job '" + id + "' not found");
                        }
                        Job job = internalResult.first();

                        // We set the proper values for the audit
                        rp.setId(job.getId());
                        rp.setUuid(job.getUuid());

                        OpenCGAResult<Job> updateResult = update(study, job, updateParams, options, userId);
                        result.append(updateResult);
                        return null;
                    });
                } catch (CatalogException e) {
                    Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                    result.getEvents().add(event);
                    result.setNumErrors(result.getNumErrors() + 1);

                    logger.error("Could not update job {}: {}", id, e.getMessage(), e);
                }
            }

            return endResult(result, ignoreException);
        });
    }

    public OpenCGAResult<Job> update(String studyStr, String jobId, JobUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
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

        return run(auditParams, Enums.Action.UPDATE, JOB, studyStr, token, options, (study, userId, rp, qOptions) -> {
            rp.setId(jobId);
            OpenCGAResult<Job> internalResult = internalGet(study.getUid(), jobId, INCLUDE_JOB_IDS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Job '" + jobId + "' not found");
            }
            Job job = internalResult.first();

            // We set the proper values for the audit
            rp.setId(job.getId());
            rp.setUuid(job.getUuid());

            return update(study, job, updateParams, qOptions, userId);
        });
    }

    private OpenCGAResult<Job> update(Study study, Job job, JobUpdateParams updateParams, QueryOptions options, String userId)
            throws CatalogException {
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
        authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

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

        OpenCGAResult<Job> update = jobDBAdaptor.update(job.getUid(), updateMap, options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated job
            OpenCGAResult<Job> result = jobDBAdaptor.get(study.getUid(), new Query(JobDBAdaptor.QueryParams.UID.key(), job.getUid()),
                    options, userId);
            update.setResults(result.getResults());
        }
        return update;
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
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("query", query)
                .append("parameters", parameters)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        return runBatch(auditParams, Enums.Action.UPDATE, JOB, studyId, token, options, (study, userId, qOptions, operationUuid) -> {
            ParamUtils.checkObj(parameters, "parameters");

            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(study, finalQuery, token);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            try (DBIterator<Job> iterator = jobDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_JOB_IDS, userId)) {
                OpenCGAResult<Job> result = OpenCGAResult.empty(Job.class);
                while (iterator.hasNext()) {
                    Job job = iterator.next();
                    try {
                        run(auditParams, Enums.Action.UPDATE, JOB, operationUuid, study, userId, qOptions, (s, u, rp, qo) -> {
                            rp.setId(job.getId());
                            rp.setUuid(job.getUuid());

                            authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

                            OpenCGAResult<?> updateResult = jobDBAdaptor.update(job.getUid(), parameters, qo);
                            result.append(updateResult);
                            return null;
                        });
                    } catch (CatalogException e) {
                        Event event = new Event(Event.Type.ERROR, job.getId(), e.getMessage());
                        result.getEvents().add(event);
                        result.setNumErrors(result.getNumErrors() + 1);

                        logger.error("Cannot update job {}: {}", job.getId(), e.getMessage());
                    }
                }

                return endResult(result, ignoreException);
            }
        });
    }

    public OpenCGAResult<Job> update(String studyId, String jobId, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("jobId", jobId)
                .append("parameters", parameters)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.UPDATE, JOB, studyId, token, options, (study, userId, rp, qOptions) -> {
            rp.setId(jobId);
            ParamUtils.checkObj(parameters, "parameters");

            OpenCGAResult<Job> internalResult = internalGet(study.getUid(), jobId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Job '" + jobId + "' not found");
            }
            Job job = internalResult.first();

            // We set the proper values for the audit
            rp.setId(job.getId());
            rp.setUuid(job.getUuid());

            authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

            return jobDBAdaptor.update(job.getUid(), parameters, options);
        });
    }

    public OpenCGAResult<Job> update(String studyId, List<String> jobIds, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        return update(studyId, jobIds, parameters, false, options, token);
    }

    public OpenCGAResult<Job> update(String studyId, List<String> jobIds, ObjectMap parameters, boolean ignoreException,
                                     QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("jobIds", jobIds)
                .append("parameters", parameters)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        return runBatch(auditParams, Enums.Action.UPDATE, JOB, studyId, token, options, (study, userId, qOptions, operationUuid) -> {
            ParamUtils.checkObj(parameters, "parameters");

            OpenCGAResult<Job> result = OpenCGAResult.empty(Job.class);
            for (String id : jobIds) {
                try {
                    run(auditParams, Enums.Action.UPDATE, JOB, operationUuid, study, userId, qOptions, (s, u, rp, qo) -> {
                        OpenCGAResult<Job> internalResult = internalGet(study.getUid(), id, QueryOptions.empty(), userId);
                        if (internalResult.getNumResults() == 0) {
                            throw new CatalogException("Job '" + id + "' not found");
                        }
                        Job job = internalResult.first();

                        // We set the proper values for the audit
                        rp.setId(job.getId());
                        rp.setUuid(job.getUuid());

                        authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobPermissions.WRITE);

                        OpenCGAResult<?> updateResult = jobDBAdaptor.update(job.getUid(), parameters, options);
                        result.append(updateResult);
                        return null;
                    });
                } catch (CatalogException e) {
                    Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                    result.getEvents().add(event);
                    result.setNumErrors(result.getNumErrors() + 1);

                    logger.error("Cannot update job {}: {}", id, e.getMessage());
                }
            }

            return endResult(result, ignoreException);
        });
    }

    OpenCGAResult<JobTop> top(Query baseQuery, int limit, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        List<String> studies = studyManager.search(new Query(StudyDBAdaptor.QueryParams.OWNER.key(), userId),
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UUID.key()), token).getResults()
                .stream()
                .map(Study::getUuid)
                .collect(Collectors.toList());
        return top(studies, baseQuery, limit, userId);
    }

    public OpenCGAResult<JobTop> top(String studyStr, Query baseQuery, int limit, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("baseQuery", baseQuery)
                .append("limit", limit)
                .append("token", token);

        return run(auditParams, Enums.Action.TOP, JOB, null, token, null, (study, userId, rp, qOptions) -> {
            if (StringUtils.isEmpty(studyStr)) {
                return top(baseQuery, limit, token);
            } else {
                return top(Collections.singletonList(studyStr), baseQuery, limit, userId);
            }
        });
    }

    private OpenCGAResult<JobTop> top(List<String> studiesStr, Query baseQuery, int limit, String userId) throws CatalogException {
        fixQueryObject(null, baseQuery, userId);
        List<Study> studies = new ArrayList<>(studiesStr.size());
        for (String studyStr : studiesStr) {
            Study study = studyManager.resolveId(studyStr, userId);
            authorizationManager.checkCanViewStudy(study.getUid(), userId);
            studies.add(study);
        }

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
            List<Job> results = jobDBAdaptor.get(
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
            List<Job> results = jobDBAdaptor.get(
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
            List<Job> results = jobDBAdaptor.get(
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
            List<Job> results = jobDBAdaptor.get(
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
            OpenCGAResult result = jobDBAdaptor.groupBy(new Query(baseQuery)
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
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("field", field)
                .append("numResults", numResults)
                .append("asc", asc)
                .append("token", token);

        return run(auditParams, Enums.Action.RANK, JOB, studyId, token, null, (study, userId, rp, qOptions) -> {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.VIEW_JOBS);

            ParamUtils.checkObj(field, "field");
            ParamUtils.checkObj(token, "sessionId");
            Query finalQuery = query != null ? new Query(query) : new Query();

            // TODO: In next release, we will have to check the count parameter from the queryOptions object.
            boolean count = true;
            //query.append(CatalogJobDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<?> queryResult = null;
            if (count) {
                // We do not need to check for permissions when we show the count of files
                queryResult = jobDBAdaptor.rank(finalQuery, field, numResults, asc);
            }

            return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
        });
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyId, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("fields", fields)
                .append("options", options)
                .append("token", token);
        return run(auditParams, Enums.Action.GROUP_BY, JOB, studyId, token, options, (study, userId, rp, qOptions) -> {
            if (CollectionUtils.isEmpty(fields)) {
                throw new CatalogException("Empty fields parameter.");
            }

            Query finalQuery = query != null ? new Query(query) : new Query();
            // Add study id to the query
            finalQuery.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return jobDBAdaptor.groupBy(finalQuery, fields, qOptions, userId);
        });
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<JobPermissions>> getAcls(String studyId, List<String> jobList, String member, boolean ignoreException,
                                                               String token) throws CatalogException {
        return getAcls(studyId, jobList, Collections.singletonList(member), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<JobPermissions>> getAcls(String studyId, List<String> jobList, List<String> members,
                                                               boolean ignoreException, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobList", jobList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        return runBatch(auditParams, Enums.Action.FETCH_ACLS, JOB, studyId, token, null, (study, userId, qOptions, operationUuid) -> {
            OpenCGAResult<AclEntryList<JobPermissions>> jobAcls;
            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            InternalGetDataResult<Job> queryResult = internalGet(study.getUid(), jobList, INCLUDE_JOB_IDS, userId, ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> jobUids = queryResult.getResults().stream().map(Job::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                jobAcls = authorizationManager.getAcl(userId, study.getUid(), jobUids, members, JOB, JobPermissions.class);
            } else {
                jobAcls = authorizationManager.getAcl(userId, study.getUid(), jobUids, JOB, JobPermissions.class);
            }

            // Include non-existing jobs to the result list
            List<AclEntryList<JobPermissions>> resultList = new ArrayList<>(jobList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String jobId : jobList) {
                if (!missingMap.containsKey(jobId)) {
                    Job job = queryResult.getResults().get(counter);
                    run(auditParams, Enums.Action.FETCH_ACLS, JOB, operationUuid, study, userId, qOptions, (s, u, rp, qo) -> {
                        rp.setId(job.getId());
                        rp.setUuid(job.getUuid());
                        return null;
                    });
                    resultList.add(jobAcls.getResults().get(counter));
                    counter++;
                } else {
                    if (!ignoreException) {
                        throw new CatalogException(missingMap.get(jobId).getErrorMsg());
                    }
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, jobId, missingMap.get(jobId).getErrorMsg()));
                }
            }
            jobAcls.setResults(resultList);
            jobAcls.setEvents(eventList);

            return jobAcls;
        });
    }

    public OpenCGAResult<AclEntryList<JobPermissions>> updateAcl(String studyId, List<String> jobStrList, String memberList,
                                                                 AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("jobStrList", jobStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);

        return runBatch(auditParams, Enums.Action.UPDATE_ACLS, JOB, studyId, token, null, (study, userId, qOptions, operationUuid) -> {
            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);

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

            List<Job> jobList = internalGet(study.getUid(), jobStrList, INCLUDE_JOB_IDS, userId, false).getResults();

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            List<Long> jobUids = jobList.stream().map(Job::getUid).collect(Collectors.toList());
            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(jobUids, permissions, JOB);

            switch (action) {
                case SET:
                    authorizationManager.setAcls(study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    authorizationManager.addAcls(study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }
            for (Job job : jobList) {
                run(auditParams, Enums.Action.UPDATE_ACLS, JOB, operationUuid, study, userId, null, (s, u, rp, qo) -> {
                    rp.setId(job.getId());
                    rp.setUuid(job.getUuid());
                    return null;
                });
            }

            return authorizationManager.getAcls(study.getUid(), jobUids, members, JOB, JobPermissions.class);
        });
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("options", options)
                .append("defaultStats", defaultStats)
                .append("token", token);

        return run(auditParams, Enums.Action.FACET, JOB, studyId, token, options,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key()),
                (study, userId, rp, qOptions) -> {
                    Query finalQuery = query != null ? new Query(query) : new Query();
                    if (defaultStats || StringUtils.isEmpty(qOptions.getString(QueryOptions.FACET))) {
                        String facet = qOptions.getString(QueryOptions.FACET);
                        qOptions.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
                    }
                    AnnotationUtils.fixQueryAnnotationSearch(study, userId, finalQuery, authorizationManager);

                    try (CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager)) {
                        return catalogSolrManager.facetedQuery(study, CatalogSolrManager.JOB_SOLR_COLLECTION, finalQuery, qOptions, userId);
                    }
                });
    }

}
