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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
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
            Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.UUID.key()));

    JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
               DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
               Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<Job> internalGet(long studyUid, String entry, QueryOptions options, String user) throws CatalogException {
        Query query = new Query(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(JobDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(JobDBAdaptor.QueryParams.ID.key(), entry);
        }
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                JobDBAdaptor.QueryParams.UUID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.STUDY_UID.key(),
//                JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, options, user);
        if (jobQueryResult.getNumResults() == 0) {
            jobQueryResult = jobDBAdaptor.get(query, options);
            if (jobQueryResult.getNumResults() == 0) {
                throw new CatalogException("Job " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the job " + entry);
            }
        } else if (jobQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one job found based on " + entry);
        } else {
            return jobQueryResult;
        }
    }

    @Override
    InternalGetQueryResult<Job> internalGet(long studyUid, List<String> entryList, QueryOptions options, String user, boolean silent)
            throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing job entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query query = new Query(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

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
        query.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, options, user);
        if (silent || jobQueryResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, jobStringFunction, jobQueryResult, silent);
        }
        // Query without adding the user check
        QueryResult<Job> resultsNoCheck = jobDBAdaptor.get(query, queryOptions);

        if (resultsNoCheck.getNumResults() == jobQueryResult.getNumResults()) {
            throw CatalogException.notFound("jobs", getMissingFields(uniqueList, jobQueryResult.getResult(), jobStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the jobs.");
        }
    }

    public Long getStudyId(long jobId) throws CatalogException {
        return jobDBAdaptor.getStudyId(jobId);
    }

    public Study getStudy(Job job, String sessionId) throws CatalogException {
        ParamUtils.checkObj(job, "job");
        ParamUtils.checkObj(sessionId, "session id");

        if (job.getStudyUid() <= 0) {
            throw new CatalogException("Missing study uid field in file");
        }

        String user = catalogManager.getUserManager().getUserId(sessionId);

        Query query = new Query(StudyDBAdaptor.QueryParams.UID.key(), job.getStudyUid());
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, QueryOptions.empty(), user);
        if (studyQueryResult.getNumResults() == 1) {
            return studyQueryResult.first();
        } else {
            authorizationManager.checkCanViewStudy(job.getStudyUid(), user);
            throw new CatalogException("Incorrect study uid");
        }
    }


    public QueryResult<Job> visit(String studyStr, String jobId, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);
        Job job = internalGet(study.getUid(), jobId, INCLUDE_JOB_IDS, userId).first();
        authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.VIEW);
        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.VISITED.key(), true);
        return jobDBAdaptor.update(job.getUid(), params, QueryOptions.empty());
    }

    @Deprecated
    public QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor,
                                   Map<String, String> params, String commandLine, URI tmpOutDirUri, long outDirId,
                                   List<File> inputFiles, List<File> outputFiles, Map<String, Object> attributes,
                                   Map<String, Object> resourceManagerAttributes, Job.JobStatus status, long startTime,
                                   long endTime, QueryOptions options, String sessionId) throws CatalogException {
        Job job = new Job(-1, null, name, "", toolName, null, "", description, startTime, endTime, executor, "", commandLine, false, status,
                -1, new File().setUid(outDirId), inputFiles, outputFiles, Collections.emptyList(), params, -1, attributes,
                resourceManagerAttributes);
        return create(String.valueOf(studyId), job, options, sessionId);
    }

    public QueryResult<Job> create(String studyStr, String jobName, String description, String toolId, String execution, String outDir,
                                   Map<String, String> params, String sessionId) throws CatalogException {
        ParamUtils.checkObj(toolId, "toolId");
        if (StringUtils.isEmpty(jobName)) {
            jobName = toolId + "_" + TimeUtils.getTime();
        }
        ObjectMap attributes = new ObjectMap();
        attributes.putIfNotNull(Job.OPENCGA_OUTPUT_DIR, outDir);
        attributes.putIfNotNull(Job.OPENCGA_STUDY, studyStr);
        Job job = new Job(jobName, toolId, execution, Job.Type.ANALYSIS, description, params, attributes);

        return create(studyStr, job, QueryOptions.empty(), sessionId);
    }

    @Override
    public QueryResult<Job> create(String studyStr, Job job, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

        ParamUtils.checkObj(job, "Job");
        ParamUtils.checkParameter(job.getToolId(), "toolId");
        job.setId(ParamUtils.defaultString(job.getId(), job.getToolId() + "_" + TimeUtils.getTimeMillis()));
//        ParamUtils.checkParameter(job.getCommandLine(), "commandLine");
//        ParamUtils.checkObj(job.getOutDir(), "outDir");
        job.setDescription(ParamUtils.defaultString(job.getDescription(), ""));
        job.setCreationDate(ParamUtils.defaultString(job.getCreationDate(), TimeUtils.getTime()));
        job.setStatus(ParamUtils.defaultObject(job.getStatus(), new Job.JobStatus(Job.JobStatus.PREPARED)));
        job.setInput(ParamUtils.defaultObject(job.getInput(), Collections.emptyList()));
        job.setOutput(ParamUtils.defaultObject(job.getOutput(), Collections.emptyList()));
        job.setExecution(ParamUtils.defaultObject(job.getExecution(), ""));
        job.setParams(ParamUtils.defaultObject(job.getParams(), HashMap::new));
        job.setResourceManagerAttributes(ParamUtils.defaultObject(job.getResourceManagerAttributes(), HashMap::new));
        job.setAttributes(ParamUtils.defaultObject(job.getAttributes(), HashMap::new));
        job.setUserId(userId);
        job.setRelease(catalogManager.getStudyManager().getCurrentRelease(study, userId));
        job.setOutDir(job.getOutDir() != null && StringUtils.isNotEmpty(job.getOutDir().getPath()) ? job.getOutDir() : null);

        // FIXME check inputFiles? is a null conceptually valid?
//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        if (ListUtils.isNotEmpty(job.getInput())) {
            List<String> inputFileStringList = new ArrayList<>(job.getInput().size());
            for (File inputFile : job.getInput()) {
                inputFileStringList.add(StringUtils.isEmpty(inputFile.getPath()) ? inputFile.getName() : inputFile.getPath());
            }
            List<File> inputFileList = catalogManager.getFileManager().internalGet(study.getUid(), inputFileStringList,
                    QueryOptions.empty(), userId, false).getResult();
            job.setInput(inputFileList);
        }

        if (ListUtils.isNotEmpty(job.getOutput())) {
            List<String> outputFileStringList = new ArrayList<>(job.getOutput().size());
            for (File outputFile : job.getOutput()) {
                outputFileStringList.add(StringUtils.isEmpty(outputFile.getPath()) ? outputFile.getName() : outputFile.getPath());
            }
            List<File> outputFileList = catalogManager.getFileManager().internalGet(study.getUid(), outputFileStringList,
                    QueryOptions.empty(), userId, false).getResult();
            job.setOutput(outputFileList);
        }

        if (job.getOutDir() != null) {
            String fileName = StringUtils.isNotEmpty(job.getOutDir().getPath()) ? job.getOutDir().getPath() : job.getOutDir().getName();
            File file = catalogManager.getFileManager().internalGet(study.getUid(), fileName, QueryOptions.empty(), userId).first();

            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

            if (!file.getType().equals(File.Type.DIRECTORY)) {
                throw new CatalogException("Bad outDir type. Required type : " + File.Type.DIRECTORY);
            }

            job.setOutDir(file);
        }

        job.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.JOB));
        QueryResult<Job> queryResult = jobDBAdaptor.insert(job, study.getUid(), options);
        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getUid(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    public QueryResult<Job> get(long jobId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(jobId), options, sessionId);
    }

    public List<QueryResult<Job>> get(List<String> jobIds, QueryOptions options, boolean silent, String sessionId) throws CatalogException {
        return get(null, jobIds, options, silent, sessionId);
    }

    @Override
    public QueryResult<Job> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        fixQueryObject(study, query, userId);

        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, options, userId);

        if (jobQueryResult.getNumResults() == 0 && query.containsKey(JobDBAdaptor.QueryParams.UID.key())) {
            List<Long> idList = query.getAsLongList(JobDBAdaptor.QueryParams.UID.key());
            for (Long myId : idList) {
                authorizationManager.checkJobPermission(study.getUid(), myId, userId, JobAclEntry.JobPermissions.VIEW);
            }
        }

        return jobQueryResult;
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        if (query.containsKey("inputFiles")) {
            List<File> inputFiles = catalogManager.getFileManager().internalGet(study.getUid(), query.getAsStringList("inputFiles"),
                    FileManager.INCLUDE_FILE_IDS, userId, true).getResult();
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
                    FileManager.INCLUDE_FILE_IDS, userId, true).getResult();
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
    public QueryResult<Job> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("To be implemented");
    }

    @Override
    public DBIterator<Job> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        fixQueryObject(study, query, userId);

        return jobDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Job> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, userId);

        query.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = jobDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_JOBS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        WriteResult writeResult = new WriteResult("delete", -1, -1, -1, null, null, null);

        String userId;
        Study study;

        StopWatch watch = StopWatch.createStarted();

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the jobs to be deleted
        DBIterator<Job> iterator;
        try {
            userId = catalogManager.getUserManager().getUserId(sessionId);
            study = catalogManager.getStudyManager().resolveId(studyStr, userId);

            fixQueryObject(study, query, userId);
            finalQuery.append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = jobDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error("Delete job: {}", e.getMessage(), e);
            writeResult.setError(new Error(-1, null, e.getMessage()));
            writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
            return writeResult;
        }

        long numMatches = 0;
        long numModified = 0;
        List<WriteResult.Fail> failedList = new ArrayList<>();

        String suffixName = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        while (iterator.hasNext()) {
            Job job = iterator.next();
            numMatches += 1;

            try {
                if (checkPermissions) {
                    authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.DELETE);
                }

                // Check if the job can be deleted
                checkJobCanBeDeleted(job);

                // Delete the job
                Query updateQuery = new Query()
                        .append(JobDBAdaptor.QueryParams.UID.key(), job.getUid())
                        .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                ObjectMap updateParams = new ObjectMap()
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED)
                        .append(JobDBAdaptor.QueryParams.NAME.key(), job.getName() + suffixName);
                QueryResult<Long> update = jobDBAdaptor.update(updateQuery, updateParams, QueryOptions.empty());
                if (update.first() > 0) {
                    numModified += 1;
                    auditManager.recordDeletion(AuditRecord.Resource.job, job.getUid(), userId, null, updateParams, null, null);
                } else {
                    failedList.add(new WriteResult.Fail(job.getId(), "Unknown reason"));
                }
            } catch (Exception e) {
                failedList.add(new WriteResult.Fail(job.getId(), e.getMessage()));
                logger.debug("Cannot delete job {}: {}", job.getId(), e.getMessage(), e);
            }
        }

        writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        writeResult.setNumMatches(numMatches);
        writeResult.setNumModified(numModified);
        writeResult.setFailed(failedList);

        if (!failedList.isEmpty()) {
            writeResult.setWarning(Collections.singletonList(new Error(-1, null, "There are jobs that could not be deleted")));
        }

        return writeResult;
    }

    private void checkJobCanBeDeleted(Job job) throws CatalogException {
        switch (job.getStatus().getName()) {
            case Job.JobStatus.DELETED:
                throw new CatalogException("Job already deleted.");
            case Job.JobStatus.PREPARED:
            case Job.JobStatus.RUNNING:
            case Job.JobStatus.QUEUED:
                throw new CatalogException("The status of the job is " + job.getStatus().getName()
                        + ". Please, stop the job before deleting it.");
            case Job.JobStatus.DONE:
            case Job.JobStatus.ERROR:
            case Job.JobStatus.READY:
            default:
                break;
        }
    }

    @Override
    public QueryResult<Job> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);
        Job job = internalGet(study.getUid(), entryStr, INCLUDE_JOB_IDS, userId).first();

        authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);

        QueryResult<Job> queryResult = jobDBAdaptor.update(job.getUid(), parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.job, job.getUid(), userId, parameters, null, null);
        return queryResult;
    }

    public QueryResult<Job> update(Long jobId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        return update(null, String.valueOf(jobId), parameters, options, sessionId);
    }

    public void setStatus(String studyStr, String id, String status, String message, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);
        Job job = internalGet(study.getUid(), id, INCLUDE_JOB_IDS, userId).first();

        authorizationManager.checkJobPermission(study.getUid(), job.getUid(), userId, JobAclEntry.JobPermissions.UPDATE);

        if (status != null && !Job.JobStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid job status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        jobDBAdaptor.update(job.getUid(), parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.job, job.getUid(), userId, parameters, null, null);
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_JOBS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogJobDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = jobDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = jobDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult<Job> queue(String studyStr, String jobName, String toolId, String description, String execution, Job.Type type,
                                  Map<String, String> params, List<File> input, List<File> output, File outDir,
                                  Map<String, Object> attributes, String token)
            throws CatalogException {
        Job job = new Job(jobName, toolId, execution, type, description, params, attributes)
                .setInput(input)
                .setOutput(output)
                .setOutDir(outDir);

        return create(studyStr, job, QueryOptions.empty(), token);
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<JobAclEntry>> getAcls(String studyStr, List<String> jobList, String member, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<JobAclEntry>> jobAclList = new ArrayList<>(jobList.size());
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        InternalGetQueryResult<Job> queryResult = internalGet(study.getUid(), jobList, INCLUDE_JOB_IDS, user, silent);

        Map<String, InternalGetQueryResult.Missing> missingMap = new HashMap<>();
        if (queryResult.getMissing() != null) {
            missingMap = queryResult.getMissing().stream()
                    .collect(Collectors.toMap(InternalGetQueryResult.Missing::getId, Function.identity()));
        }
        int counter = 0;
        for (String jobId : jobList) {
            if (!missingMap.containsKey(jobId)) {
                try {
                    QueryResult<JobAclEntry> allJobAcls;
                    if (StringUtils.isNotEmpty(member)) {
                        allJobAcls = authorizationManager.getJobAcl(study.getUid(), queryResult.getResult().get(counter).getUid(), user,
                                member);
                    } else {
                        allJobAcls = authorizationManager.getAllJobAcls(study.getUid(), queryResult.getResult().get(counter).getUid(),
                                user);
                    }
                    allJobAcls.setId(jobId);
                    jobAclList.add(allJobAcls);
                } catch (CatalogException e) {
                    if (!silent) {
                        throw e;
                    } else {
                        jobAclList.add(new QueryResult<>(jobId, queryResult.getDbTime(), 0, 0, "", missingMap.get(jobId).getErrorMsg(),
                                Collections.emptyList()));
                    }
                }
                counter += 1;
            } else {
                jobAclList.add(new QueryResult<>(jobId, queryResult.getDbTime(), 0, 0, "", missingMap.get(jobId).getErrorMsg(),
                        Collections.emptyList()));
            }
        }
        return jobAclList;
    }

    public List<QueryResult<JobAclEntry>> updateAcl(String studyStr, List<String> jobStringList, String memberIds, AclParams aclParams,
                                                    String sessionId) throws CatalogException {
        if (jobStringList == null || jobStringList.isEmpty()) {
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

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        List<Job> jobList = internalGet(study.getUid(), jobStringList, INCLUDE_JOB_IDS, userId, false).getResult();

        authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(study.getUid(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (aclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allJobPermissions = EnumSet.allOf(JobAclEntry.JobPermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(study.getUid(), jobList.stream().map(Job::getUid)
                                .collect(Collectors.toList()), members, permissions, allJobPermissions, Entity.JOB);
            case ADD:
                return authorizationManager.addAcls(study.getUid(), jobList.stream().map(Job::getUid)
                                .collect(Collectors.toList()), members, permissions, Entity.JOB);
            case REMOVE:
                return authorizationManager.removeAcls(jobList.stream().map(Job::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.JOB);
            case RESET:
                return authorizationManager.removeAcls(jobList.stream().map(Job::getUid).collect(Collectors.toList()),
                        members, null, Entity.JOB);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }


    // **************************   Private methods  ******************************** //

    private long getJobId(boolean silent, String jobStr) throws CatalogException {
        long jobId = Long.parseLong(jobStr);
        try {
            jobDBAdaptor.checkId(jobId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return jobId;
    }

}
