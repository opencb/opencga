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
import org.opencb.opencga.catalog.utils.ParamUtils;
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
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobManager extends ResourceManager<Job> {

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
               DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
               Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Job smartResolutor(long studyUid, String entry, String user) throws CatalogException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(JobDBAdaptor.QueryParams.ID.key(), entry);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.STUDY_UID.key(), JobDBAdaptor.QueryParams.ID.key(),
                JobDBAdaptor.QueryParams.STATUS.key()));
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
            return jobQueryResult.first();
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
        MyResource resource = getUid(jobId, studyStr, sessionId);
        authorizationManager.checkJobPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                JobAclEntry.JobPermissions.VIEW);
        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.VISITED.key(), true);
        return jobDBAdaptor.update(resource.getResource().getUid(), params, QueryOptions.empty());
    }

    @Deprecated
    public QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor,
                                   Map<String, String> params, String commandLine, URI tmpOutDirUri, long outDirId,
                                   List<File> inputFiles, List<File> outputFiles, Map<String, Object> attributes,
                                   Map<String, Object> resourceManagerAttributes, Job.JobStatus status, long startTime,
                                   long endTime, QueryOptions options, String sessionId) throws CatalogException {
        Job job = new Job(-1, name, name, "", toolName, null, "", description, startTime, endTime, executor, "", commandLine, false, status,
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
    public QueryResult<Job> create(String studyStr, Job job, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

        ParamUtils.checkObj(job, "Job");
        ParamUtils.checkParameter(job.getId(), "id");
        ParamUtils.checkParameter(job.getToolId(), "toolId");
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

        // FIXME check inputFiles? is a null conceptually valid?
//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);


        List<File> inputFileList = new ArrayList<>();
        for (File inputFile : job.getInput()) {
            String fileId = StringUtils.isEmpty(inputFile.getPath()) ? inputFile.getName() : inputFile.getPath();
            try {
                File file = catalogManager.getFileManager().smartResolutor(study.getUid(), fileId, userId);
                inputFileList.add(file);
            } catch (CatalogException e) {
                throw new CatalogException("Could not create job: " + e.getMessage(), e);
            }
        }
        job.setInput(inputFileList);

        List<File> outputFileList = new ArrayList<>();
        for (File outputFile : job.getOutput()) {
            String fileId = StringUtils.isEmpty(outputFile.getPath()) ? outputFile.getName() : outputFile.getPath();
            try {
                File file = catalogManager.getFileManager().smartResolutor(study.getUid(), fileId, userId);
                inputFileList.add(file);
            } catch (CatalogException e) {
                throw new CatalogException("Could not create job: " + e.getMessage(), e);
            }
        }
        job.setOutput(outputFileList);

        if (job.getOutDir() != null) {
            String fileName = StringUtils.isNotEmpty(job.getOutDir().getPath()) ? job.getOutDir().getPath() : job.getOutDir().getName();
            File file = catalogManager.getFileManager().smartResolutor(study.getUid(), fileName, userId);
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

            if (!file.getType().equals(File.Type.DIRECTORY)) {
                throw new CatalogException("Bad outDir type. Required type : " + File.Type.DIRECTORY);
            }

            job.setOutDir(file);
        }

        QueryResult<Job> queryResult = jobDBAdaptor.insert(job, study.getUid(), options);
        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getUid(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    public QueryResult<Job> get(long jobId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(jobId), options, sessionId);
    }

    public List<QueryResult<Job>> get(List<String> jobIds, QueryOptions options, boolean silent, String sessionId) throws CatalogException {
        return get(null, jobIds, new Query(), options, silent, sessionId);
    }

    @Override
    public QueryResult<Job> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        fixQueryObject(study, query, sessionId);

        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, options, userId);

        if (jobQueryResult.getNumResults() == 0 && query.containsKey(JobDBAdaptor.QueryParams.UID.key())) {
            List<Long> idList = query.getAsLongList(JobDBAdaptor.QueryParams.UID.key());
            for (Long myId : idList) {
                authorizationManager.checkJobPermission(study.getUid(), myId, userId, JobAclEntry.JobPermissions.VIEW);
            }
        }

        return jobQueryResult;
    }

    private void fixQueryObject(Study study, Query query, String sessionId) throws CatalogException {
        if (query.containsKey("inputFiles")) {
            MyResources<File> resource = catalogManager.getFileManager().getUids(query.getAsStringList("inputFiles"), study.getFqn(),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.INPUT_UID.key(), resource.getResourceList().stream().map(File::getUid)
                    .collect(Collectors.toList()));
            query.remove("inputFiles");
        }
        if (query.containsKey("outputFiles")) {
            MyResources<File> resource = catalogManager.getFileManager().getUids(query.getAsStringList("outputFiles"), study.getFqn(),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.OUTPUT_UID.key(), resource.getResourceList().stream().map(File::getUid)
                    .collect(Collectors.toList()));
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

        fixQueryObject(study, query, sessionId);

        return jobDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Job> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, sessionId);

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

            fixQueryObject(study, query, sessionId);
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
    public QueryResult<Job> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResource resource = getUid(entryStr, studyStr, sessionId);
        authorizationManager.checkJobPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                JobAclEntry.JobPermissions.UPDATE);

        QueryResult<Job> queryResult = jobDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.job, resource.getResource().getUid(), resource.getUser(), parameters, null, null);
        return queryResult;
    }

    public QueryResult<Job> update(Long jobId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        return update(null, String.valueOf(jobId), parameters, options, sessionId);
    }

    public void setStatus(String studyStr, String id, String status, String message, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        MyResource resource = getUid(id, studyStr, sessionId);

        authorizationManager.checkJobPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                JobAclEntry.JobPermissions.UPDATE);

        if (status != null && !Job.JobStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid job status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        jobDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.job, resource.getResource().getUid(), resource.getUser(), parameters, null, null);
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

    public QueryResult<Job> queue(String studyStr, String jobName, String description, String executable, Job.Type type,
                                  Map<String, String> params, List<File> input, List<File> output, File outDir,
                                  Map<String, Object> attributes, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

        Job job = new Job(jobName, userId, executable, type, input, output, outDir, params,
                catalogManager.getStudyManager().getCurrentRelease(study, userId))
                .setDescription(description)
                .setAttributes(attributes);

        QueryResult<Job> queryResult = jobDBAdaptor.insert(job, study.getUid(), new QueryOptions());
        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getUid(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<JobAclEntry>> getAcls(String studyStr, List<String> jobList, String member, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<JobAclEntry>> jobAclList = new ArrayList<>(jobList.size());

        for (String job : jobList) {
            try {
                MyResource<Job> resource = getUid(job, studyStr, sessionId);

                QueryResult<JobAclEntry> allJobAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allJobAcls = authorizationManager.getJobAcl(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser(), member);
                } else {
                    allJobAcls = authorizationManager.getAllJobAcls(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser());
                }
                allJobAcls.setId(job);
                jobAclList.add(allJobAcls);
            } catch (CatalogException e) {
                if (silent) {
                    jobAclList.add(new QueryResult<>(job, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return jobAclList;
    }

    public List<QueryResult<JobAclEntry>> updateAcl(String studyStr, List<String> jobList, String memberIds, AclParams aclParams,
                                                    String sessionId) throws CatalogException {
        if (jobList == null || jobList.isEmpty()) {
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

        // Obtain the resource ids
        MyResources<Job> resource = getUids(jobList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resource.getStudy().getUid(), resource.getUser());

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resource.getStudy().getUid(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (aclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allJobPermissions = EnumSet.allOf(JobAclEntry.JobPermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(resource.getStudy().getUid(), resource.getResourceList().stream().map(Job::getUid)
                                .collect(Collectors.toList()), members, permissions, allJobPermissions, Entity.JOB);
            case ADD:
                return authorizationManager.addAcls(resource.getStudy().getUid(), resource.getResourceList().stream().map(Job::getUid)
                                .collect(Collectors.toList()), members, permissions, Entity.JOB);
            case REMOVE:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(Job::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.JOB);
            case RESET:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(Job::getUid).collect(Collectors.toList()),
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
