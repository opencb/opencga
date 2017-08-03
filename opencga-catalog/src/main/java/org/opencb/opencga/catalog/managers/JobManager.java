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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IEntryManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobManager extends AbstractManager implements IEntryManager<Long, Job> {

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);
    private UserManager userManager;

    public static final String DELETE_FILES = "deleteFiles";

    public JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, DBAdaptorFactory catalogDBAdaptorFactory,
                      CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    public JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                      DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                      Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        this.userManager = catalogManager.getUserManager();
    }

    @Deprecated
    public JobManager(AuthorizationManager authorizationManager, AuditManager auditManager, DBAdaptorFactory catalogDBAdaptorFactory,
                      CatalogIOManagerFactory ioManagerFactory, Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }


    @Override
    public Long getStudyId(long jobId) throws CatalogException {
        return jobDBAdaptor.getStudyId(jobId);
    }

    @Override
    public MyResourceId getId(String jobStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(jobStr)) {
            throw new CatalogException("Missing job parameter");
        }

        String userId;
        long studyId;
        long jobId;

        if (StringUtils.isNumeric(jobStr) && Long.parseLong(jobStr) > configuration.getCatalog().getOffset()) {
            jobId = Long.parseLong(jobStr);
            jobDBAdaptor.exists(jobId);
            studyId = jobDBAdaptor.getStudyId(jobId);
            userId = userManager.getId(sessionId);
        } else {
            if (jobStr.contains(",")) {
                throw new CatalogException("More than one job found");
            }

            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            jobId = smartResolutor(jobStr, studyId);
        }

        return new MyResourceId(userId, studyId, jobId);
    }

    @Override
    public MyResourceIds getIds(String jobStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(jobStr)) {
            throw new CatalogException("Missing job parameter");
        }

        String userId;
        long studyId;
        List<Long> jobIds;

        if (StringUtils.isNumeric(jobStr) && Long.parseLong(jobStr) > configuration.getCatalog().getOffset()) {
            jobIds = Arrays.asList(Long.parseLong(jobStr));
            jobDBAdaptor.exists(jobIds.get(0));
            studyId = jobDBAdaptor.getStudyId(jobIds.get(0));
            userId = userManager.getId(sessionId);
        } else {
            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            String[] jobSplit = jobStr.split(",");
            jobIds = new ArrayList<>(jobSplit.length);
            for (String jobStrAux : jobSplit) {
                jobIds.add(smartResolutor(jobStrAux, studyId));
            }
        }

        return new MyResourceIds(userId, studyId, jobIds);
    }

    private Long smartResolutor(String jobName, long studyId) throws CatalogException {
        if (StringUtils.isNumeric(jobName)) {
            long jobId = Long.parseLong(jobName);
            if (jobId > configuration.getCatalog().getOffset()) {
                jobDBAdaptor.exists(jobId);
                return jobId;
            }
        }

        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(JobDBAdaptor.QueryParams.NAME.key(), jobName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, JobDBAdaptor.QueryParams.ID.key());
        QueryResult<Job> queryResult = jobDBAdaptor.get(query, qOptions);

        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one job id found based on " + jobName);
        } else if (queryResult.getNumResults() == 0) {
            throw new CatalogException("Error: No job found based on " + jobName);
        } else {
            return queryResult.first().getId();
        }
    }

    public QueryResult<ObjectMap> visit(long jobId, String sessionId) throws CatalogException {
        MyResourceId resource = getId(Long.toString(jobId), null, sessionId);

        authorizationManager.checkJobPermission(resource.getStudyId(), jobId, resource.getUser(), JobAclEntry.JobPermissions.VIEW);
        return jobDBAdaptor.incJobVisits(jobId);
    }

    public QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor,
                                   Map<String, String> params, String commandLine, URI tmpOutDirUri, long outDirId,
                                   List<File> inputFiles, List<File> outputFiles, Map<String, Object> attributes,
                                   Map<String, Object> resourceManagerAttributes, Job.JobStatus status, long startTime,
                                   long endTime, QueryOptions options, String sessionId) throws CatalogException {
        Job job = new Job(-1, name, "", toolName, null, "", description, startTime, endTime, "", executor, "", commandLine, 0, status,
                -1, new File().setId(outDirId), inputFiles, outputFiles, Collections.emptyList(), params, -1, attributes,
                resourceManagerAttributes, "", "");
        return create(String.valueOf(studyId), job, options, sessionId);
    }

    @Override
    public QueryResult<Job> create(String studyStr, Job job, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

        ParamUtils.checkObj(job, "Job");
        ParamUtils.checkParameter(job.getName(), "Name");
        ParamUtils.checkParameter(job.getToolName(), "toolName");
        ParamUtils.checkParameter(job.getCommandLine(), "commandLine");
        ParamUtils.checkObj(job.getOutDir(), "outDir");
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
        job.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));

        // FIXME check inputFiles? is a null conceptually valid?
//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        authorizationManager.checkFilePermission(studyId, job.getOutDir().getId(), userId, FileAclEntry.FilePermissions.WRITE);
        for (File inputFile : job.getInput()) {
            authorizationManager.checkFilePermission(studyId, inputFile.getId(), userId, FileAclEntry.FilePermissions.VIEW);
        }
        QueryOptions fileQueryOptions = new QueryOptions("include", Arrays.asList(
                "projects.studies.files.id",
                "projects.studies.files.type",
                "projects.studies.files.path"));
        File outDir = fileDBAdaptor.get(job.getOutDir().getId(), fileQueryOptions).first();

        if (!outDir.getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogException("Bad outDir type. Required type : " + File.Type.DIRECTORY);
        }

        QueryResult<Job> queryResult = jobDBAdaptor.insert(job, studyId, options);
//        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.job, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Job> get(Long jobId, QueryOptions options, String sessionId) throws CatalogException {
        MyResourceId resource = getId(Long.toString(jobId), null, sessionId);
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.ID.key(), jobId)
                .append(JobDBAdaptor.QueryParams.STUDY_ID.key(), resource.getStudyId());
        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, options, resource.getUser());
        if (jobQueryResult.getNumResults() <= 0) {
            throw CatalogAuthorizationException.deny(resource.getUser(), "view", "job", jobId, "");
        }
        return jobQueryResult;
    }

    @Override
    public QueryResult<Job> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        long studyId = query.getLong(JobDBAdaptor.QueryParams.STUDY_ID.key(), -1);
        return get(studyId, query, options, sessionId);
    }

    public QueryResult<Job> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        // If studyId is null, check if there is any on the query
        // Else, ensure that studyId is in the Query
        if (studyId <= 0) {
            throw new CatalogException("Missing study parameter");
        }
        query.put(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        if (query.containsKey("inputFiles")) {
            MyResourceIds inputFiles = catalogManager.getFileManager().getIds(query.getString("inputFiles"), Long.toString(studyId),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.INPUT_ID.key(), inputFiles.getResourceIds());
            query.remove("inputFiles");
        }
        if (query.containsKey("outputFiles")) {
            MyResourceIds inputFiles = catalogManager.getFileManager().getIds(query.getString("outputFiles"), Long.toString(studyId),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.OUTPUT_ID.key(), inputFiles.getResourceIds());
            query.remove("outputFiles");
        }

        String userId = userManager.getId(sessionId);

        QueryResult<Job> queryResult = jobDBAdaptor.get(query, options, userId);
        return queryResult;
    }

    @Override
    public QueryResult<Job> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("To be implemented");
    }

    @Override
    public DBIterator<Job> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // If studyId is null, check if there is any on the query
        // Else, ensure that studyId is in the Query
        if (studyId <= 0) {
            throw new CatalogException("Missing study parameter");
        }
        query.put(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        if (query.containsKey("inputFiles")) {
            MyResourceIds inputFiles = catalogManager.getFileManager().getIds(query.getString("inputFiles"), Long.toString(studyId),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.INPUT_ID.key(), inputFiles.getResourceIds());
            query.remove("inputFiles");
        }
        if (query.containsKey("outputFiles")) {
            MyResourceIds inputFiles = catalogManager.getFileManager().getIds(query.getString("outputFiles"), Long.toString(studyId),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.OUTPUT_ID.key(), inputFiles.getResourceIds());
            query.remove("outputFiles");
        }

        return jobDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Job> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (query.containsKey("inputFiles")) {
            MyResourceIds inputFiles = catalogManager.getFileManager().getIds(query.getString("inputFiles"), Long.toString(studyId),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.INPUT_ID.key(), inputFiles.getResourceIds());
            query.remove("inputFiles");
        }
        if (query.containsKey("outputFiles")) {
            MyResourceIds inputFiles = catalogManager.getFileManager().getIds(query.getString("outputFiles"), Long.toString(studyId),
                    sessionId);
            query.put(JobDBAdaptor.QueryParams.OUTPUT_ID.key(), inputFiles.getResourceIds());
            query.remove("outputFiles");
        }

        query.append(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = jobDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_JOBS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public QueryResult<Job> update(Long jobId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        MyResourceId resource = getId(Long.toString(jobId), null, sessionId);
        authorizationManager.checkJobPermission(resource.getStudyId(), jobId, resource.getUser(), JobAclEntry.JobPermissions.UPDATE);
        QueryResult<Job> queryResult = jobDBAdaptor.update(jobId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.job, jobId, resource.getUser(), parameters, null, null);
        return queryResult;
    }

    @Override
    public List<QueryResult<Job>> delete(String jobIdStr, @Nullable String studyStr, ObjectMap options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(jobIdStr, "id");
//        options = ParamUtils.defaultObject(options, QueryOptions::new);

//        boolean deleteFiles = options.getBoolean(DELETE_FILES);
//        options.remove(DELETE_FILES);

        MyResourceIds resourceIds = getIds(jobIdStr, studyStr, sessionId);
        String userId = resourceIds.getUser();

        List<QueryResult<Job>> queryResultList = new ArrayList<>(resourceIds.getResourceIds().size());
        for (Long jobId : resourceIds.getResourceIds()) {
            QueryResult<Job> queryResult = null;
            try {
                authorizationManager.checkJobPermission(resourceIds.getStudyId(), jobId, userId, JobAclEntry.JobPermissions.DELETE);

                QueryResult<Job> jobQueryResult = jobDBAdaptor.get(jobId, QueryOptions.empty());
                if (jobQueryResult.first().getOutput() != null && jobQueryResult.first().getOutput().size() > 0) {
                    throw new CatalogException("The job created " + jobQueryResult.first().getOutput().size() + " files. Please, delete "
                            + "them first.");
                }

                switch (jobQueryResult.first().getStatus().getName()) {
                    case Job.JobStatus.TRASHED:
                    case Job.JobStatus.DELETED:
                        throw new CatalogException("The job {" + jobId + "} was already " + jobQueryResult.first().getStatus().getName());
                    case Job.JobStatus.PREPARED:
                    case Job.JobStatus.RUNNING:
                    case Job.JobStatus.QUEUED:
                        throw new CatalogException("The job {" + jobId + "} is " + jobQueryResult.first().getStatus().getName()
                                + ". Please, stop the job before deleting it.");
                    case Job.JobStatus.DONE:
                    case Job.JobStatus.ERROR:
                    case Job.JobStatus.READY:
                    default:
                        break;
                }

                ObjectMap params = new ObjectMap()
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.DELETED);
                queryResult = jobDBAdaptor.update(jobId, params);
                queryResult.setId("Delete job " + jobId);
                auditManager.recordAction(AuditRecord.Resource.job, AuditRecord.Action.delete, AuditRecord.Magnitude.high, jobId, userId,
                        jobQueryResult.first(), queryResult.first(), "", null);
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.job, AuditRecord.Action.delete, AuditRecord.Magnitude.high,
                        jobId, userId, null, null, e.getMessage(), null);
                queryResult = new QueryResult<>("Delete job " + jobId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                e.printStackTrace();
                queryResult = new QueryResult<>("Delete job " + jobId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }

            // Remove jobId references from file
            try {
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                        .append(FileDBAdaptor.QueryParams.JOB_ID.key(), jobId);

                ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.JOB_ID.key(), -1);
                fileDBAdaptor.update(query, params);
            } catch (CatalogDBException e) {
                logger.error("An error occurred when removing reference of job " + jobId + " from files", e);
            }

        }

        return queryResultList;
    }

    @Override
    public List<QueryResult<Job>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, JobDBAdaptor.QueryParams.ID.key());
        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, queryOptions);
        List<Long> jobIds = jobQueryResult.getResult().stream().map(Job::getId).collect(Collectors.toList());
        String jobStr = StringUtils.join(jobIds, ",");
        return delete(jobStr, null, QueryOptions.empty(), sessionId);
    }

    @Override
    public List<QueryResult<Job>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Cannot restore jobs. Restore job not implemented.");
    }

    @Override
    public List<QueryResult<Job>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Cannot restore jobs. Restore job not implemented.");
    }

    @Override
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        MyResourceId resource = getId(id, null, sessionId);

        authorizationManager.checkJobPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                JobAclEntry.JobPermissions.UPDATE);

        if (status != null && !Job.JobStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid job status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        jobDBAdaptor.update(resource.getResourceId(), parameters);
        auditManager.recordUpdate(AuditRecord.Resource.job, resource.getResourceId(), resource.getUser(), parameters, null, null);
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_JOBS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
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

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_JOBS);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = jobDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult<Job> queue(long studyId, String jobName, String description, String executable, Job.Type type,
                                  Map<String, String> params, List<File> input, List<File> output, File outDir, String userId,
                                  Map<String, Object> attributes)
            throws CatalogException {
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

        Job job = new Job(jobName, userId, executable, type, input, output, outDir, params,
                catalogManager.getStudyManager().getCurrentRelease(studyId))
                .setDescription(description)
                .setAttributes(attributes);

        QueryResult<Job> queryResult = jobDBAdaptor.insert(job, studyId, new QueryOptions());
        auditManager.recordAction(AuditRecord.Resource.job, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    public List<QueryResult<JobAclEntry>> updateAcl(String job, String studyStr, String memberIds, AclParams aclParams, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(job)) {
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
        MyResourceIds resourceIds = getIds(job, studyStr, sessionId);

        // Check the user has the permissions needed to change permissions
        for (Long jobId : resourceIds.getResourceIds()) {
            authorizationManager.checkJobPermission(resourceIds.getStudyId(), jobId, resourceIds.getUser(),
                    JobAclEntry.JobPermissions.SHARE);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, resourceIds.getStudyId(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        String collectionName = MongoDBAdaptorFactory.JOB_COLLECTION;

        switch (aclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
            case ADD:
                return authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
            case REMOVE:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
            case RESET:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    public URI createJobOutDir(long studyId, String dirName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(dirName, "dirName");

        String userId = userManager.getId(sessionId);

        URI uri = studyDBAdaptor.get(studyId, new QueryOptions("include", Collections.singletonList("projects.studies.uri")))
                .first().getUri();

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uri);
        return catalogIOManager.createJobOutDir(userId, dirName);
    }

}
