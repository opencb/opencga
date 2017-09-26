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
import org.opencb.commons.utils.CollectionUtils;
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
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
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
            userId = userManager.getUserId(sessionId);
        } else {
            if (jobStr.contains(",")) {
                throw new CatalogException("More than one job found");
            }

            userId = userManager.getUserId(sessionId);
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
            userId = userManager.getUserId(sessionId);
        } else {
            userId = userManager.getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            String[] jobSplit = jobStr.split(",");
            jobIds = new ArrayList<>(jobSplit.length);
            for (String jobStrAux : jobSplit) {
                jobIds.add(smartResolutor(jobStrAux, studyId));
            }
        }

        return new MyResourceIds(userId, studyId, jobIds);
    }

    public QueryResult<Job> visit(long jobId, String sessionId) throws CatalogException {
        MyResourceId resource = getId(Long.toString(jobId), null, sessionId);

        authorizationManager.checkJobPermission(resource.getStudyId(), jobId, resource.getUser(), JobAclEntry.JobPermissions.VIEW);
        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.VISITED.key(), true);
        return jobDBAdaptor.update(jobId, params, QueryOptions.empty());
    }

    public QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor,
                                   Map<String, String> params, String commandLine, URI tmpOutDirUri, long outDirId,
                                   List<File> inputFiles, List<File> outputFiles, Map<String, Object> attributes,
                                   Map<String, Object> resourceManagerAttributes, Job.JobStatus status, long startTime,
                                   long endTime, QueryOptions options, String sessionId) throws CatalogException {
        Job job = new Job(-1, name, "", toolName, null, "", description, startTime, endTime, executor, "", commandLine, false, status,
                -1, new File().setId(outDirId), inputFiles, outputFiles, Collections.emptyList(), params, -1, attributes,
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
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_JOBS);

        ParamUtils.checkObj(job, "Job");
        ParamUtils.checkParameter(job.getName(), "Name");
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
        job.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));

        // FIXME check inputFiles? is a null conceptually valid?
//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        for (File inputFile : job.getInput()) {
            authorizationManager.checkFilePermission(studyId, inputFile.getId(), userId, FileAclEntry.FilePermissions.VIEW);
        }
        if (job.getOutDir() != null) {
            authorizationManager.checkFilePermission(studyId, job.getOutDir().getId(), userId, FileAclEntry.FilePermissions.WRITE);
            QueryOptions fileQueryOptions = new QueryOptions("include", Arrays.asList(
                    "projects.studies.files.id",
                    "projects.studies.files.type",
                    "projects.studies.files.path"));
            File outDir = fileDBAdaptor.get(job.getOutDir().getId(), fileQueryOptions).first();

            if (!outDir.getType().equals(File.Type.DIRECTORY)) {
                throw new CatalogException("Bad outDir type. Required type : " + File.Type.DIRECTORY);
            }
        }

        QueryResult<Job> queryResult = jobDBAdaptor.insert(job, studyId, options);
        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getId(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    public QueryResult<Job> get(long jobId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(jobId), options, sessionId);
    }

    @Override
    public QueryResult<Job> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

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

        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, options, userId);

        if (jobQueryResult.getNumResults() == 0 && query.containsKey("id")) {
            List<Long> idList = query.getAsLongList("id");
            for (Long myId : idList) {
                authorizationManager.checkJobPermission(studyId, myId, userId, JobAclEntry.JobPermissions.VIEW);
            }
        }

        return jobQueryResult;
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

        String userId = userManager.getUserId(sessionId);
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
    public QueryResult<Job> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceId resource = getId(entryStr, studyStr, sessionId);
        authorizationManager.checkJobPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                JobAclEntry.JobPermissions.UPDATE);

        QueryResult<Job> queryResult = jobDBAdaptor.update(resource.getResourceId(), parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.job, resource.getResourceId(), resource.getUser(), parameters, null, null);
        return queryResult;
    }

    public QueryResult<Job> update(Long jobId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        return update(null, String.valueOf(jobId), parameters, options, sessionId);
    }

    @Override
    public List<QueryResult<Job>> delete(@Nullable String studyStr, String jobIdStr, ObjectMap options, String sessionId)
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
                if (jobQueryResult.first().getOutput() != null && CollectionUtils.isNotEmpty(jobQueryResult.first().getOutput())) {
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
                queryResult = jobDBAdaptor.update(jobId, params, QueryOptions.empty());
                queryResult.setId("Delete job " + jobId);
                auditManager.recordDeletion(AuditRecord.Resource.job, jobId, userId, jobQueryResult.first(), queryResult.first(),
                        null, null);

            } catch (CatalogAuthorizationException e) {
                auditManager.recordDeletion(AuditRecord.Resource.job, jobId, userId, null, e.getMessage(), null);

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
                fileDBAdaptor.update(query, params, QueryOptions.empty());
            } catch (CatalogDBException e) {
                logger.error("An error occurred when removing reference of job " + jobId + " from files", e);
            }

        }

        return queryResultList;
    }

    public List<QueryResult<Job>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, JobDBAdaptor.QueryParams.ID.key());
        QueryResult<Job> jobQueryResult = jobDBAdaptor.get(query, queryOptions);
        List<Long> jobIds = jobQueryResult.getResult().stream().map(Job::getId).collect(Collectors.toList());
        String jobStr = StringUtils.join(jobIds, ",");
        return delete(null, jobStr, QueryOptions.empty(), sessionId);
    }

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

        jobDBAdaptor.update(resource.getResourceId(), parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.job, resource.getResourceId(), resource.getUser(), parameters, null, null);
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
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

        String userId = userManager.getUserId(sessionId);
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
        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getId(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    public URI createJobOutDir(long studyId, String dirName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(dirName, "dirName");

        String userId = userManager.getUserId(sessionId);

        URI uri = studyDBAdaptor.get(studyId, new QueryOptions("include", Collections.singletonList("projects.studies.uri")))
                .first().getUri();

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uri);
        return catalogIOManager.createJobOutDir(userId, dirName);
    }


    // **************************   ACLs  ******************************** //

    public List<QueryResult<JobAclEntry>> getAcls(String studyStr, String jobStr, String sessionId) throws CatalogException {
        MyResourceIds resource = getIds(jobStr, studyStr, sessionId);

        List<QueryResult<JobAclEntry>> jobAclList = new ArrayList<>(resource.getResourceIds().size());
        for (Long jobId : resource.getResourceIds()) {
            QueryResult<JobAclEntry> allJobAcls = authorizationManager.getAllJobAcls(resource.getStudyId(), jobId, resource.getUser());
            allJobAcls.setId(String.valueOf(jobId));
            jobAclList.add(allJobAcls);
        }

        return jobAclList;
    }

    public List<QueryResult<JobAclEntry>> getAcl(String studyStr, String jobStr, String member, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(member, "member");

        MyResourceIds resource = getIds(jobStr, studyStr, sessionId);
        checkMembers(resource.getStudyId(), Arrays.asList(member));

        List<QueryResult<JobAclEntry>> jobAclList = new ArrayList<>(resource.getResourceIds().size());
        for (Long jobId : resource.getResourceIds()) {
            QueryResult<JobAclEntry> allJobAcls = authorizationManager.getJobAcl(resource.getStudyId(), jobId, resource.getUser(), member);
            allJobAcls.setId(String.valueOf(jobId));
            jobAclList.add(allJobAcls);
        }

        return jobAclList;
    }

    public List<QueryResult<JobAclEntry>> updateAcl(String studyStr, String jobStr, String memberIds, AclParams aclParams, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(jobStr)) {
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
        MyResourceIds resourceIds = getIds(jobStr, studyStr, sessionId);

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
        checkMembers(resourceIds.getStudyId(), members);
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


    // **************************   Private methods  ******************************** //

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

}
