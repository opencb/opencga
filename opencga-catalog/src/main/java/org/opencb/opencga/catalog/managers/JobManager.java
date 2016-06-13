package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IJobManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.FileAcl;
import org.opencb.opencga.catalog.models.acls.JobAcl;
import org.opencb.opencga.catalog.models.acls.StudyAcl;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobManager extends AbstractManager implements IJobManager {

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);

    public JobManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                      CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                      CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    @Deprecated
    public JobManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                      CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                      Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }


    @Override
    public Long getStudyId(long jobId) throws CatalogException {
        return jobDBAdaptor.getStudyIdByJobId(jobId);
    }

    @Override
    public Long getJobId(String userId, String jobStr) throws CatalogException {
        if (StringUtils.isNumeric(jobStr)) {
            return Long.parseLong(jobStr);
        }

        // Resolve the studyIds and filter the jobName
        ObjectMap parsedSampleStr = parseFeatureId(userId, jobStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String jobName = parsedSampleStr.getString("featureName");

        Query query = new Query(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogJobDBAdaptor.QueryParams.NAME.key(), jobName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.jobs.id");
        QueryResult<Job> queryResult = jobDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one job id found based on " + jobName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Override
    public QueryResult<JobAcl> getJobAcls(String jobStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long jobId = getJobId(userId, jobStr);
        authorizationManager.checkJobPermission(jobId, userId, JobAcl.JobPermissions.SHARE);
        Long studyId = getStudyId(jobId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();
        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }
        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getId());

                            if (!groupUsers.containsKey(group.getId())) {
                                groupUsers.put(group.getId(), new ArrayList<>());
                            }
                            groupUsers.get(group.getId()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<JobAcl> jobAclQueryResult = jobDBAdaptor.getJobAcl(jobId, memberList);

        if (members.size() == 0) {
            return jobAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one sampleAcl per member
        Map<String, JobAcl> jobAclHashMap = new HashMap<>();
        for (JobAcl jobAcl : jobAclQueryResult.getResult()) {
            for (String tmpMember : jobAcl.getUsers()) {
                if (memberList.contains(tmpMember)) {
                    if (tmpMember.startsWith("@")) {
                        // Check if the user was demanding the group directly or a user belonging to the group
                        if (groupIds.contains(tmpMember)) {
                            jobAclHashMap.put(tmpMember,
                                    new JobAcl(Collections.singletonList(tmpMember), jobAcl.getPermissions()));
                        } else {
                            // Obtain the user(s) belonging to that group whose permissions wanted the userId
                            if (groupUsers.containsKey(tmpMember)) {
                                for (String tmpUserId : groupUsers.get(tmpMember)) {
                                    if (userIds.contains(tmpUserId)) {
                                        jobAclHashMap.put(tmpUserId, new JobAcl(Collections.singletonList(tmpUserId),
                                                jobAcl.getPermissions()));
                                    }
                                }
                            }
                        }
                    } else {
                        // Add the user
                        jobAclHashMap.put(tmpMember, new JobAcl(Collections.singletonList(tmpMember), jobAcl.getPermissions()));
                    }
                }
            }
        }

        // We recreate the output that is in jobAclHashMap but in the same order the members were queried.
        List<JobAcl> jobAclList = new ArrayList<>(jobAclHashMap.size());
        for (String member : members) {
            if (jobAclHashMap.containsKey(member)) {
                jobAclList.add(jobAclHashMap.get(member));
            }
        }

        // Update queryResult info
        jobAclQueryResult.setId(jobStr);
        jobAclQueryResult.setNumResults(jobAclList.size());
        jobAclQueryResult.setNumTotalResults(jobAclList.size());
        jobAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        jobAclQueryResult.setResult(jobAclList);

        return jobAclQueryResult;
    }

    @Override
    public QueryResult<ObjectMap> visit(long jobId, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkJobPermission(jobId, userId, JobAcl.JobPermissions.VIEW);
        return jobDBAdaptor.incJobVisits(jobId);
    }

    @Deprecated
    @Override
    public QueryResult<Job> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        try {

            return create(
                    objectMap.getLong("studyId"),
                    objectMap.getString("name"),
                    objectMap.getString("toolName"),
                    objectMap.getString("description"),
                    objectMap.getString("execution"),
                    Collections.emptyMap(),
                    objectMap.getString("commandLine"),
                    objectMap.containsKey("tmpOutDirUri") ? new URI(null, objectMap.getString("tmpOutDirUri"), null) : null,
                    objectMap.getLong("outDirId"),
                    objectMap.getAsLongList("inputFiles"),
                    objectMap.getAsLongList("outputFiles"),
                    objectMap.getMap("attributes"),
                    objectMap.getMap("resourceManagerAttributes"),
                    new Job.JobStatus(options.getString("status")),
                    objectMap.getLong("startTime"),
                    objectMap.getLong("endTime"),
                    options,
                    sessionId
            );
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor,
                                   Map<String, String> params, String commandLine, URI tmpOutDirUri, long outDirId,
                                   List<Long> inputFiles, List<Long> outputFiles, Map<String, Object> attributes,
                                   Map<String, Object> resourceManagerAttributes, Job.JobStatus status, long startTime,
                                   long endTime, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(toolName, "toolName");
        ParamUtils.checkParameter(commandLine, "commandLine");
        description = ParamUtils.defaultString(description, "");
        status = ParamUtils.defaultObject(status, new Job.JobStatus(Job.JobStatus.PREPARED));
        inputFiles = ParamUtils.defaultObject(inputFiles, Collections.<Long>emptyList());
        outputFiles = ParamUtils.defaultObject(outputFiles, Collections.<Long>emptyList());

        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.CREATE_JOBS);

        // FIXME check inputFiles? is a null conceptually valid?
//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        authorizationManager.checkFilePermission(outDirId, userId, FileAcl.FilePermissions.CREATE);
        for (Long inputFile : inputFiles) {
            authorizationManager.checkFilePermission(inputFile, userId, FileAcl.FilePermissions.VIEW);
        }
        QueryOptions fileQueryOptions = new QueryOptions("include", Arrays.asList(
                "projects.studies.files.id",
                "projects.studies.files.type",
                "projects.studies.files.path"));
        File outDir = fileDBAdaptor.getFile(outDirId, fileQueryOptions).first();

        if (!outDir.getType().equals(File.Type.FOLDER)) {
            throw new CatalogException("Bad outDir type. Required type : " + File.Type.FOLDER);
        }

        Job job = new Job(name, userId, toolName, description, commandLine, outDir.getId(), tmpOutDirUri, inputFiles);
        job.setOutput(outputFiles);
        job.setStatus(status);
        job.setStartTime(startTime);
        job.setEndTime(endTime);
        job.setParams(params);
        job.setExecution(executor);

        if (resourceManagerAttributes != null) {
            job.getResourceManagerAttributes().putAll(resourceManagerAttributes);
        }
        if (attributes != null) {
            job.setAttributes(attributes);
        }

        QueryResult<Job> queryResult = jobDBAdaptor.createJob(studyId, job, options);
//        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.job, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Job> read(Long jobId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkJobPermission(jobId, userId, JobAcl.JobPermissions.VIEW);
        QueryResult<Job> queryResult = jobDBAdaptor.getJob(jobId, options);
        return queryResult;
    }

    @Override
    public QueryResult<Job> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        long studyId = query.getLong(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), -1);
        return readAll(studyId, query, options, sessionId);
    }

    @Override
    public QueryResult<Job> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        // If studyId is null, check if there is any on the query
        // Else, ensure that studyId is in the Query
        if (studyId < 0) {
            studyId = query.getLong(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), -1);
        } else {
            query.put(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }
        //query.putAll(options);
        String userId;
        if (sessionId.length() == 40) {
            catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().checkValidAdminSession(sessionId);
            userId = "admin";
        } else {
            userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        }

        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "jobs", studyId, null);
        }

        QueryResult<Job> queryResult = jobDBAdaptor.get(query, options);
        authorizationManager.filterJobs(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<Job> update(Long jobId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkObj(parameters, "parameters");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkJobPermission(jobId, userId, JobAcl.JobPermissions.UPDATE);
        QueryResult<Job> queryResult = jobDBAdaptor.update(jobId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.job, jobId, userId, parameters, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Job> delete(Long jobId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkJobPermission(jobId, userId, JobAcl.JobPermissions.DELETE);

        QueryResult<Job> queryResult = jobDBAdaptor.delete(jobId, new QueryOptions());
        // Delete the output files of the job if they are not in use.
        // TODO: Add an if clause to do this only when the user does not want to keep the output files.
        // 2. Check the output files that were created with the deleted jobs.
        Query query = new Query(CatalogFileDBAdaptor.QueryParams.ID.key(), queryResult.first().getOutput());
        try {
            fileDBAdaptor.delete(query, new QueryOptions());
        } catch (CatalogDBException e) {
            logger.info("Delete job { Job: " + queryResult.first() + " }:" + e.getMessage());
        }

        auditManager.recordDeletion(AuditRecord.Resource.job, jobId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_JOBS);

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
    public QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_JOBS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = jobDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_JOBS);

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

    @Override
    public URI createJobOutDir(long studyId, String dirName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(dirName, "dirName");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        URI uri = studyDBAdaptor.getStudy(studyId, new QueryOptions("include", Collections.singletonList("projects.studies.uri")))
                .first().getUri();

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uri);
        return catalogIOManager.createJobOutDir(userId, dirName);
    }

    @Override
    public long getToolId(String toolId) throws CatalogException {
        try {
            return Integer.parseInt(toolId);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        String[] split = toolId.split("@");
        if (split.length != 2) {
            return -1;
        }
        return jobDBAdaptor.getToolId(split[0], split[1]);
    }

    @Override
    public QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result,
                                        String path, boolean openTool, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(alias, "alias");
        ParamUtils.checkObj(description, "description"); //description can be empty
        ParamUtils.checkParameter(path, "path");
        ParamUtils.checkParameter(sessionId, "sessionId");
        //TODO: Check Path

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        List<AclEntry> acl = Arrays.asList(new AclEntry(userId, true, true, true, true));
        if (openTool) {
            acl.add(new AclEntry(AclEntry.USER_OTHERS_ID, true, false, true, false));
        }

        String name = Paths.get(path).getFileName().toString();

        Tool tool = new Tool(-1, alias, name, description, manifest, result, path, acl);

        QueryResult<Tool> queryResult = jobDBAdaptor.createTool(userId, tool);
//        auditManager.recordCreation(AuditRecord.Resource.tool, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.tool, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Tool> readTool(long id, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(sessionId, "sessionId");

        //TODO: Check ACLs
        return jobDBAdaptor.getTool(id);
    }

    @Override
    public QueryResult<Tool> readAllTools(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobDBAdaptor.getAllTools(query, queryOptions);
    }
}
