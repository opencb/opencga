package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.authorization.StudyPermission;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.managers.api.IJobManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobManager extends AbstractManager implements IJobManager {

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);

    public JobManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                      AuditManager auditManager,
                      CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                      Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }


    @Override
    public Integer getStudyId(int jobId) throws CatalogException {
        return jobDBAdaptor.getStudyIdByJobId(jobId);
    }

    @Override
    public QueryResult<ObjectMap> visit(int jobId, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkReadJob(userId, jobId);
        return jobDBAdaptor.incJobVisits(jobId);
    }

    @Override
    public QueryResult<Job> create(QueryOptions params, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(params, "Params");
        try {
            return create(
                    params.getInt("studyId"),
                    params.getString("name"),
                    params.getString("toolName"),
                    params.getString("description"),
                    params.getString("execution"),
                    Collections.emptyMap(),
                    params.getString("commandLine"),
                    params.containsKey("tmpOutDirUri")? new URI(null, params.getString("tmpOutDirUri"), null) : null,
                    params.getInt("outDirId"),
                    params.getAsIntegerList("inputFiles"),
                    params.getAsIntegerList("outputFiles"),
                    params.getMap("attributes"),
                    params.getMap("resourceManagerAttributes"),
                    Job.Status.valueOf(params.getString("status")),
                    params.getLong("startTime"),
                    params.getLong("endTime"),
                    params,
                    sessionId
            );
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public QueryResult<Job> create(int studyId, String name, String toolName, String description, String executor, Map<String, String> params, String commandLine,
                                   URI tmpOutDirUri, int outDirId, List<Integer> inputFiles, List<Integer> outputFiles,
                                   Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes,
                                   Job.Status status, long startTime, long endTime, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(toolName, "toolName");
        ParamUtils.checkParameter(commandLine, "commandLine");
        description = ParamUtils.defaultString(description, "");
        status = ParamUtils.defaultObject(status, Job.Status.PREPARED);
        inputFiles = ParamUtils.defaultObject(inputFiles, Collections.<Integer>emptyList());
        outputFiles = ParamUtils.defaultObject(outputFiles, Collections.<Integer>emptyList());

        // FIXME check inputFiles? is a null conceptually valid?

//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        authorizationManager.checkFilePermission(outDirId, userId, CatalogPermission.WRITE);
        for (Integer inputFile : inputFiles) {
            authorizationManager.checkFilePermission(inputFile, userId, CatalogPermission.READ);
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
        auditManager.recordCreation(AuditRecord.Resource.job, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Job> read(Integer jobId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        QueryResult<Job> queryResult = jobDBAdaptor.getJob(jobId, options);
        authorizationManager.checkReadJob(userId, queryResult.first());
        return queryResult;
    }

    @Override
    public QueryResult<Job> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, QueryOptions::new);
        //Get studyId from Query and call to readAll
        int studyId = query.getInt("studyId", -1);
        return readAll(studyId, query, options, sessionId);
    }

    @Override
    public QueryResult<Job> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, QueryOptions::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        // If studyId is null, check if there is any on the query
        // Else, ensure that studyId is in the Query
        if (studyId < 0) {
            studyId = query.getInt("studyId", -1);
        } else {
            query.put("studyId", studyId);
        }
        query.putAll(options);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            if (studyId < 0) {
                throw new CatalogException("Permission denied. Can't get jobs without specify an StudyId");
            } else {
                authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);
            }
        }
        QueryResult<Job> queryResult = jobDBAdaptor.getAllJobs(query, options);
        authorizationManager.filterJobs(userId, queryResult.getResult(), studyId);
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<Job> update(Integer jobId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkObj(parameters, "parameters");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = jobDBAdaptor.getStudyIdByJobId(jobId);
        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.LAUNCH_JOBS);
        }
        QueryResult<Job> queryResult = jobDBAdaptor.modifyJob(jobId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.job, jobId, userId, parameters, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Job> delete(Integer jobId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = jobDBAdaptor.getStudyIdByJobId(jobId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_STUDY);

        QueryResult<Job> queryResult = jobDBAdaptor.deleteJob(jobId);
        auditManager.recordDeletion(AuditRecord.Resource.job, jobId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(dirName, "dirName");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.DELETE_JOBS);

        URI uri = studyDBAdaptor.getStudy(studyId, new QueryOptions("include", Collections.singletonList("projects.studies.uri")))
                .first().getUri();

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uri);
        return catalogIOManager.createJobOutDir(userId, dirName);
    }

    @Override
    public int getToolId(String toolId) throws CatalogException {
        try {
            return Integer.parseInt(toolId);
        } catch (NumberFormatException ignore) {
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
        auditManager.recordCreation(AuditRecord.Resource.tool, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Tool> readTool(int id, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(sessionId, "sessionId");

        //TODO: Check ACLs
        return jobDBAdaptor.getTool(id);
    }

    @Override
    public QueryResult<Tool> readAllTools(QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobDBAdaptor.getAllTools(queryOptions);
    }
}
