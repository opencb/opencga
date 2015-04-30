package org.opencb.opencga.catalog;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.api.IJobManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class JobManager implements IJobManager {

    final protected AuthorizationManager authorizationManager;
    final protected CatalogUserDBAdaptor userDBAdaptor;
    final protected CatalogStudyDBAdaptor studyDBAdaptor;
    final protected CatalogFileDBAdaptor fileDBAdaptor;
    final protected CatalogSamplesDBAdaptor sampleDBAdaptor;
    final protected CatalogJobDBAdaptor jobDBAdaptor;
    final protected CatalogIOManagerFactory catalogIOManagerFactory;

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);

    public JobManager(AuthorizationManager authorizationManager, CatalogDBAdaptor catalogDBAdaptor,
                      CatalogIOManagerFactory ioManagerFactory) {
        this.authorizationManager = authorizationManager;
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptor.getCatalogSamplesDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
    }
    @Override
    public Integer getStudyId(int jobId) throws CatalogException {
        return jobDBAdaptor.getStudyIdByJobId(jobId);
    }

    @Override
    public QueryResult<ObjectMap> visit(int jobId, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = jobDBAdaptor.getStudyIdByJobId(jobId);
        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. Can't read job");
        }
        return jobDBAdaptor.incJobVisits(jobId);
    }

    @Override
    public QueryResult<Job> create(QueryOptions params, String sessionId)
            throws CatalogException {
        checkObj(params, "Params");
        try {
            return create(
                    params.getInt("studyId"),
                    params.getString("name"),
                    params.getString("toolName"),
                    params.getString("description"),
                    params.getString("commandLine"),
                    params.containsKey("tmpOutDirUri")? new URI(null, params.getString("tmpOutDirUri"), null) : null,
                    params.getInt("outDirId"),
                    params.getAsIntegerList("inputFiles"),
                    params.getMap("attributes"),
                    params.getMap("resourceManagerAttributes"),
                    Job.Status.valueOf(params.getString("status")),
                    params,
                    sessionId
            );
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public QueryResult<Job> create(int studyId, String name, String toolName, String description, String commandLine,
                                   URI tmpOutDirUri, int outDirId, List<Integer> inputFiles,
                                   Map<String, Object> attributes,Map<String, Object> resourceManagerAttributes,
                                   Job.Status status, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(toolName, "toolName");
        checkParameter(commandLine, "commandLine");
        description = defaultString(description, "");
        status = defaultObject(status, Job.Status.PREPARED);

        // FIXME check inputFiles? is a null conceptually valid?

//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. Can't create job");
        }
        QueryOptions fileQueryOptions = new QueryOptions("include", Arrays.asList("id", "type", "path"));
        File outDir = fileDBAdaptor.getFile(outDirId, fileQueryOptions).getResult().get(0);

        if (!outDir.getType().equals(File.Type.FOLDER)) {
            throw new CatalogException("Bad outDir type. Required type : " + File.Type.FOLDER);
        }

        Job job = new Job(name, userId, toolName, description, commandLine, outDir.getId(), tmpOutDirUri, inputFiles);
        job.setStatus(status);
        if (resourceManagerAttributes != null) {
            job.getResourceManagerAttributes().putAll(resourceManagerAttributes);
        }
        if (attributes != null) {
            job.setAttributes(attributes);
        }

        return jobDBAdaptor.createJob(studyId, job, options);
    }

    @Override
    public QueryResult<Job> read(Integer jobId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = jobDBAdaptor.getStudyIdByJobId(jobId);
        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. Can't read job");
        }

        return jobDBAdaptor.getJob(jobId, options);
    }

    @Override
    public QueryResult<Job> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        query = defaultObject(query, new QueryOptions());
        query.put("studyId", studyId);
        return readAll(query, options, sessionId);
    }

    @Override
    public QueryResult<Job> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkObj(options, "options");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            if (!options.containsKey("studyId")) {
                throw new CatalogException("Permission denied. Can't get jobs without specify an StudyId");
            } else {
                int studyId = options.getInt("studyId");
                if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
                    throw new CatalogException("Permission denied. Can't get jobs");
                }
            }
        }
        return jobDBAdaptor.searchJob(options);
    }

    @Override
    public QueryResult<Job> update(Integer jobId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkObj(parameters, "parameters");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = jobDBAdaptor.getStudyIdByJobId(jobId);
        if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
                throw new CatalogException("Permission denied. Can't modify jobs");
            }
        }
        return jobDBAdaptor.modifyJob(jobId, parameters);
    }

    @Override
    public QueryResult<Job> delete(Integer jobId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = jobDBAdaptor.getStudyIdByJobId(jobId);
        if (!authorizationManager.getStudyACL(userId, studyId).isDelete()) {
            throw new CatalogException("Permission denied. Can't delete job");
        }

        return jobDBAdaptor.deleteJob(jobId);
    }

    @Override
    public URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(dirName, "dirName");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. Can't read study");
        }
        URI uri = studyDBAdaptor.getStudy(studyId, new QueryOptions("include", Arrays.asList("projects.studies.uri")))
                .first().getUri();

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uri);
        return catalogIOManager.createJobOutDir(userId, dirName);
    }
}
