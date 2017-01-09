/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.catalog.monitor.daemons;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.api.IJobManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.ExecutionOutputRecorder;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 18/08/16.
 */
public class IndexDaemon extends MonitorParentDaemon {

    public static final String INDEX_TYPE = "INDEX_TYPE";
    public static final String ALIGNMENT_TYPE = "ALIGNMENT";
    public static final String VARIANT_TYPE = "VARIANT";

    private CatalogIOManager catalogIOManager;
    private String binHome;
    private Path tempJobFolder;

//    private VariantIndexOutputRecorder variantIndexOutputRecorder;

    public IndexDaemon(int interval, String sessionId, CatalogManager catalogManager, String appHome)
            throws URISyntaxException, CatalogIOException {
        super(interval, sessionId, catalogManager);
        this.binHome = appHome + "/bin/";
        URI uri = UriUtils.createUri(catalogManager.getConfiguration().getTempJobsDir());
        this.tempJobFolder = Paths.get(uri.getPath());
        this.catalogIOManager = catalogManager.getCatalogIOManagerFactory().get("file");
//        this.variantIndexOutputRecorder = new VariantIndexOutputRecorder(catalogManager, catalogIOManager, sessionId);
    }

    @Override
    public void run() {

        IJobManager jobManager = catalogManager.getJobManager();

        Query runningJobsQuery = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING)
                .append(JobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);

        Query queuedJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED);
        queuedJobsQuery.put(JobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);

        Query preparedJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.PREPARED);
        preparedJobsQuery.put(JobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);

        // Sort jobs by creation date
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key())
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

        // Sort jobs by creation date. Limit to 1 result
        QueryOptions queryOptionsLimit1 = new QueryOptions(queryOptions).append(QueryOptions.LIMIT, 1);

        int maxConcurrentIndexJobs = 1; // TODO: Read from configuration?

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- INDEX DAEMON -----", TimeUtils.getTimeMillis());

            /*
            RUNNING JOBS
             */
            try {
                QueryResult<Job> runningJobs = jobManager.get(runningJobsQuery, queryOptions, sessionId);
                logger.debug("Checking running jobs. {} running jobs found", runningJobs.getNumResults());
                for (Job job : runningJobs.getResult()) {
                    checkRunningJob(job);
                }
            } catch (CatalogException e) {
                logger.warn("Cannot obtain running jobs");
                e.printStackTrace();
            }


            /*
            QUEUED JOBS
             */
            try {
                QueryResult<Job> queuedJobs = jobManager.get(queuedJobsQuery, queryOptions, sessionId);
                logger.debug("Checking queued jobs. {} queued jobs found", queuedJobs.getNumResults());
                for (Job job : queuedJobs.getResult()) {
                    checkQueuedJob(job);
                }
            } catch (CatalogException e) {
                logger.warn("Cannot obtain queued jobs");
                e.printStackTrace();
            }


            /*
            PREPARED JOBS
             */
            try {
                QueryResult<Job> preparedJobs = jobManager.get(preparedJobsQuery, queryOptionsLimit1, sessionId);
                if (preparedJobs != null && preparedJobs.getNumResults() > 0) {
                    if (getRunningOrQueuedJobs() < maxConcurrentIndexJobs) {
                        queuePreparedIndex(preparedJobs.first());
                    } else {
                        logger.debug("Too many jobs indexing now, waiting for indexing new jobs");
                    }
                }
            } catch (CatalogException e) {
                logger.warn("Cannot obtain prepared jobs");
                e.printStackTrace();
            }

        }
    }

    private void checkRunningJob(Job job) {
        Path tmpOutdirPath = getJobTemporaryFolder(job.getId());
        Job.JobStatus jobStatus;

        ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, this.sessionId);
        if (!tmpOutdirPath.toFile().exists()) {
            jobStatus = new Job.JobStatus(Job.JobStatus.ERROR, "Temporal output directory not found");
            try {
                logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.RUNNING, jobStatus.getName());
                outputRecorder.updateJobStatus(job, jobStatus);
            } catch (CatalogException e) {
                logger.warn("Could not update job {} to status error", job.getId());
            } finally {
                closeSessionId(job);
            }
        } else {
            String status = executorManager.status(tmpOutdirPath, job);
            if (!status.equalsIgnoreCase(Job.JobStatus.UNKNOWN) && !status.equalsIgnoreCase(Job.JobStatus.RUNNING)) {
//                variantIndexOutputRecorder.registerStorageETLResults(job, tmpOutdirPath);
                logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.RUNNING, status);
                try {
//                    outputRecorder.recordJobOutputAndPostProcess(job, status);
                    // TODO: Should this copy the output files?
//                    outputRecorder.recordJobOutput;
                    outputRecorder.updateJobStatus(job, new Job.JobStatus(status));
                    logger.info("Removing temporal directory.");
                    this.catalogIOManager.deleteDirectory(UriUtils.createUri(tmpOutdirPath.toString()));
                } catch (CatalogException | URISyntaxException e) {
                    logger.error(e.getMessage());
                } finally {
                    closeSessionId(job);
                }
            }

//            Path jobStatusFile = tmpOutdirPath.resolve(JOB_STATUS_FILE);
//            if (jobStatusFile.toFile().exists()) {
//                try {
//                    jobStatus = objectReader.readValue(jobStatusFile.toFile());
//                } catch (IOException e) {
//                    logger.warn("Could not read job status file.");
//                    return;
//                    // TODO: Add a maximum number of attempts....
//                }
//                if (jobStatus != null && !jobStatus.getName().equalsIgnoreCase(Job.JobStatus.RUNNING)) {
//                    String sessionId = (String) job.getResourceManagerAttributes().get("sessionId");
//                    ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, sessionId);
//                    try {
//                        outputRecorder.recordJobOutputAndPostProcess(job, jobStatus);
//
//                    } catch (CatalogException | IOException e) {
//                        logger.error(e.getMessage());
//                    }
//                }
//            } else {
//                // TODO: Call the executor status
//                logger.debug("Call executor status not yet implemented.");
////                    executorManager.status(job).equalsIgnoreCase()
//            }
        }
    }

    private void checkQueuedJob(Job job) {

        Path tmpOutdirPath = getJobTemporaryFolder(job.getId());
        if (!tmpOutdirPath.toFile().exists()) {
            logger.warn("Attempting to create the temporal output directory again");
            try {
                catalogIOManager.createDirectory(tmpOutdirPath.toUri());
            } catch (CatalogIOException e) {
                logger.error("Could not create the temporal output directory to run the job");
            }
        } else {
            String status = executorManager.status(tmpOutdirPath, job);
            if (!status.equalsIgnoreCase(Job.JobStatus.UNKNOWN) && !status.equalsIgnoreCase(Job.JobStatus.QUEUED)) {
                try {
                    logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.QUEUED, Job.JobStatus.RUNNING);
                    catalogManager.getJobManager()
                            .setStatus(Long.toString(job.getId()), Job.JobStatus.RUNNING, "The job is running", sessionId);
                } catch (CatalogException e) {
                    logger.warn("Could not update job {} to status running", job.getId());
                }
            }
//
//            Path jobStatusFile = tmpOutdirPath.resolve(JOB_STATUS_FILE);
//            if (jobStatusFile.toFile().exists()) {
//                Job.JobStatus jobStatus = null;
//                try {
//                    jobStatus = objectReader.readValue(jobStatusFile.toFile());
//                } catch (IOException e) {
//                    logger.warn("Could not read job status file.");
//                    // TODO: Add a maximum number of attempts....
//                }
//                if (jobStatus != null && !jobStatus.getName().equalsIgnoreCase(Job.JobStatus.QUEUED)) {
//                    ObjectMap objectMap = new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING);
//                    try {
//                        catalogManager.getJobManager().update(job.getId(), objectMap, new QueryOptions(), sessionId);
//                    } catch (CatalogException e) {
//                        logger.warn("Could not update job {} to status running", job.getId());
//                    }
//                }
//            } else {
//                String status = executorManager.status(job);
//                if (!status.equalsIgnoreCase(Job.JobStatus.QUEUED)) {
//                    ObjectMap objectMap = new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING);
//                    try {
//                        catalogManager.getJobManager().update(job.getId(), objectMap, new QueryOptions(), sessionId);
//                    } catch (CatalogException e) {
//                        logger.warn("Could not update job {} to status running", job.getId());
//                    }
//                }
//            }
        }
    }

    private void queuePreparedIndex(Job job) {
        // Create the temporal output directory.
        Path path = getJobTemporaryFolder(job.getId());
        try {
            catalogIOManager.createDirectory(path.toUri());
        } catch (CatalogIOException e) {
            logger.warn("Could not create the temporal output directory {} to run the job", path);
            return;
            // TODO: Maximum attemps ... -> Error !
        }

        // Defined where the stdout and stderr will be stored
        String stderr = path.resolve(job.getName() + "_" + job.getId() + ".err").toString();
        String stdout = path.resolve(job.getName() + "_" + job.getId() + ".out").toString();

        // Obtain a new session id for the user so we can guarantee the session will be open during execution.
        String userId = job.getUserId();
        String userSessionId = null;
        try {
            userSessionId = catalogManager.getUserManager().getNewUserSession(sessionId, userId).first().getId();
        } catch (CatalogException e) {
            logger.warn("Could not obtain a new session id for user {}. Error: {}", userId, e.getMessage());
        }
        job.getParams().put("sid", userSessionId);

        // TODO: This command line could be created outside this class
        // Build the command line.
        StringBuilder commandLine = new StringBuilder(binHome).append(job.getExecutable());

        // we assume job.output equals params.outdir
        job.getParams().put("outdir", path.toString());

        if (job.getAttributes().get(INDEX_TYPE).toString().equalsIgnoreCase(VARIANT_TYPE)) {
            job.getParams().put("path", Long.toString(job.getOutDirId()));

            commandLine.append(" variant index");
            Set<String> knownParams = new HashSet<>(Arrays.asList(
                    "aggregated", "aggregation-mapping-file", "annotate", "annotator", "bgzip", "calculate-stats",
                    "exclude-genotypes", "file-id", "gvcf", "h", "help", "include-extra-fields", "load", "log-file",
                    "L", "log-level", "o", "outdir", "overwrite-annotations", "path", "queue", "s", "study", "S", "sid", "session-id",
                    "transform", "transformed-files"));
            for (Map.Entry<String, String> param : job.getParams().entrySet()) {
                commandLine.append(' ');
                if (knownParams.contains(param.getKey())) {
                    commandLine.append("--").append(param.getKey());
                    if (!param.getValue().equalsIgnoreCase("true")) {
                        commandLine.append(" ").append(param.getValue());
                    }
                } else {
                    if (!param.getKey().startsWith("-D")) {
                        commandLine.append("-D");
                    }
                    commandLine.append(param.getKey()).append('=').append(param.getValue());
                }
            }
        } else {
            commandLine.append(" alignment index");
            for (Map.Entry<String, String> param : job.getParams().entrySet()) {
                commandLine.append(' ');
                commandLine.append("--").append(param.getKey());
                if (!param.getValue().equalsIgnoreCase("true")) {
                    commandLine.append(" ").append(param.getValue());
                }
            }
        }

        logger.info("Updating job CLI '{}' from '{}' to '{}'", commandLine.toString(), Job.JobStatus.PREPARED, Job.JobStatus.QUEUED);

        try {
            catalogManager.getJobManager().setStatus(Long.toString(job.getId()), Job.JobStatus.QUEUED,
                    "The job is in the queue waiting to be executed", sessionId);
//            Job.JobStatus jobStatus = new Job.JobStatus(Job.JobStatus.QUEUED, "The job is in the queue waiting to be executed");
//            updateObjectMap.put(JobDBAdaptor.QueryParams.STATUS.key(), jobStatus);
            ObjectMap updateObjectMap = new ObjectMap();
            updateObjectMap.put(JobDBAdaptor.QueryParams.COMMAND_LINE.key(), commandLine.toString());
            job.getAttributes().put("sessionId", userSessionId);

            updateObjectMap.put(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), job.getAttributes());

            job.getResourceManagerAttributes().put(AbstractExecutor.STDOUT, stdout);
            job.getResourceManagerAttributes().put(AbstractExecutor.STDERR, stderr);
            job.getResourceManagerAttributes().put(AbstractExecutor.OUTDIR, path.toString());
            updateObjectMap.put(JobDBAdaptor.QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key(), job.getResourceManagerAttributes());

            QueryResult<Job> update = catalogManager.getJobManager().update(job.getId(), updateObjectMap, new QueryOptions(), sessionId);
            if (update.getNumResults() == 1) {
                executorManager.execute(update.first());
            } else {
                logger.error("Could not update nor run job {}" + job.getId());
            }
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Path getJobTemporaryFolder(long jobId) {
        return getJobTemporaryFolder(jobId, tempJobFolder);
    }

    public static Path getJobTemporaryFolder(long jobId, Path tempJobFolder) {
        return tempJobFolder.resolve(getJobTemporaryFolderName(jobId));
    }

    public static Path getJobTemporaryFolder(long jobId, String tempJobFolder) {
        URI uri = URI.create(tempJobFolder);
        return Paths.get(uri.getPath()).resolve(getJobTemporaryFolderName(jobId));
    }

    public static String getJobTemporaryFolderName(long jobId) {
        return "J_" + jobId;
    }

    private long getRunningOrQueuedJobs() throws CatalogException {
        Query runningJobsQuery = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Arrays.asList(Job.JobStatus.RUNNING, Job.JobStatus.QUEUED))
                .append(JobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);
        return catalogManager.getJobManager().get(runningJobsQuery, QueryOptions.empty(), sessionId).getNumTotalResults();
    }

    private void closeSessionId(Job job) {
        String sessionId = ((String) job.getAttributes().get("sessionId"));

        String userId;
        try {
            userId = catalogManager.getUserManager().getId(sessionId);
            catalogManager.getUserManager().logout(userId, sessionId);
        } catch (CatalogException e) {
            logger.error("An error occurred when trying to close the session id: {}", e.getMessage());
        } finally {
            // Remove the session id from the job attributes
            job.getAttributes().remove("sessionId");
            ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), job.getAttributes());
            try {
                catalogManager.getJobManager().update(job.getId(), params, new QueryOptions(), this.sessionId);
            } catch (CatalogException e) {
                logger.error("Could not remove session id from attributes of job {}. {}", job.getId(), e.getMessage());
            }
        }
    }
}
