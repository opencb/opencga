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

package org.opencb.opencga.catalog.monitor.daemons;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.ExecutionOutputRecorder;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 16/06/16.
 */
public class ExecutionDaemon extends MonitorParentDaemon {

    private int runningJobs;
    private String binAnalysis;
    private Path tempJobFolder;

    private CatalogIOManager catalogIOManager;
    // FIXME: This should not be used directly! All the queries MUST go through the CatalogManager
    @Deprecated
    private JobDBAdaptor jobDBAdaptor;

    public ExecutionDaemon(int interval, String sessionId, CatalogManager catalogManager, String appHome)
            throws CatalogDBException, URISyntaxException, CatalogIOException {
        super(interval, sessionId, catalogManager);
        URI uri = UriUtils.createUri(catalogManager.getConfiguration().getTempJobsDir());
        this.tempJobFolder = Paths.get(uri.getPath());
        this.catalogIOManager = catalogManager.getCatalogIOManagerFactory().get("file");
        this.binAnalysis = appHome + "/bin/opencga-analysis.sh";
        this.jobDBAdaptor = dbAdaptorFactory.getCatalogJobDBAdaptor();
    }

    @Override
    public void run() {

        Query runningJobsQuery = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING)
                .append(JobDBAdaptor.QueryParams.TYPE.key(), "!=" + Job.Type.INDEX);
        Query queuedJobsQuery = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED)
                .append(JobDBAdaptor.QueryParams.TYPE.key(), "!=" + Job.Type.INDEX);
        Query preparedJobsQuery = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.PREPARED)
                .append(JobDBAdaptor.QueryParams.TYPE.key(), "!=" + Job.Type.INDEX);
        // Sort jobs by creation date
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key())
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- EXECUTION DAEMON -----", TimeUtils.getTimeMillis());

            /*
            RUNNING JOBS
             */
            try {
                QueryResult<Long> count = jobDBAdaptor.count(runningJobsQuery);
                logger.debug("Checking running jobs. {} running jobs found", count.first());
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            try (DBIterator<Job> iterator = jobDBAdaptor.iterator(runningJobsQuery, queryOptions)) {
                while (iterator.hasNext()) {
                    checkRunningJob(iterator.next());
                }
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            /*
            QUEUED JOBS
             */
            try {
                QueryResult<Long> count = jobDBAdaptor.count(queuedJobsQuery);
                logger.debug("Checking queued jobs. {} jobs found", count.first());
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            try (DBIterator<Job> iterator = jobDBAdaptor.iterator(queuedJobsQuery, queryOptions)) {
                while (iterator.hasNext()) {
                    checkQueuedJob(iterator.next(), tempJobFolder, catalogIOManager);
                }
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            /*
            PREPARED JOBS
             */
            try {
                QueryResult<Long> count = jobDBAdaptor.count(preparedJobsQuery);
                logger.debug("Checking prepared jobs. {} jobs found", count.first());
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            try (DBIterator<Job> iterator = jobDBAdaptor.iterator(preparedJobsQuery, queryOptions)) {
                while (iterator.hasNext()) {
                    checkPreparedJob(iterator.next());
                }
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }
        }
    }

    private void checkRunningJob(Job job) throws CatalogIOException {
        Path tmpOutdirPath = getJobTemporaryFolder(job.getUid(), tempJobFolder);
        Job.JobStatus jobStatus;

        ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, this.sessionId);
        if (!tmpOutdirPath.toFile().exists()) {
            jobStatus = new Job.JobStatus(Job.JobStatus.ERROR, "Temporal output directory not found");
            try {
                logger.info("Updating job {} from {} to {}", job.getUid(), Job.JobStatus.RUNNING, jobStatus.getName());
                outputRecorder.updateJobStatus(job, jobStatus);
            } catch (CatalogException e) {
                logger.warn("Could not update job {} to status error", job.getUid(), e);
            } finally {
                cleanPrivateJobInformation(job);
            }
        } else {
            String status = executorManager.status(tmpOutdirPath, job);
            if (!status.equalsIgnoreCase(Job.JobStatus.UNKNOWN) && !status.equalsIgnoreCase(Job.JobStatus.RUNNING)) {
                logger.info("Updating job {} from {} to {}", job.getUid(), Job.JobStatus.RUNNING, status);
                try {
                    outputRecorder.updateJobStatus(job, new Job.JobStatus(status));
                } catch (CatalogException e) {
                    logger.error("{}", e.getMessage(), e);
                    return;
                }

                try {
                    String userToken = catalogManager.getUserManager().getSystemTokenForUser(job.getUserId(), sessionId);
                    outputRecorder.recordJobOutput(job, tmpOutdirPath, userToken);
                } catch (CatalogException | IOException e) {
                    logger.error("{}", e.getMessage(), e);
                    try {
                        outputRecorder.updateJobStatus(job, new Job.JobStatus(Job.JobStatus.ERROR));
                    } catch (CatalogException e1) {
                        logger.error("{}", e1.getMessage(), e1);
                    }
                } finally {
                    cleanPrivateJobInformation(job);
                }
            }
        }
    }

    private void checkPreparedJob(Job job) {
        // Create the temporal output directory.
        Path path = getJobTemporaryFolder(job.getUid(), tempJobFolder);
        try {
            catalogIOManager.createDirectory(path.toUri());
        } catch (CatalogIOException e) {
            logger.warn("Could not create the temporal output directory " + path + " to run the job", e);
            return;
            // TODO: Maximum attemps ... -> Error !
        }

        // Define where the stdout and stderr will be stored
        String stderr = path.resolve(job.getName() + '_' + job.getUid() + ".err").toString();
        String stdout = path.resolve(job.getName() + '_' + job.getUid() + ".out").toString();

        // Create token without expiration time for the user
        try {
            String userToken = catalogManager.getUserManager().getSystemTokenForUser(job.getUserId(), sessionId);

            StringBuilder commandLine = new StringBuilder(binAnalysis).append(' ');
            if (job.getToolId().equals("opencga-analysis")) {
                commandLine.append(job.getExecution()).append(' ');
            } else {
                commandLine.append("tools execute ");
            }
            commandLine.append("--job ").append(job.getUid()).append(' ');

            logger.info("Updating job {} from {} to {}", job.getUid(), Job.JobStatus.PREPARED, Job.JobStatus.QUEUED);
            try {
                job.getAttributes().put(Job.OPENCGA_TMP_DIR, path.toString());

                job.getResourceManagerAttributes().put(AbstractExecutor.STDOUT, stdout);
                job.getResourceManagerAttributes().put(AbstractExecutor.STDERR, stderr);
                job.getResourceManagerAttributes().put(AbstractExecutor.OUTDIR, path.toString());

                ObjectMap params = new ObjectMap()
                        .append(JobDBAdaptor.QueryParams.COMMAND_LINE.key(), commandLine.toString())
                        .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED)
                        .append(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), job.getAttributes())
                        .append(JobDBAdaptor.QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key(), job.getResourceManagerAttributes());

                QueryResult<Job> update = jobDBAdaptor.update(job.getUid(), params, QueryOptions.empty());
                if (update.getNumResults() == 1) {
                    job = update.first();
                    executeJob(job, userToken);
                } else {
                    logger.error("Could not update nor run job {}" + job.getUid());
                }
            } catch (CatalogException e) {
                logger.error("Could not update job {}. {}", job.getUid(), e.getMessage());
                e.printStackTrace();
            }

        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

}
