/*
 * Copyright 2015 OpenCB
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

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.api.IJobManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.ExecutionOutputRecorder;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.opencb.opencga.catalog.monitor.executors.ParentExecutor;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by imedina on 18/08/16.
 */
public class IndexDaemon extends MonitorParentDaemon {

    public static final String INDEX_TYPE = "INDEX_TYPE";
    public static final String ALIGNMENT_TYPE = "ALIGNMENT";
    public static final String VARIANT_TYPE = "VARIANT";

    private CatalogIOManager catalogIOManager;
    private String binHome;

    private ObjectMapper objectMapper;
    private ObjectReader objectReader;

    public IndexDaemon(int interval, String sessionId, CatalogManager catalogManager, String appHome) {
        super(interval, sessionId, catalogManager);
        this.binHome = appHome + "/bin/";
    }

    @Override
    public void run() {

        IJobManager jobManager = catalogManager.getJobManager();

        // TODO: Sort results by date
        Query runningJobsQuery = new Query(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING);
        runningJobsQuery.put(CatalogJobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);

        Query queuedJobsQuery = new Query(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED);
        queuedJobsQuery.put(CatalogJobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);

        Query preparedJobsQuery = new Query(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.PREPARED);
        preparedJobsQuery.put(CatalogJobDBAdaptor.QueryParams.TYPE.key(), Job.Type.INDEX);

        QueryOptions queryOptions = new QueryOptions();

        objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader(Job.JobStatus.class);

        int numRunningJobs = 0;

        try {
            catalogIOManager = catalogManager.getCatalogIOManagerFactory().get("file");
        } catch (CatalogIOException e) {
            exit = true;
            e.printStackTrace();
        }


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
                QueryResult<Job> runningJobs = jobManager.readAll(runningJobsQuery, queryOptions, sessionId);
                numRunningJobs = runningJobs.getNumResults();
                logger.debug("Checking running jobs. {} running jobs found", runningJobs.getNumResults());
                for (Job job : runningJobs.getResult()) {
                    checkRunningJob(job);
                }
            } catch (CatalogException e) {
                e.printStackTrace();
            }


            /*
            QUEUED JOBS
             */
            try {
                QueryResult<Job> queuedJobs = jobManager.readAll(queuedJobsQuery, queryOptions, sessionId);
                logger.debug("Checking queued jobs. {} running jobs found", queuedJobs.getNumResults());
                for (Job job : queuedJobs.getResult()) {
                    checkQueuedJob(job);
                }
            } catch (CatalogException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }


            /*
            PREPARED JOBS
             */
            try {
                queryOptions.put(QueryOptions.LIMIT, 1);
//                queryOptions.put(QueryOptions.SORT, CatalogJobDBAdaptor.QueryParams.CREATION_DATE.key());
                QueryResult<Job> preparedJobs = jobManager.readAll(preparedJobsQuery, queryOptions, sessionId);
                if (preparedJobs != null && preparedJobs.getNumResults() > 0) {
                    if (numRunningJobs <= 2) {
                        queuePreparedIndex(preparedJobs.first());
                    } else {
                        logger.debug("Too many jobs indexing now, waiting for indexing new jobs");
                    }
                }
            } catch (CatalogException e) {
                e.printStackTrace();
            }

        }
    }

    private void checkRunningJob(Job job) {
        logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.RUNNING, Job.JobStatus.READY);

        try {
            Path tmpOutdirPath = Paths.get(catalogManager.getCatalogConfiguration().getTempJobsDir(), "J_" + job.getId());
            Job.JobStatus jobStatus;

            if (!tmpOutdirPath.toFile().exists()) {
                ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, this.sessionId);
                jobStatus = new Job.JobStatus(Job.JobStatus.ERROR, "Temporal output directory not found");
                outputRecorder.updateJobStatus(job, jobStatus);
            } else {
                Path jobStatusFile = tmpOutdirPath.resolve("job.status");
                if (jobStatusFile.toFile().exists()) {
                    jobStatus = objectReader.readValue(jobStatusFile.toFile());
                    if (!jobStatus.getName().equalsIgnoreCase(Job.JobStatus.RUNNING)) {
                        String sessionId = (String) job.getResourceManagerAttributes().get("sessionId");
                        ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, sessionId);
                        outputRecorder.recordJobOutputAndPostProcess(job, jobStatus);
                    }
                } else {
                    // TODO: Call the executor status
                    logger.debug("Not yet implemented.");
//                    executorManager.status(job).equalsIgnoreCase()
                }
            }
        } catch (JsonProcessingException e) {
            // TODO: Error status
            e.printStackTrace();
        } catch (IOException e) {
            // TODO: Support up to a number of attempts
            e.printStackTrace();
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage());
            e.printStackTrace();
        }
    }

    private void checkQueuedJob(Job job) throws ExecutionException {
        logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.QUEUED, Job.JobStatus.RUNNING);
        try {
            Path tmpOutdirPath = Paths.get(catalogManager.getCatalogConfiguration().getTempJobsDir(), "J_" + job.getId());
            if (!tmpOutdirPath.toFile().exists()) {
                logger.warn("Attempting to create the temporal output directory again");
                catalogIOManager.createDirectory(tmpOutdirPath.toUri());
            } else {
                Path jobStatusFile = Paths.get(catalogManager.getCatalogConfiguration().getTempJobsDir(), "J_" + job.getId(), "job.status");
                if (jobStatusFile.toFile().exists()) {
                    Job.JobStatus jobStatus = objectReader.readValue(jobStatusFile.toFile());
                    if (!jobStatus.getName().equalsIgnoreCase(Job.JobStatus.QUEUED)) {
                        ObjectMap objectMap = new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING);
                        catalogManager.getJobManager().update(job.getId(), objectMap, QueryOptions.empty(), sessionId);
                    }
                } else {
                    String status = executorManager.status(job);
                    if (!status.equalsIgnoreCase(Job.JobStatus.QUEUED)) {
                        ObjectMap objectMap = new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING);
                        catalogManager.getJobManager().update(job.getId(), objectMap, QueryOptions.empty(), sessionId);
                    }
                }
            }

        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage());
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            logger.warn("job.status file damaged.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void queuePreparedIndex(Job job) throws CatalogException {

        // Create the temporal output directory.
        Path path = Paths.get(catalogManager.getCatalogConfiguration().getTempJobsDir(), "J_" + job.getId());
        catalogIOManager.createDirectory(path.toUri());

        // Defined where the stdout and stderr will be stored
        String stderr = path.resolve(job.getName() + "_" + job.getId() + ".err").toString();
        String stdout = path.resolve(job.getName() + "_" + job.getId() + ".out").toString();

        // Obtain a new session id for the user so we can guarantee the session will be open during execution.
        String userId = job.getUserId();
        String userSessionId = catalogManager.getUserManager().getNewUserSession(sessionId, userId).first().getString("sessionId");
        job.getParams().put("sid", userSessionId);

        // TODO: This command line could be created outside this class
        // Build the command line.
        StringBuilder commandLine = new StringBuilder(binHome).append(job.getExecutable());

        if (job.getAttributes().get(INDEX_TYPE).toString().equalsIgnoreCase(VARIANT_TYPE)) {
            commandLine.append(" variant index");
        } else {
            commandLine.append(" alignment index");
        }

        // we assume job.output equals params.outdir
        job.getParams().put("outdir", path.toString());
        for (Map.Entry<String, String> param : job.getParams().entrySet()) {
            commandLine.append(" ")
                    .append("--").append(param.getKey());
                    if (!param.getValue().equalsIgnoreCase("true")) {
                        commandLine.append(" ").append(param.getValue());
                    }
        }

        logger.info("Updating job CLI '{}' from '{}' to '{}'", commandLine.toString(), Job.JobStatus.PREPARED, Job.JobStatus.QUEUED);

        try {
            ObjectMap updateObjectMap = new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED);
            updateObjectMap.put(CatalogJobDBAdaptor.QueryParams.COMMAND_LINE.key(), commandLine.toString());
            // TODO: Remove the line below
            job.getAttributes().put(ExecutorManager.TMP_OUT_DIR, path.toString());
            updateObjectMap.put(CatalogJobDBAdaptor.QueryParams.ATTRIBUTES.key(), job.getAttributes());

            job.getResourceManagerAttributes().put("sessionId", userSessionId);
            job.getResourceManagerAttributes().put(ParentExecutor.STDOUT, stdout);
            job.getResourceManagerAttributes().put(ParentExecutor.STDERR, stderr);
            updateObjectMap.put(CatalogJobDBAdaptor.QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key(), job.getResourceManagerAttributes());

//            updateObjectMap.put(CatalogJobDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);

            QueryResult<Job> update = catalogManager.getJobManager().update(job.getId(), updateObjectMap, new QueryOptions(), sessionId);
            if (update.getNumResults() == 1) {
//                executorManager.run(update.first());
                Runnable runnable = () -> {
                    try {
                        // TODO: Return job and modify job
                        executorManager.execute(update.first());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
                Thread thread = new Thread(runnable);
                thread.start();
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
}
