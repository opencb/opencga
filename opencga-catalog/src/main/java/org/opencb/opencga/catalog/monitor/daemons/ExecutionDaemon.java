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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.hpg.bigdata.analysis.tools.ToolManager;
import org.opencb.hpg.bigdata.analysis.tools.manifest.Param;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.api.IJobManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.TimeUtils;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by imedina on 16/06/16.
 */
public class ExecutionDaemon extends MonitorParentDaemon {

    private int runningJobs;

    private CatalogIOManager catalogIOManager;
    private ToolManager toolManager;
    private String binHome;

    public ExecutionDaemon(int interval, String sessionId, CatalogManager catalogManager, String appHome)
            throws URISyntaxException, CatalogIOException {
        super(interval, sessionId, catalogManager);
        this.binHome = appHome + "/bin/";
        this.catalogIOManager = catalogManager.getCatalogIOManagerFactory().get("file");
        try {
            this.toolManager = new ToolManager(Paths.get(catalogManager.getConfiguration().getToolDir()));
        } catch (AnalysisToolException e) {
            throw new IllegalArgumentException("Tool directory does not contain any tools");
        }
    }

    @Override
    public void run() {

        IJobManager jobManager = catalogManager.getJobManager();
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
                QueryResult<Job> runningJobs = jobManager.get(runningJobsQuery, queryOptions, sessionId);
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
                QueryResult<Job> queuedJobs = jobManager.get(queuedJobsQuery, queryOptions, sessionId);
                logger.debug("Checking queued jobs. {} running jobs found", queuedJobs.getNumResults());
                for (Job job : queuedJobs.getResult()) {
                    checkQueuedJob(job);
                }
            } catch (CatalogException e) {
                e.printStackTrace();
            }

            /*
            PREPARED JOBS
             */
            try {
                QueryResult<Job> preparedJobs = jobManager.get(preparedJobsQuery, queryOptions, sessionId);
                logger.debug("Checking prepared jobs. {} running jobs found", preparedJobs.getNumResults());
                for (Job job : preparedJobs.getResult()) {
                    checkPreparedJob(job);
                }
            } catch (CatalogException e) {
                e.printStackTrace();
            }


        }
    }

    private void checkRunningJob(Job job) {
        logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.RUNNING, Job.JobStatus.READY);

        try {
            catalogManager.getJobManager().setStatus(Long.toString(job.getId()), Job.JobStatus.READY, null, sessionId);
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage());
            e.printStackTrace();
        }
    }

    private void checkQueuedJob(Job job) {
        logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.QUEUED, Job.JobStatus.RUNNING);

        try {
            logger.info("Running job {}" + job.getName());
            catalogManager.getJobManager().setStatus(Long.toString(job.getId()), Job.JobStatus.RUNNING, null, sessionId);

            runTool(job);
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage());
            e.printStackTrace();
        }
    }

    private void checkPreparedJob(Job job) {
        logger.info("Updating job {}({}) from {} to {}", job.getName(), job.getId(), Job.JobStatus.PREPARED, Job.JobStatus.QUEUED);

        try {
            catalogManager.getJobManager().setStatus(Long.toString(job.getId()), Job.JobStatus.QUEUED, null, sessionId);
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage());
            e.printStackTrace();
        }

    }

    private void runTool(Job job) {
        // Create the temporal output directory.
        Path path = getJobTemporaryFolder(job.getId());
        try {
            catalogIOManager.createDirectory(path.toUri());
        } catch (CatalogIOException e) {
            logger.warn("Could not create the temporal output directory " + path + " to run the job", e);
            return;
            // TODO: Maximum attemps ... -> Error !
        }

        // Check output parameters and change the folder to the temporary folder.
        try {
            List<Param> outputParams = toolManager.getOutputParams(job.getToolName(), job.getExecution());
            int folderCount = 0;
            for (Param outputParam : outputParams) {
                if (outputParam.getDataType().equals(Param.Type.FOLDER)) {
                    folderCount += 1;
                    job.getParams().put(outputParam.getName(), path.toString());
                } else if (outputParam.getDataType().equals(Param.Type.FILE)) {
                    if (StringUtils.isNotEmpty(job.getParams().get(outputParam.getName()))) {
                        // It has been passed so we need to redirect it to the output folder
                        String outputFileName = Paths.get(job.getParams().get(outputParam.getName())).getFileName().toString();
                        job.getParams().put(outputParam.getName(), path.resolve(outputFileName).toString());
                    }
                }
            }
            if (folderCount > 1) {
                logger.error("More than one output folder detected. Nothing to do.");
                return;
            }
        } catch (AnalysisToolException e) {
            logger.error(e.getMessage(), e);
            return;
        }
        // Update the params map from the job entry
        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.PARAMS.key(), job.getParams());
        try {
            QueryResult<Job> update = catalogManager.getJobManager().update(job.getId(), params, QueryOptions.empty(), sessionId);
            if (update.getNumResults() == 1) {
                job = update.first();
                try {
                    // Send the command line to the executor
                    String commandLine = binHome + "opencga-analysis.sh tools run --job " + job.getId();
                    executorManager.execute(job, commandLine);
                } catch (Exception e) {
                    logger.error("Error executing job {}.", job.getId(), e);
                }
            } else {
                logger.error("Could not update nor run job {}" + job.getId());
            }
        } catch (CatalogException e) {
            logger.error("Could not update job {}.", job.getId(), e);
        }


    }

}
