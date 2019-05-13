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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.executors.BatchExecutor;
import org.opencb.opencga.catalog.monitor.executors.ExecutorFactory;
import org.opencb.opencga.core.models.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 16/06/16.
 */
public abstract class MonitorParentDaemon implements Runnable {

    protected int interval;
    protected CatalogManager catalogManager;
    // FIXME: This should not be used directly! All the queries MUST go through the CatalogManager
    @Deprecated
    protected DBAdaptorFactory dbAdaptorFactory;
    protected BatchExecutor batchExecutor;

    protected boolean exit = false;

    protected String sessionId;

    protected Logger logger;

    public MonitorParentDaemon(int interval, String sessionId, CatalogManager catalogManager) throws CatalogDBException {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
        logger = LoggerFactory.getLogger(this.getClass());
        dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        ExecutorFactory executorFactory = new ExecutorFactory(catalogManager.getConfiguration());
        this.batchExecutor = executorFactory.getExecutor();
    }

    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    static Path getJobTemporaryFolder(long jobId, Path tempJobFolder) {
        return tempJobFolder.resolve(getJobTemporaryFolderName(jobId));
    }

    static String getJobTemporaryFolderName(long jobId) {
        return "J_" + jobId;
    }

    void executeJob(Job job, String token) {
        try {
            batchExecutor.execute(job, token);
        } catch (Exception e) {
            logger.error("Error executing job {}.", job.getUid(), e);
        }
    }

    void checkQueuedJob(Job job, Path tempJobFolder, CatalogIOManager catalogIOManager) {

        Path tmpOutdirPath = getJobTemporaryFolder(job.getUid(), tempJobFolder);
        if (!tmpOutdirPath.toFile().exists()) {
            logger.warn("Attempting to create the temporal output directory again");
            try {
                catalogIOManager.createDirectory(tmpOutdirPath.toUri());
            } catch (CatalogIOException e) {
                logger.error("Could not create the temporal output directory to run the job");
            }
        } else {
            String status = batchExecutor.status(tmpOutdirPath, job);
            if (!status.equalsIgnoreCase(Job.JobStatus.UNKNOWN) && !status.equalsIgnoreCase(Job.JobStatus.QUEUED)) {
                try {
                    logger.info("Updating job {} from {} to {}", job.getUid(), Job.JobStatus.QUEUED, Job.JobStatus.RUNNING);
                    setNewStatus(job.getUid(), Job.JobStatus.RUNNING, "The job is running");
                } catch (CatalogException e) {
                    logger.warn("Could not update job {} to status running", job.getUid());
                }
            }
        }
    }

    void setNewStatus(long jobId, String status, String message) throws CatalogDBException {
        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_MSG.key(), message);
        dbAdaptorFactory.getCatalogJobDBAdaptor().update(jobId, parameters, QueryOptions.empty());
    }

    void cleanPrivateJobInformation(Job job) {
        // Remove the session id from the job attributes
        job.getAttributes().remove(Job.OPENCGA_TMP_DIR);
        job.getAttributes().remove(Job.OPENCGA_OUTPUT_DIR);
        job.getAttributes().remove(Job.OPENCGA_STUDY);

        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), job.getAttributes());
        try {
            dbAdaptorFactory.getCatalogJobDBAdaptor().update(job.getUid(), params, QueryOptions.empty());
        } catch (CatalogException e) {
            logger.error("Could not remove session id from attributes of job {}. ", job.getUid(), e);
        }
    }

    void buildCommandLine(Map<String, String> params, StringBuilder commandLine, Set<String> knownParams) {
        for (Map.Entry<String, String> param : params.entrySet()) {
            if (param.getKey().equals("sid")) {
                continue;
            }
            commandLine.append(' ');
            if (knownParams == null || knownParams.contains(param.getKey())) {
                if (!("false").equalsIgnoreCase(param.getValue())) {
                    if (param.getKey().length() == 1) {
                        commandLine.append('-');
                    } else {
                        commandLine.append("--");
                    }
                    commandLine.append(param.getKey());
                    if (!param.getValue().equalsIgnoreCase("true")) {
                        commandLine.append(' ').append(param.getValue());
                    }
                }
            } else {
                if (!param.getKey().startsWith("-D")) {
                    commandLine.append("-D");
                }
                commandLine.append(param.getKey()).append('=').append(param.getValue());
            }
        }
    }

}
