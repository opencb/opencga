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

import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.catalog.monitor.executors.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 16/06/16.
 */
public abstract class MonitorParentDaemon implements Runnable {

    protected int interval;
    protected CatalogManager catalogManager;
    protected DBAdaptorFactory dbAdaptorFactory;
    protected AbstractExecutor executorManager;

    protected boolean exit = false;

    protected String sessionId;

    protected Logger logger;

    public MonitorParentDaemon(int interval, String sessionId, CatalogManager catalogManager) throws CatalogDBException {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
        logger = LoggerFactory.getLogger(this.getClass());

        configureDBAdaptor(catalogManager.getConfiguration());
        ExecutorManager executorFactory = new ExecutorManager(catalogManager.getConfiguration());
        this.executorManager = executorFactory.getExecutor();

//        if (catalogManager.getCatalogConfiguration().getExecution().getMode().equalsIgnoreCase("local")) {
//            this.executorManager = new LocalExecutorManager(catalogManager, sessionId);
//            logger.info("Jobs will be launched locally");
//        } else {
//            this.executorManager = new SgeExecutorManager(catalogManager, sessionId);
//            logger.info("Jobs will be launched to SGE");
//        }
    }

    private void configureDBAdaptor(Configuration configuration) throws CatalogDBException {

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", configuration.getCatalog().getDatabase().getUser())
                .add("password", configuration.getCatalog().getDatabase().getPassword())
                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
                .build();

        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : configuration.getCatalog().getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        dbAdaptorFactory = new MongoDBAdaptorFactory(dataStoreServerAddresses, mongoDBConfiguration,
                catalogManager.getCatalogDatabase()) {};
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

    void executeJob(Job job, QueryResult<Job> update) {
        if (update.getNumResults() == 1) {
            job = update.first();
            try {
                executorManager.execute(job);
            } catch (Exception e) {
                logger.error("Error executing job {}.", job.getId(), e);
            }
        } else {
            logger.error("Could not update nor run job {}" + job.getId());
        }
    }

    void checkQueuedJob(Job job, Path tempJobFolder, CatalogIOManager catalogIOManager) {

        Path tmpOutdirPath = getJobTemporaryFolder(job.getId(), tempJobFolder);
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
                    setNewStatus(job.getId(), Job.JobStatus.RUNNING, "The job is running");
                } catch (CatalogException e) {
                    logger.warn("Could not update job {} to status running", job.getId());
                }
            }
        }
    }

    void setNewStatus(long jobId, String status, String message) throws CatalogDBException {
        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(JobDBAdaptor.QueryParams.STATUS_MSG.key(), message);
        dbAdaptorFactory.getCatalogJobDBAdaptor().update(jobId, parameters);
    }

    void cleanPrivateJobInformation(Job job) {
        // Remove the session id from the job attributes
        job.getAttributes().remove(Job.OPENCGA_USER_TOKEN);
        job.getAttributes().remove(Job.OPENCGA_OUTPUT_DIR);
        job.getAttributes().remove(Job.OPENCGA_STUDY);

        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), job.getAttributes());
        try {
            dbAdaptorFactory.getCatalogJobDBAdaptor().update(job.getId(), params);
        } catch (CatalogException e) {
            logger.error("Could not remove session id from attributes of job {}. ", job.getId(), e);
        }
    }

    void buildCommandLine(Map<String, String> params, StringBuilder commandLine, Set<String> knownParams) {
        for (Map.Entry<String, String> param : params.entrySet()) {
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
