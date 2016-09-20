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

package org.opencb.opencga.app.daemon;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.monitor.ExecutionOutputRecorder;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.core.SgeManager;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by jacobo on 23/10/14.
 */
@Deprecated
public class DaemonLoop implements Runnable {

    public static final String PORT = "OPENCGA.APP.DAEMON.PORT";
    public static final String SLEEP = "OPENCGA.APP.DAEMON.SLEEP";
    public static final String USER = "OPENCGA.APP.DAEMON.USER";
    public static final String PASSWORD = "OPENCGA.APP.DAEMON.PASSWORD";
    public static final String DELETE_DELAY = "OPENCGA.APP.DAEMON.DELETE_DELAY";

    private final Properties properties;

    private Server server;
    private Thread thread;
    private boolean exit = false;
    private CatalogManager catalogManager;

    private static Logger logger = LoggerFactory.getLogger(DaemonLoop.class);
    private ExecutionOutputRecorder analysisOutputRecorder;
    private String sessionId;

    @Deprecated
    public DaemonLoop(Properties properties) {
        this.properties = properties;
        try {
            CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(new FileInputStream(Paths.get(Config.getOpenCGAHome(),
                    "conf", "catalog-configuration.yml").toFile()));
            catalogManager = new CatalogManager(catalogConfiguration);
        } catch (CatalogException | IOException e) {
            e.printStackTrace();
        }

//        analysisFileIndexer = new AnalysisFileIndexer(catalogManager, Config.getAnalysisProperties());

        int port = Integer.parseInt(properties.getProperty(DaemonLoop.PORT, "61976"));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(true, "org.opencb.opencga.app.daemon.rest");
        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder(sc);

        logger.info("Server in port : {}", port);
        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/rest/*");

        thread = new Thread(this);
    }


    @Override
    public void run() {
        int sleep = Integer.parseInt(properties.getProperty(SLEEP, "4000"));
        sessionId = null;
        try {
            QueryResult<ObjectMap> login = catalogManager.login(properties.getProperty(USER), properties.getProperty(PASSWORD), "daemon");
            sessionId = login.getResult().get(0).getString("sessionId");
        } catch (CatalogException | IOException e) {
            e.printStackTrace();
            exit = true;
        }
        analysisOutputRecorder = new ExecutionOutputRecorder(catalogManager, sessionId);

        while (!exit) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- WakeUp {} -----", TimeUtils.getTimeMillis());

            logger.info("----- Pending jobs -----");
            try {
                QueryResult<Job> unfinishedJobs = catalogManager.getUnfinishedJobs(sessionId);
                for (Job job : unfinishedJobs.getResult()) {
                    String status = null;
                    try {
                        status = SgeManager.status(job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME).toString());
                    } catch (Exception e) {
                        logger.warn(e.getMessage());
                    }
                    String jobStatusEnum = job.getStatus().getName();
//                    String type = job.getResourceManagerAttributes().get(Job.TYPE).toString();
//                    System.out.println("job : {id: " + job.getId() + ", status: '" + job.getName() + "', name: '" + job.getName() + "'}, sgeStatus : " + status);
                    logger.info("job : {id: " + job.getId() + ", status: '" + job.getStatus().getName() + "', name: '" + job.getName() + "'}, sgeStatus : " + status);

                    //Track SGEManager
                    if (status != null) {
                        switch (status) {
                            case SgeManager.FINISHED:
                                if (!Job.JobStatus.DONE.equals(job.getStatus().getName())) {
                                    catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.JobStatus.DONE), sessionId);
                                    jobStatusEnum = Job.JobStatus.DONE;
                                }
                                break;
                            case SgeManager.ERROR:
                            case SgeManager.EXECUTION_ERROR:
                                if (!Job.JobStatus.DONE.equals(job.getStatus().getName())) {
                                    ObjectMap parameters = new ObjectMap();
                                    parameters.put("status", Job.JobStatus.DONE);
                                    String error = Job.ERRNO_FINISH_ERROR;
                                    parameters.put("error", error);
                                    parameters.put("errorDescription", Job.ERROR_DESCRIPTIONS.get(error));
                                    catalogManager.modifyJob(job.getId(), parameters, sessionId);
                                    jobStatusEnum = Job.JobStatus.DONE;
                                    job.setError(error);
                                }
                                break;
                            case SgeManager.QUEUED:
                                if (!Job.JobStatus.QUEUED.equals(job.getStatus().getName())) {
                                    catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.JobStatus.QUEUED), sessionId);
                                    jobStatusEnum = Job.JobStatus.QUEUED;
                                }
                                break;
                            case SgeManager.RUNNING:
                                if (!Job.JobStatus.RUNNING.equals(job.getStatus().getName())) {
                                    catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.JobStatus.RUNNING), sessionId);
                                    jobStatusEnum = Job.JobStatus.RUNNING;
                                }
                                break;
                            case SgeManager.TRANSFERRED:
                                break;
                            case SgeManager.UNKNOWN:
                                break;
                        }
                    }

                    //Track Catalog Job status
                    switch (jobStatusEnum) {
                        case Job.JobStatus.DONE:
                            boolean jobOk = job.getError() == null || (job.getError() != null && job.getError().isEmpty());
                            analysisOutputRecorder.recordJobOutputAndPostProcess(job, !jobOk);
                            if (jobOk) {
                                catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.JobStatus.READY), sessionId);
                            } else {
                                catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.JobStatus.ERROR), sessionId);
                            }
                            break;
                        case Job.JobStatus.PREPARED:
                            try {
                                ExecutorManager.execute(catalogManager, job, sessionId);
                            } catch (ExecutionException e) {
                                ObjectMap params = new ObjectMap("status", Job.JobStatus.ERROR);
                                String error = Job.ERRNO_NO_QUEUE;
                                params.put("error", error);
                                params.put("errorDescription", Job.ERROR_DESCRIPTIONS.get(error));
                                catalogManager.modifyJob(job.getId(), params, sessionId);
                            }
                            break;
                        case Job.JobStatus.QUEUED:
                            break;
                        case Job.JobStatus.RUNNING:
                            break;
                        case Job.JobStatus.ERROR:
                        case Job.JobStatus.READY:
                            //Never expected!
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            logger.info("----- Pending deletions -----");
            try {
                QueryResult<File> files = catalogManager.searchFile(-1, new Query(FileDBAdaptor.QueryParams.FILE_STATUS.key(),
                        File.FileStatus.TRASHED), new QueryOptions(), sessionId);
                long currentTimeMillis = System.currentTimeMillis();
                for (File file : files.getResult()) {
                    try {       //TODO: skip if the file is a non-empty folder
                        long deleteDate = new ObjectMap(file.getAttributes()).getLong("deleteDate", 0);
                        if (currentTimeMillis - deleteDate > Long.valueOf(properties.getProperty(DELETE_DELAY, "30")) * 1000) { //Seconds to millis
                            QueryResult<Study> studyQueryResult = catalogManager.getStudy(catalogManager.getStudyIdByFileId(file.getId()), sessionId);
                            Study study = studyQueryResult.getResult().get(0);
                            logger.info("Deleting file {} from study {id: {}, alias: {}}", file, study.getId(), study.getAlias());
                            new CatalogFileUtils(catalogManager).delete(file, sessionId);
                        } else {
                            logger.info("Don't delete file {id: {}, path: '{}', attributes: {}}}", file.getId(), file.getPath(), file.getAttributes());
                            logger.info("{}", (currentTimeMillis - deleteDate) / 1000);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (sessionId != null) {
            try {
                catalogManager.logout(properties.getProperty(USER), sessionId);
            } catch (CatalogException e) {
                e.printStackTrace();
            }
            sessionId = null;
        }

        try {
            Thread.sleep(200);
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() throws Exception {
        //Start services
        server.start();
        thread.start();
    }

    public int join() {
        //Join services
        try {
            logger.info("Join to Server");
            server.join();
            logger.info("Join to Thread");
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 2;
        }
        return 0;
    }

    synchronized public void stop() {
        exit = true;
        thread.interrupt();
    }
}
