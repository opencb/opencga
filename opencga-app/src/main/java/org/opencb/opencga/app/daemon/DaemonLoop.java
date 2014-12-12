package org.opencb.opencga.app.daemon;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.SgeManager;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jacobo on 23/10/14.
 */
public class DaemonLoop implements Runnable {

    public static final String PORT     = "OPENCGA.APP.DAEMON.PORT";
    public static final String SLEEP    = "OPENCGA.APP.DAEMON.SLEEP";
    public static final String USER     = "OPENCGA.APP.DAEMON.USER";
    public static final String PASSWORD = "OPENCGA.APP.DAEMON.PASSWORD";
    public static final String DELETE_DELAY = "OPENCGA.APP.DAEMON.DELETE_DELAY";

    private final Properties properties;

    private Server server;
    private Thread thread;
    private boolean exit = false;
    private CatalogManager catalogManager;

    private static Logger logger = LoggerFactory.getLogger(DaemonLoop.class);
    private AnalysisOutputRecorder analysisOutputRecorder;
    private String sessionId;

    public DaemonLoop(Properties properties) {
        this.properties = properties;
        try {
            catalogManager = new CatalogManager(Config.getCatalogProperties());
        } catch (IOException | CatalogIOManagerException | CatalogDBException e) {
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
        } catch (CatalogDBException | IOException e) {
            e.printStackTrace();
            exit = true;
        }
        analysisOutputRecorder = new AnalysisOutputRecorder(catalogManager, sessionId);

        while(!exit) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                if(!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- WakeUp {} -----", TimeUtils.getTimeMillis());

            logger.info("----- Pending jobs -----");
            try {
                QueryResult<Job> unfinishedJobs = catalogManager.getUnfinishedJobs(sessionId);
                for (Job job : unfinishedJobs.getResult()) {
                    String status = SgeManager.status(job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME).toString());
                    String jobStatus = job.getStatus();
//                    String type = job.getResourceManagerAttributes().get(Job.TYPE).toString();
//                    System.out.println("job : {id: " + job.getId() + ", status: '" + job.getStatus() + "', name: '" + job.getName() + "'}, sgeStatus : " + status);
                    logger.info("job : {id: " + job.getId() + ", status: '" + job.getStatus() + "', name: '" + job.getName() + "'}, sgeStatus : " + status);

                    //Track SGEManager
                    switch(status) {
                        case SgeManager.FINISHED:
                            if(!Job.DONE.equals(job.getStatus())) {
                                catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.DONE), sessionId);
                                jobStatus = Job.DONE;
                            }
                            break;
                        case SgeManager.ERROR:
                        case SgeManager.EXECUTION_ERROR:
                            if(!Job.ERROR.equals(job.getStatus())) {
                                catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.ERROR), sessionId);
                                jobStatus = Job.ERROR;
                            }
                            break;
                        case SgeManager.QUEUED:
                            if(!Job.QUEUED.equals(job.getStatus())) {
                                catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.QUEUED), sessionId);
                                jobStatus = Job.QUEUED;
                            }
                            break;
                        case SgeManager.RUNNING:
                            if(!Job.RUNNING.equals(job.getStatus())) {
                                catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.RUNNING), sessionId);
                                jobStatus = Job.RUNNING;
                            }
                            break;
                        case SgeManager.TRANSFERRED:
                            break;
                        case SgeManager.UNKNOWN:
                            break;
                    }

                    //Track Catalog Job status
                    switch (jobStatus) {
                        case Job.DONE:
                            analysisOutputRecorder.recordJobOutput(job);
                            break;
                        case Job.ERROR:
                            String jobErrorPolicy = "recordOutput";
                            switch(jobErrorPolicy) {
                                case "deleteOutput":
                                    throw new UnsupportedOperationException("Unimplemented policy");
                                case "waitForInstructions":
                                    throw new UnsupportedOperationException("Unimplemented policy");
                                case "recordOutput":
                                    analysisOutputRecorder.recordJobOutput(job);
                                    break;
                            }
                            break;
                        case Job.PREPARED:
                            AnalysisJobExecuter.execute(job);
                            catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.QUEUED), sessionId);
                            break;
                        case Job.QUEUED:
                            break;
                        case Job.RUNNING:
                            break;
                        case Job.READY:
                            //Never expected!
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            logger.info("----- Pending deletions -----");
            try {
                QueryResult<File> files = catalogManager.searchFile(-1, new QueryOptions("status", File.DELETING), sessionId);
                long currentTimeMillis = System.currentTimeMillis();
                for (File file : files.getResult()) {
                    long deleteDate = new ObjectMap(file.getAttributes()).getLong(File.DELETE_DATE, 0);
                    if(currentTimeMillis - deleteDate > Long.valueOf(properties.getProperty(DELETE_DELAY, "30"))*1000) { //Seconds to millis
                        QueryResult<Study> studyQueryResult = catalogManager.getStudy(catalogManager.getStudyIdByFileId(file.getId()), sessionId);
                        Study study = studyQueryResult.getResult().get(0);
                        logger.info("Deleting file {} from study {id: {}, alias: {}}", file, study.getId(), study.getAlias());
                        CatalogIOManager catalogIOManager = catalogManager.getCatalogIOManagerFactory().get(study.getUri());
                        catalogIOManager.deleteFile(catalogIOManager.getFileUri(study.getUri(), file.getPath()));
                        catalogManager.modifyFile(file.getId(), new ObjectMap("status", File.DELETED), sessionId);
                    } else {
                        logger.info("Don't delete file {id: {}, path: '{}', attributes: {}}}", file.getId(), file.getPath(), file.getAttributes());
                        logger.info("{}", (currentTimeMillis - deleteDate)/1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(sessionId != null) {
            try {
                catalogManager.logout(properties.getProperty(USER), sessionId);
            } catch (CatalogDBException | IOException e) {
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
