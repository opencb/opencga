package org.opencb.opencga.app.daemon;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisFileIndexer;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.SgeManager;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by jacobo on 23/10/14.
 */
public class DaemonLoop implements Runnable {

    public static final String PORT     = "OPENCGA.APP.DAEMON.PORT";
    public static final String SLEEP    = "OPENCGA.APP.DAEMON.SLEEP";
    public static final String USER     = "OPENCGA.APP.DAEMON.USER";
    public static final String PASSWORD = "OPENCGA.APP.DAEMON.PASSWORD";

    private final Properties properties;

    private Server server;
    private Thread thread;
    private boolean exit = false;
    private CatalogManager catalogManager;

    private static Logger logger = LoggerFactory.getLogger(DaemonLoop.class);
    private final AnalysisFileIndexer analysisFileIndexer;

    public DaemonLoop(Properties properties) {
        this.properties = properties;
        try {
            catalogManager = new CatalogManager(Config.getCatalogProperties());
        } catch (IOException | CatalogIOManagerException | CatalogManagerException e) {
            e.printStackTrace();
        }

        analysisFileIndexer = new AnalysisFileIndexer(catalogManager, Config.getAnalysisProperties());

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
        String sessionId = null;
        try {
            QueryResult<ObjectMap> login = catalogManager.login(properties.getProperty(USER), properties.getProperty(PASSWORD), "daemon");
            sessionId = login.getResult().get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException e) {
            e.printStackTrace();
            exit = true;
        }

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
                    System.out.println("job = " + job);
                    switch(SgeManager.status(String.valueOf(job.getId()))) {
                        case SgeManager.UNKNOWN:

                            break;
                        case SgeManager.ERROR:
                            //TODO: Handle error
                            break;
                        case SgeManager.QUEUED:
                        case SgeManager.RUNNING:
                        case SgeManager.TRANSFERRED:
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            logger.info("----- Pending index -----");
            try {
                QueryResult<File> files = catalogManager.searchFile(-1, new QueryOptions("indexState", Index.PENDING), sessionId);
                for (File file : files.getResult()) {
                    System.out.println("file = " + file);
                    for (Index index : file.getIndices()) {
                        if(index.getState().equals(Index.PENDING)) {
                            switch(SgeManager.status(index.getJobId())) {
                                case SgeManager.UNKNOWN:
                                    analysisFileIndexer.finishIndex(index.getJobId(), sessionId);
                                    break;
                                case SgeManager.ERROR:
                                    //TODO: Handle error
                                    break;
                                case SgeManager.QUEUED:
                                case SgeManager.RUNNING:
                                case SgeManager.TRANSFERRED:
                                    break;
                            }

                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(sessionId != null) {
            try {
                catalogManager.logout(properties.getProperty(USER), sessionId);
            } catch (CatalogManagerException | IOException e) {
                e.printStackTrace();
            }
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
