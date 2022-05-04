/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.master.monitor;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.master.monitor.daemons.ExecutionDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/**
 * Created by imedina on 16/06/16.
 */
public class MonitorService {

    private Configuration configuration;
    private CatalogManager catalogManager;
    private String appHome;

    private static Server server;
    private int port;

    private ExecutionDaemon executionDaemon;
//    private FileDaemon fileDaemon;
//    private AuthorizationDaemon authorizationDaemon;

    private Thread executionThread;
//    private Thread indexThread;
//    private Thread fileThread;
//    private Thread authorizationThread;

    private volatile boolean exit;

    protected static Logger logger;


    public MonitorService(Configuration configuration, String appHome, String token)
            throws CatalogException {
        this.configuration = configuration;
        this.appHome = appHome;

        init(token);
    }

    private void init(String token) throws CatalogException {
        String logDir = configuration.getLogDir();
        boolean logFileEnabled;

        if (StringUtils.isNotBlank(configuration.getLogLevel())) {
            Level level = Level.toLevel(configuration.getLogLevel(), Level.INFO);
            System.setProperty("opencga.log.level", level.name());
        }

        if (StringUtils.isBlank(logDir) || logDir.equalsIgnoreCase("null")) {
            logFileEnabled = false;
        } else {
            logFileEnabled = true;
            System.setProperty("opencga.log.file.name", "opencga-master");
            System.setProperty("opencga.log.dir", logDir);
        }
        System.setProperty("opencga.log.file.enabled", Boolean.toString(logFileEnabled));
        Configurator.reconfigure(Paths.get(appHome, "conf", "log4j2.service.xml").toUri());

        logger = LoggerFactory.getLogger(this.getClass());

        this.catalogManager = new CatalogManager(this.configuration);
        String nonExpiringToken = this.catalogManager.getUserManager().getAdminNonExpiringToken(token);

        executionDaemon = new ExecutionDaemon(configuration.getMonitor().getExecutionDaemonInterval(), nonExpiringToken,
                catalogManager, appHome);
//            fileDaemon = new FileDaemon(configuration.getMonitor().getFileDaemonInterval(),
//                    configuration.getMonitor().getDaysToRemove(), nonExpiringToken, catalogManager);

        executionThread = new Thread(executionDaemon, "execution-thread");
//            fileThread = new Thread(fileDaemon, "file-thread");
//            authorizationThread = new Thread(authorizationDaemon, "authorization-thread");

        this.port = configuration.getMonitor().getPort();
    }

    public void start() throws Exception {

        // Launching the two daemons in two different threads
        executionThread.start();
//        indexThread.start();
//        authorizationThread.start();
//        fileThread.start();

        // Preparing the REST server configuration
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.registerClasses(AdminWebService.class);

        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder("opencga", sc);

        server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/monitor/*");

        server.start();
        logger.info("REST server started, listening on {}", port);

        // A hook is added in case the JVM is shutting down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (server.isRunning()) {
                    stopRestServer();
                }
            } catch (Exception e) {
                logger.error("Error while stopping rest", e);
            }
        }));

        // A separated thread is launched to shut down the server
        new Thread(() -> {
            try {
                while (true) {
                    if (exit) {
                        stopRestServer();
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.warn("Catch interruption! Exit");
                        exit = true;
                    }
                }
            } catch (Exception e) {
                logger.error("Error while stopping rest", e);
            }
        }).start();

        // AdminWSServer server needs a reference to this class to cll to .stop()
        AdminWebService.setServer(this);
    }

    public void stop() throws Exception {
        executionDaemon.setExit(true);
//        fileDaemon.setExit(true);
//        executionDaemon.setExit(true);
//        authorizationDaemon.setExit(true);

        // By setting exit to true the monitor thread will close the Jetty server
        exit = true;
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            // Blocking the main thread
            server.join();
        }
    }

    private void stopRestServer() throws Exception {
        // By setting exit to true the monitor thread will close the Jetty server
        logger.info("*********************************");
        logger.info("Shutting down Jetty REST server");
        server.stop();
        logger.info("REST server shut down");
        logger.info("*********************************");
    }
}
