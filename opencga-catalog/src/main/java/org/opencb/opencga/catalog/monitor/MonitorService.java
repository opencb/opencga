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

package org.opencb.opencga.catalog.monitor;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.monitor.daemons.ExecutionDaemon;
import org.opencb.opencga.catalog.monitor.daemons.FileDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 16/06/16.
 */
public class MonitorService {

    private CatalogConfiguration catalogConfiguration;
    private CatalogManager catalogManager;

    private static Server server;
    private int port;

    private ExecutionDaemon executionDaemon;
    private FileDaemon fileDaemon;

    private Thread executionThread;
    private Thread fileThread;

    private boolean exit;

    protected static Logger logger;


    public MonitorService(CatalogConfiguration catalogConfiguration) {
        this.catalogConfiguration = catalogConfiguration;

        init();
    }

    private void init() {
        logger = LoggerFactory.getLogger(this.getClass());

        try {
            this.catalogManager = new CatalogManager(this.catalogConfiguration);

            executionDaemon = new ExecutionDaemon(catalogConfiguration.getMonitor().getExecutionDaemonInterval(), catalogManager);
            fileDaemon = new FileDaemon(catalogConfiguration.getMonitor().getFileDaemonInterval(),
                    catalogConfiguration.getMonitor().getDaysToRemove(), catalogManager);

            executionThread = new Thread(executionDaemon);
            fileThread = new Thread(fileDaemon);

            this.port = catalogConfiguration.getMonitor().getPort();
        } catch (CatalogException e) {
            e.printStackTrace();
        }
    }

    public void start() throws Exception {

        // Launching the two daemons in two different threads
        executionThread.start();
        fileThread.start();

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
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (server.isRunning()) {
                        stopRestServer();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // A separated thread is launched to shut down the server
        new Thread(() -> {
            try {
                while (true) {
                    if (exit) {
                        stopRestServer();
                        break;
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // AdminWSServer server needs a reference to this class to cll to .stop()
        AdminWebService.setServer(this);
    }

    public void stop() throws Exception {
        executionDaemon.setExit(true);
        fileDaemon.setExit(true);

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
