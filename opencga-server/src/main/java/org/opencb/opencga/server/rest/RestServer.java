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

package org.opencb.opencga.server.rest;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.server.AbstractStorageServer;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Created by imedina on 02/01/16.
 */
public class RestServer extends AbstractStorageServer {

    private static Server server;

    private boolean exit;

    public RestServer() {
    }

    public RestServer(int port, String defaultStorageEngine) {
        super(port, defaultStorageEngine);

        init();
    }

    public RestServer(Path configDir) {
        super(configDir);

        init();
    }

    public RestServer(Configuration configuration, CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration) {
        super(configuration, catalogConfiguration, storageConfiguration);

        init();
    }

    @Deprecated
    public RestServer(StorageConfiguration storageConfiguration) {
        super(storageConfiguration.getServer().getRest(), storageConfiguration.getDefaultStorageEngineId());
        this.storageConfiguration = storageConfiguration;

        logger = LoggerFactory.getLogger(this.getClass());
    }

    private void init() {
        logger = LoggerFactory.getLogger(this.getClass());

        if (configuration != null) {
            this.port = configuration.getRest().getPort();
        }
    }

    @Override
    public void start() throws Exception {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(true, "org.opencb.opencga.server.rest");

        // Registering MultiPart class for POST forms
        resourceConfig.register(MultiPartFeature.class);

        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder("opencga", sc);

        server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/webservices/rest/*");
        context.setInitParameter("config-dir", configDir.toFile().toString());

        server.start();
        logger.info("REST server started, listening on {}", port);

        // A hook is added in case the JVM is shutting down
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (server.isRunning()) {
                        stopJettyServer();
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
                        stopJettyServer();
                        break;
                    }
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // AdminWSServer server needs a reference to this class to cll to .stop()
        AdminRestWebService.setServer(this);
    }

    @Override
    public void stop() throws Exception {
        // By setting exit to true the monitor thread will close the Jetty server
        exit = true;
    }

    @Override
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            // Blocking the main thread
            server.join();
        }
    }

    private void stopJettyServer() throws Exception {
        // By setting exit to true the monitor thread will close the Jetty server
        logger.info("Shutting down Jetty server");
        server.stop();
        logger.info("REST server shut down");
    }

}
