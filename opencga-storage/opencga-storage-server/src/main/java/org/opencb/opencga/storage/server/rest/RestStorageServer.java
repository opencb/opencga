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

package org.opencb.opencga.storage.server.rest;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.core.config.RestServerConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.server.common.AbstractStorageServer;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 02/01/16.
 */
public class RestStorageServer extends AbstractStorageServer {

    private static Server server;

    private boolean exit;

    public RestStorageServer() {
        this(storageConfiguration.getServer().getRest().getPort(), storageConfiguration.getVariant().getDefaultEngine());
    }

    public RestStorageServer(int port, String defaultStorageEngine) {
        super(port, defaultStorageEngine);

        logger = LoggerFactory.getLogger(this.getClass());
    }

    public RestStorageServer(StorageConfiguration storageConfiguration) {
        super(storageConfiguration.getServer().getRest().getPort(), storageConfiguration.getVariant().getDefaultEngine());
        RestStorageServer.storageConfiguration = storageConfiguration;

        logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public void start() throws Exception {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(true, "org.opencb.opencga.storage.server.rest");

        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder("opencga", sc);

        logger.info("Server in port : {}", port);
        server = new Server();

        HttpConfiguration httpConfig = getHttpConfiguration();

        ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
        httpConnector.setPort(port);

        server.addConnector(httpConnector);

        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/webservices/rest/*");
        context.setInitParameter("testparam", "testparamvalue");

        GenericRestWebService.setStorageConfiguration(storageConfiguration);
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

    private HttpConfiguration getHttpConfiguration() {
        HttpConfiguration httpConfig = new HttpConfiguration();
        RestServerConfiguration.HttpConfiguration restHttpConf = storageConfiguration.getServer().getRest().getHttpConfiguration();
        if (restHttpConf.getOutputBufferSize() > 0) {
            httpConfig.setOutputBufferSize(restHttpConf.getOutputBufferSize());
        }
        if (restHttpConf.getOutputAggregationSize() > 0) {
            httpConfig.setOutputAggregationSize(restHttpConf.getOutputAggregationSize());
        }
        if (restHttpConf.getRequestHeaderSize() > 0) {
            httpConfig.setRequestHeaderSize(restHttpConf.getRequestHeaderSize());
        }
        if (restHttpConf.getResponseHeaderSize() > 0) {
            httpConfig.setResponseHeaderSize(restHttpConf.getResponseHeaderSize());
        }
        if (restHttpConf.getHeaderCacheSize() > 0) {
            httpConfig.setHeaderCacheSize(restHttpConf.getHeaderCacheSize());
        }
        return httpConfig;
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
