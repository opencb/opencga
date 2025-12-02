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

package org.opencb.opencga.server;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.opencb.opencga.core.config.RestServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by imedina on 02/01/16.
 */
public class RestServer extends AbstractServer {

    private static Server server;
//    private Path opencgaHome;
    private boolean exit;
    private final Logger logger = LoggerFactory.getLogger(RestServer.class);

    public RestServer(Path opencgaHome, int port) {
        super(opencgaHome, port);
    }

    @Override
    public void start() throws Exception {
        initServer();

        Path war = getOpencgaWar();

        initWebApp(war);

        server.start();
        logger.info("REST server started, listening on {}", server.getURI());

        initHooks();

//        // AdminWSServer server needs a reference to this class to cll to .stop()
//        AdminRestWebService.setServer(this);
    }

    protected Server initServer() {
        server = new Server();

        HttpConfiguration httpConfig = getHttpConfiguration();

        ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
        httpConnector.setPort(port);

        server.addConnector(httpConnector);
        return server;
    }

    protected Path getOpencgaWar() throws Exception {
        Optional<Path> warPath;
        try (Stream<Path> stream = Files.list(opencgaHome)) {
            warPath = stream
                    .filter(path -> path.toString().endsWith("war"))
                    .findFirst();
        } catch (IOException e) {
            throw new Exception("Error accessing OpenCGA Home: " + opencgaHome.toString(), e);
        }
        // Check is a war file has been found in opencgaHome
        if (!warPath.isPresent()) {
            throw new Exception("No war file found at " + opencgaHome.toString());
        }
        return warPath.get();
    }

    protected WebAppContext initWebApp(Path war) throws Exception {
        String opencgaVersion = war.toFile().getName().replace(".war", "");
        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/" + opencgaVersion);
        webapp.setWar(war.toString());
        webapp.setClassLoader(this.getClass().getClassLoader());
        webapp.setInitParameter("OPENCGA_HOME", opencgaHome.toFile().toString());
        webapp.getServletContext().setAttribute("OPENCGA_HOME", opencgaHome.toFile().toString());
//        webapp.setInitParameter("log4jConfiguration", opencgaHome.resolve("conf/log4j2.server.xml").toString());
        server.setHandler(webapp);
        return webapp;
    }

    protected void initHooks() {
        // A hook is added in case the JVM is shutting down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (server.isRunning()) {
                    stopJettyServer();
                }
            } catch (Exception e) {
                logger.error("Error stopping Jetty server", e);
            }
        }));

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
                logger.error("Error stopping Jetty server", e);
            }
        }).start();
    }

    protected HttpConfiguration getHttpConfiguration() {
        HttpConfiguration httpConfig = new HttpConfiguration();
        RestServerConfiguration.HttpConfiguration restHttpConf = configuration.getServer().getRest().getHttpConfiguration();
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
