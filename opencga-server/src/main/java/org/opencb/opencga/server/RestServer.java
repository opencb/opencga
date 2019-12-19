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

package org.opencb.opencga.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.server.rest.AdminRestWebService;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Created by imedina on 02/01/16.
 */
public class RestServer extends AbstractStorageServer {

    private static Server server;
//    private Path opencgaHome;
    private boolean exit;

    public RestServer(Path opencgaHome) {
        this(opencgaHome, 0);
    }

    public RestServer(Path opencgaHome, int port) {
        super(opencgaHome, port);
    }

    @Override
    public void start() throws Exception {
        server = new Server(port);

        WebAppContext webapp = new WebAppContext();
        Optional<Path> warPath = Files.list(opencgaHome)
                .filter(path -> path.toString().endsWith("war"))
                .findFirst();

        // Check is a war file has been found in opencgaHome
        if (!warPath.isPresent()) {
            throw new Exception("No war file found at: " + opencgaHome.toString());
        }

        String opencgaVersion = warPath.get().toFile().getName().replace(".war", "");
        webapp.setContextPath("/" + opencgaVersion);
        webapp.setWar(warPath.get().toString());
        webapp.setInitParameter("OPENCGA_HOME", opencgaHome.toFile().toString());
        server.setHandler(webapp);

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
