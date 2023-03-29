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

package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.opencb.opencga.server.AbstractStorageServer;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by imedina on 02/01/16.
 */
public class EnterpriseRestServer extends AbstractStorageServer {

    private static Server server;
    private final EnterpriseConfiguration enterpriseConfiguration;
    private boolean exit;

    public EnterpriseRestServer(Path opencgaHome) {
        this(opencgaHome, 0);
    }

    public EnterpriseRestServer(Path opencgaHome, int port) {
        super(opencgaHome, port);

        // Read enterprise configuration file
        Path configDirPath = opencgaHome.resolve("conf");
        InputStream configInputStream;
        try {
            String confPath = configDirPath.toFile().getAbsolutePath()  + "/enterprise-configuration.yml";
            logger.info("Reading enterprise-configuration.yml file: '{}'", confPath);
            configInputStream = Files.newInputStream(Paths.get(confPath));
            enterpriseConfiguration = EnterpriseConfiguration.load(configInputStream);
        } catch (IOException e) {
            logger.error("Could not load enterprise-configuration.yml file");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() throws Exception {
        server = new Server(port);

        WebAppContext webapp = new WebAppContext();
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

        String opencgaVersion = warPath.get().toFile().getName().replace(".war", "");
        webapp.setContextPath("/" + opencgaVersion);
        webapp.setWar(warPath.get().toString());
        webapp.setClassLoader(this.getClass().getClassLoader());
        webapp.setInitParameter("OPENCGA_HOME", opencgaHome.toFile().toString());
        webapp.getServletContext().setAttribute("OPENCGA_HOME", opencgaHome.toFile().toString());
//        webapp.setInitParameter("log4jConfiguration", opencgaHome.resolve("conf/log4j2.server.xml").toString());
        server.setHandler(webapp);

        if (enterpriseConfiguration.getSso() != null && enterpriseConfiguration.getSso().isActive()) {
            // Start CAS configuration
            FilterHolder validationFilterHolder = new FilterHolder();
            validationFilterHolder.setName("CAS Validation Filter");
            validationFilterHolder.setClassName("org.jasig.cas.client.validation.Cas20ProxyReceivingTicketValidationFilter");
            Map<String, String> initParameters = new HashMap<>();
            initParameters.put("casServerUrlPrefix", enterpriseConfiguration.getSso().getCasServerPrefixUrl());
            initParameters.put("serverName", enterpriseConfiguration.getSso().getServerName());
            validationFilterHolder.setInitParameters(initParameters);
            webapp.addFilter(validationFilterHolder, "/webservices/rest/v2/*", EnumSet.of(DispatcherType.REQUEST));

            FilterHolder authenticationFilterHolder = new FilterHolder();
            authenticationFilterHolder.setName("CAS Authentication Filter");
            authenticationFilterHolder.setClassName("org.jasig.cas.client.authentication.AuthenticationFilter");
            initParameters = new HashMap<>();
            initParameters.put("casServerUrlPrefix", enterpriseConfiguration.getSso().getCasServerPrefixUrl());
            initParameters.put("serverName", enterpriseConfiguration.getSso().getServerName());
            authenticationFilterHolder.setInitParameters(initParameters);
            webapp.addFilter(authenticationFilterHolder, "/webservices/rest/v2/*", EnumSet.of(DispatcherType.REQUEST));

            FilterHolder requestWrapperFilterHolder = new FilterHolder();
            requestWrapperFilterHolder.setName("CAS HttpServletRequest Wrapper Filter");
            requestWrapperFilterHolder.setClassName("org.jasig.cas.client.util.HttpServletRequestWrapperFilter");
            initParameters = new HashMap<>();
            initParameters.put("casServerUrlPrefix", enterpriseConfiguration.getSso().getCasServerPrefixUrl());
            initParameters.put("serverName", enterpriseConfiguration.getSso().getServerName());
            webapp.addFilter(requestWrapperFilterHolder, "/webservices/rest/v2/*", EnumSet.of(DispatcherType.REQUEST));
            // End of CAS configuration
        }

        server.start();
        logger.info("REST server started, listening on {}", server.getURI());

        // A hook is added in case the JVM is shutting down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (server.isRunning()) {
                    stopJettyServer();
                }
            } catch (Exception e) {
                e.printStackTrace();
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
                e.printStackTrace();
            }
        }).start();
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
        logger.info("Enterprise REST server shutdown");
    }

}
