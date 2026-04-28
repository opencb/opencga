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
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jasig.cas.client.configuration.ConfigurationKeys;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.RestServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
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
        addSingleSignOnFilters(webapp);
        return webapp;
    }

    private void addSingleSignOnFilters(WebAppContext webapp) throws Exception {
        if (configuration == null || configuration.getSso() == null || !configuration.getSso().isActive()) {
            return;
        }
        // Check all mandatory fields
        ParamUtils.checkParameter(configuration.getSso().getCasServerPrefixUrl(), "sso.casServerPrefixUrl");
        ParamUtils.checkParameter(configuration.getSso().getServerName(), "sso.serverName");
        ParamUtils.checkParameter(configuration.getSso().getProtocol(), "sso.protocol");
        Map<String, String> initParameters = configuration.getSso().getInitParameters() != null
                ? configuration.getSso().getInitParameters() : Collections.emptyMap();

        switch (configuration.getSso().getProtocol().toUpperCase()) {
            case "CAS":
                logger.info("Using CAS protocol");
                // Start CAS protocol configuration
                FilterHolder casValidationFilterHolder = new FilterHolder();
                casValidationFilterHolder.setName("CAS Validation Filter");
                casValidationFilterHolder.setClassName("org.jasig.cas.client.validation.Cas20ProxyReceivingTicketValidationFilter");
                Map<String, String> casInitParameters = new HashMap<>();
                casInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), configuration.getSso().getCasServerPrefixUrl());
                casInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), configuration.getSso().getServerName());
                casInitParameters.putAll(initParameters);
                casValidationFilterHolder.setInitParameters(casInitParameters);
                webapp.addFilter(casValidationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                FilterHolder casAuthenticationFilterHolder = new FilterHolder();
                casAuthenticationFilterHolder.setName("CAS Authentication Filter");
                casAuthenticationFilterHolder.setClassName("org.opencb.opencga.server.sso.OpencgaAuthenticationFilter");
                casInitParameters = new HashMap<>();
                casInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), configuration.getSso().getCasServerPrefixUrl());
                casInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), configuration.getSso().getServerName());
                casInitParameters.putAll(initParameters);
                casAuthenticationFilterHolder.setInitParameters(casInitParameters);
                webapp.addFilter(casAuthenticationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                FilterHolder requestWrapperFilterHolder = new FilterHolder();
                requestWrapperFilterHolder.setName("CAS HttpServletRequest Wrapper Filter");
                requestWrapperFilterHolder.setClassName("org.jasig.cas.client.util.HttpServletRequestWrapperFilter");
                casInitParameters = new HashMap<>();
                casInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), configuration.getSso().getCasServerPrefixUrl());
                casInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), configuration.getSso().getServerName());
                casInitParameters.putAll(initParameters);
                requestWrapperFilterHolder.setInitParameters(casInitParameters);
                webapp.addFilter(requestWrapperFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));
                // End of CAS configuration
                break;
            case "SAML1":
                logger.info("Using SAML1 protocol");
                // Start SAML1 protocol configuration
                FilterHolder samlValidationFilterHolder = new FilterHolder();
                samlValidationFilterHolder.setName("CAS Validation Filter");
                samlValidationFilterHolder.setClassName("org.jasig.cas.client.validation.Saml11TicketValidationFilter");
                Map<String, String> samlInitParameters = new HashMap<>();
                samlInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), configuration.getSso().getCasServerPrefixUrl());
                samlInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), configuration.getSso().getServerName());
                samlInitParameters.putAll(initParameters);
                samlValidationFilterHolder.setInitParameters(samlInitParameters);
                webapp.addFilter(samlValidationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                FilterHolder samlAuthenticationFilterHolder = new FilterHolder();
                samlAuthenticationFilterHolder.setName("CAS Authentication Filter");
                samlAuthenticationFilterHolder.setClassName("org.opencb.opencga.server.sso.Saml11OpencgaAuthenticationFilter");
                samlInitParameters = new HashMap<>();
                samlInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), configuration.getSso().getCasServerPrefixUrl());
                samlInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), configuration.getSso().getServerName());
                samlInitParameters.putAll(initParameters);
                samlAuthenticationFilterHolder.setInitParameters(samlInitParameters);
                webapp.addFilter(samlAuthenticationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                FilterHolder saml1RequestWrapperFilterHolder = new FilterHolder();
                saml1RequestWrapperFilterHolder.setName("CAS HttpServletRequest Wrapper Filter");
                saml1RequestWrapperFilterHolder.setClassName("org.jasig.cas.client.util.HttpServletRequestWrapperFilter");
                samlInitParameters = new HashMap<>();
                samlInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), configuration.getSso().getCasServerPrefixUrl());
                samlInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), configuration.getSso().getServerName());
                samlInitParameters.putAll(initParameters);
                saml1RequestWrapperFilterHolder.setInitParameters(samlInitParameters);
                webapp.addFilter(saml1RequestWrapperFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));
                // End of SAML1 configuration
                break;
            default:
                throw new Exception("Unsupported protocol '" + configuration.getSso().getProtocol()
                        + "' found. Supported protocols are 'CAS' and 'SAML1'");
        }
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
