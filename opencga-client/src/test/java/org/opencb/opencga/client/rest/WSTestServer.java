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

package org.opencb.opencga.client.rest;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 9/25/14.
 */
public class WSTestServer {

    private Server server;
    private String restURL;
    public static final int PORT = 8890;
    public static final String DATABASE_PREFIX = "opencga_server_test_";

    private CatalogManager catalogManager;
    private CatalogManagerExternalResource catalogManagerResource;

    public void initServer() throws Exception {

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(false, "org.opencb.opencga.server.ws");
        resourceConfig.property("jersey.config.server.provider.packages", "org.opencb.opencga.server.ws;com.wordnik.swagger.jersey.listing;com.jersey.jaxb;com.fasterxml.jackson.jaxrs.json");
        resourceConfig.property("jersey.config.server.provider.classnames", "org.glassfish.jersey.media.multipart.MultiPartFeature");

        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder(sc);

        server = new Server(PORT);

        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/webservices/rest/*");

        System.err.println("Starting server");
        server.start();
        System.err.println("Waiting for connections");
        System.out.println(server.getState());

        System.out.println(server.getURI());
    }

    public void shutdownServer() throws Exception {
        System.err.println("Shutdown server");
        server.stop();
        server.join();
    }

    public void setUp() throws Exception {
        //Create test environment. Override OpenCGA_Home
        Path opencgaHome = Paths.get("/tmp/opencga-server-test");
        System.setProperty("app.home", opencgaHome.toString());
//        Config.setOpenCGAHome(opencgaHome.toString());

        Files.createDirectories(opencgaHome);
        Files.createDirectories(opencgaHome.resolve("conf"));

        CatalogManagerTest catalogManagerTest =  new CatalogManagerTest();
        catalogManagerResource = catalogManagerTest.catalogManagerResource;
        catalogManagerResource.before();

        catalogManagerResource.getConfiguration().serialize(new FileOutputStream(opencgaHome.resolve("conf").resolve("configuration.yml").toFile()));
//        InputStream inputStream = new ByteArrayInputStream((AnalysisJobExecutor.OPENCGA_ANALYSIS_JOB_EXECUTOR + "=LOCAL" + "\n" +
//                AnalysisFileIndexer.OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX + "=" + DATABASE_PREFIX).getBytes());
//        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);
//        inputStream = OpenCGAWSServerTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
//        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("storage-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        catalogManagerTest.setUpCatalogManager(catalogManagerResource.getCatalogManager()); //Clear and setup CatalogDatabase
        catalogManager = catalogManagerResource.getCatalogManager();
    }
}

