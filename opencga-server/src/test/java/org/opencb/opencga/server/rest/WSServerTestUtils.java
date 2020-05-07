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

package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.databind.ObjectReader;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

/**
 * Created by ralonso on 9/25/14.
 */
public class WSServerTestUtils {

    private Server server;
    private String restURL;
    private Path configDir;
    public static final int PORT = 8889;
    public static final String DATABASE_PREFIX = "opencga_server_test_";
    private CatalogManagerExternalResource catalogManagerResource;


    public static <T> QueryResponse<T> parseResult(String json, Class<T> clazz) throws IOException {
//        ObjectReader reader = OpenCGAWSServer.jsonObjectMapper.reader(OpenCGAWSServer.jsonObjectMapper.getTypeFactory().constructParametrizedType(
//                QueryResponse.class, QueryResponse.class, OpenCGAWSServer.jsonObjectMapper.getTypeFactory().constructParametrizedType(QueryResult.class, QueryResult.class, clazz)));
//        return reader.readValue(json);
        ObjectReader reader = getUpdateObjectMapper().reader(
                getUpdateObjectMapper().getTypeFactory().constructParametrizedType(QueryResponse.class, QueryResult.class, clazz)
        );
        return reader.readValue(json);
    }

    public void initServer() throws Exception {

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(false, "org.opencb.opencga.server.rest");
        resourceConfig.property("jersey.config.server.provider.packages", "org.opencb.opencga.server.ws;io.swagger.jersey.listing;com.jersey.jaxb;com.fasterxml.jackson.jaxrs.json");
        resourceConfig.property("jersey.config.server.provider.classnames", "org.glassfish.jersey.media.multipart.MultiPartFeature");

        // Registering MultiPart class for POST forms
        resourceConfig.register(MultiPartFeature.class);

        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder("opencga", sc);

        server = new Server(PORT);

        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/webservices/rest/*");

        context.setInitParameter("config-dir", configDir.toFile().toString());

        System.err.println("Starting server");
        server.start();
        System.err.println("Waiting for conections");
        System.out.println(server.getState());


        restURL = server.getURI().resolve("/opencga/webservices/rest/").resolve("v1/").toString();
        System.out.println(server.getURI());
    }

    public void shutdownServer() throws Exception {
        System.err.println("Shutdown server");
        server.stop();
        server.join();
        catalogManagerResource.after();
    }

    public WebTarget getWebTarget() {
        Client webClient = ClientBuilder.newClient(new ClientConfig().register(MultiPartFeature.class));
        return webClient.target(restURL);
    }

    public void setUp() throws Exception {
        //Create test environment. Override OpenCGA_Home

        CatalogManagerTest catalogManagerTest =  new CatalogManagerTest();
        catalogManagerResource = catalogManagerTest.catalogManagerResource;
        catalogManagerResource.before();

        Path opencgaHome = catalogManagerResource.getOpencgaHome();
        configDir = opencgaHome.resolve("conf");
        System.setProperty("app.home", opencgaHome.toString());
//        Config.setOpenCGAHome(opencgaHome.toString());

        Files.createDirectories(opencgaHome);
        Files.createDirectories(opencgaHome.resolve("conf"));

        catalogManagerResource.getConfiguration().serialize(new FileOutputStream(configDir.resolve("configuration.yml").toFile()));
//        InputStream inputStream = new ByteArrayInputStream((ExecutorManager.OPENCGA_ANALYSIS_JOB_EXECUTOR + "=LOCAL" + "\n" +
//                "OPENCGA.ANALYSIS.STORAGE.DATABASE_PREFIX" + "=" + DATABASE_PREFIX).getBytes());
//        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);
        InputStream inputStream = OpenCGAWSServerTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("storage-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        catalogManagerTest.setUpCatalogManager(catalogManagerResource.getCatalogManager()); //Clear and setup CatalogDatabase
        OpenCGAWSServer.catalogManager = catalogManagerResource.getCatalogManager();
    }
}

