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

package org.opencb.opencga.server.ws;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.tools.ant.types.Commandline;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class OpenCGAWSServerTest {

    public static final String TEST_SERVER_USER = "test_server_user";
    private Client webClient;
    private WebTarget webTarget;

    public static Server server;
    public static final int PORT = 8889;
    public static String restURL;
    //    WebResource webResource;

//    ObjectMapper objectMapper;
//
//    /** User variables **/
//    String userId;
//    String sessionId;
//
//    /** Project variables **/
//    int prId;


    @BeforeClass
    static public void initServer() throws Exception {

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
        System.err.println("Waiting for conections");
        System.out.println(server.getState());


        restURL = server.getURI().resolve("/opencga/webservices/rest/").resolve("v1/").toString();
        System.out.println(server.getURI());
    }

    @AfterClass
    static public void shutdownServer() throws Exception {
        System.err.println("Shout down server");
        server.stop();
        server.join();
    }


    @Before
    public void init() throws IOException, CatalogException {
        webClient = ClientBuilder.newClient();
        webClient.register(MultiPartFeature.class);
        webTarget = webClient.target(restURL);

        //Create test environment. Override OpenCGA_Home
        Path opencgaHome = Paths.get("/tmp/opencga-server-test");
        System.setProperty("app.home", opencgaHome.toString());
        Config.setOpenCGAHome(opencgaHome.toString());

        Files.createDirectories(opencgaHome);
        Files.createDirectories(opencgaHome.resolve("conf"));

        InputStream inputStream = OpenCGAWSServerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("catalog.properties"), StandardCopyOption.REPLACE_EXISTING);
        String databasePrefix = "opencga_server_test_";
        inputStream = new ByteArrayInputStream((AnalysisJobExecutor.OPENCGA_ANALYSIS_JOB_EXECUTOR + "=LOCAL" + "\n" +
                AnalysisFileIndexer.OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX + "=" + databasePrefix).getBytes());
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);
        inputStream = OpenCGAWSServerTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("storage-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        CatalogManagerTest catalogManagerTest = new CatalogManagerTest();

        //Drop default user mongoDB database.
        String databaseName = databasePrefix + TEST_SERVER_USER + "_" + ProjectWSServerTest.PROJECT_ALIAS;
        new MongoDataStoreManager("localhost", 27017).drop(databaseName);

        catalogManagerTest.setUp(); //Clear and setup CatalogDatabase
        OpenCGAWSServer.catalogManager = catalogManagerTest.getTestCatalogManager();

    }

    /** First echo message to test Server connectivity **/
    @Test
    public void testConnectivity() throws InterruptedException, IOException {
        String message = "Test";
        WebTarget testPath = webTarget.path("test").path("echo").path(message);
        System.out.println("testPath = " + testPath);
        String s = testPath.request().get(String.class);
        assertEquals("Expected [" + message + "], actual [" + s + "]", message, s);

        testPath = webTarget.path("test").path("echo");
        System.out.println("testPath = " + testPath);
        MultiPart multiPart = new MultiPart();
        multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);
        FormDataBodyPart bodyPart = new FormDataBodyPart("message", message);
        multiPart.bodyPart(bodyPart);

        s = testPath.request().post(Entity.entity(multiPart, multiPart.getMediaType()), String.class);
        assertEquals("Expected [" + message + "], actual [" + s + "]", message, s);
    }

    /** User tests **/
    @Test
    public void userTests() throws IOException {
        UserWSServerTest userTest = new UserWSServerTest(webTarget);
        User user = userTest.createUser(TEST_SERVER_USER);
        String sessionId = userTest.loginUser(user.getId());
        userTest.updateUser(user.getId(), sessionId);
    }

    @Test
    public void workflowCreation() throws Exception {
        UserWSServerTest userTest = new UserWSServerTest(webTarget);
        User user = userTest.createUser(TEST_SERVER_USER);
        String sessionId = userTest.loginUser(user.getId());
        user = userTest.info(user.getId(), sessionId);

        ProjectWSServerTest prTest = new ProjectWSServerTest(webTarget);
        Project project = prTest.createProject(user.getId(), sessionId);
        prTest.modifyProject(project.getId(), sessionId);
        project = prTest.info(project.getId(), sessionId);
        userTest.getAllProjects(user.getId(), sessionId);

        StudyWSServerTest stTest = new StudyWSServerTest(webTarget);
        Study study = stTest.createStudy(project.getId(), sessionId);
        stTest.modifyStudy(study.getId(), sessionId);
        study = stTest.info(study.getId(), sessionId);
        prTest.getAllStudies(project.getId(), sessionId);

        FileWSServerTest fileTest = new FileWSServerTest(webTarget);
        File fileVcf = fileTest.uploadVcf(study.getId(), sessionId);
        assertEquals(File.Status.READY, fileVcf.getStatus());
        assertEquals(File.Bioformat.VARIANT, fileVcf.getBioformat());
        Job indexJobVcf = fileTest.index(fileVcf.getId(), sessionId);

        /* Emulate DAEMON working */
        indexJobVcf = runIndexJob(sessionId, indexJobVcf);
        assertEquals(Job.Status.READY, indexJobVcf.getStatus());

        QueryOptions queryOptions = new QueryOptions("limit", 10);
        queryOptions.put("region", "1");
        List<Variant> variants = fileTest.fetchVariants(fileVcf.getId(), sessionId, queryOptions);
        assertEquals(10, variants.size());


        File fileBam = fileTest.uploadBam(study.getId(), sessionId);
        assertEquals(File.Status.READY, fileBam.getStatus());
        assertEquals(File.Bioformat.ALIGNMENT, fileBam.getBioformat());
        Job indexJobBam = fileTest.index(fileBam.getId(), sessionId);

        /* Emulate DAEMON working */
        indexJobBam = runIndexJob(sessionId, indexJobBam);
        assertEquals(Job.Status.READY, indexJobBam.getStatus());

        queryOptions = new QueryOptions("limit", 10);
        queryOptions.put("region", "20:60000-60200");
        queryOptions.put(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, false);
        List<ObjectMap> alignments = fileTest.fetchAlignments(fileBam.getId(), sessionId, queryOptions);
//        assertEquals(10, alignments.size());

    }

    /**
     * Do not execute Job using its command line, won't find the opencga-storage.sh
     * Call directly to the OpenCGAStorageMain
     */
    private Job runIndexJob(String sessionId, Job indexJob) throws AnalysisExecutionException, IOException, CatalogException {
        String[] args = Commandline.translateCommandline(indexJob.getCommandLine());
        org.opencb.opencga.storage.app.StorageMain.main(Arrays.copyOfRange(args, 1, args.length));
        indexJob.setCommandLine("echo 'Executing fake CLI'");
        AnalysisJobExecutor.execute(OpenCGAWSServer.catalogManager, indexJob, sessionId);
        return OpenCGAWSServer.catalogManager.getJob(indexJob.getId(), null, sessionId).first();
    }

}
