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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.catalog.models.*;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OpenCGAWSServerTest {

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
    public void init() {
        webClient = ClientBuilder.newClient();
        webClient.register(MultiPartFeature.class);
        webTarget = webClient.target(restURL);
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
        User user = userTest.createUser(getRandomUserId());
        String sessionId = userTest.loginUser(user.getId());
        userTest.updateUser(user.getId(), sessionId);
    }

    @Test
    public void workflowCreation() throws Exception {
        UserWSServerTest userTest = new UserWSServerTest(webTarget);
        User user = userTest.createUser(getRandomUserId());
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
        File file = fileTest.uploadVcf(study.getId(), sessionId);
        Job indexJob = fileTest.index(file.getId(), sessionId);

        /* Emulate DAEMON working */
        AnalysisJobExecutor.execute(OpenCGAWSServer.catalogManager, indexJob, sessionId);

        QueryOptions queryOptions = new QueryOptions("limit", 10);
        queryOptions.put("region", "1");
        List<Variant> variants = fileTest.fetchVariants(file.getId(), sessionId, queryOptions);
        assertEquals(10, variants.size());

    }

    public String getRandomUserId() {
        return "user_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
    }

//


}
