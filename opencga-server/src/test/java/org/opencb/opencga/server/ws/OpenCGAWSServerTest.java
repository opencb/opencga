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

import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import static org.junit.Assert.assertEquals;

public class OpenCGAWSServerTest {

    private Client webClient;
    private WebTarget webTarget;

    private String restURL = "http://localhost:8080/opencga/rest/";
//    WebResource webResource;

//    ObjectMapper objectMapper;
//
//    /** User variables **/
//    String userId;
//    String sessionId;
//
//    /** Project variables **/
//    int prId;

    @Before
    public void init(){
        webClient = ClientBuilder.newClient();
        webTarget = webClient.target(restURL);
//        webResource = client.resource("http://localhost:8080/opencga/rest/");
//        objectMapper = new ObjectMapper();
    }

    /** First echo message to test Server connectivity **/
    @Test
    public void testConnectivity(){
        String message = "Test";
//        String s = webResource.path("test").path("echo").path(message).get(String.class);
        String s = webTarget.path("test").path("echo").path(message).request().get(String.class);
        assertEquals("Expected [" + message + "], actual [" + s + "]", message, s);
    }

    /** User tests **/
   //@Test
    public void userTests(){
        UserWSServerTest userTest = new UserWSServerTest(webTarget);
        userTest.createUser();
        userTest.loginUser();
        userTest.modifyUser();
    }
    @Test
    public void workflowCreation(){
        UserWSServerTest userTest = new UserWSServerTest(webTarget);
        userTest.createUser();
        userTest.loginUser();

        ProjectWSServerTest prTest = new ProjectWSServerTest(userTest);
        prTest.createProject();
        prTest.modifyProject();
        prTest.info();
        prTest.getAllProjects();

        StudyWSServerTest stTest = new StudyWSServerTest(prTest);
        stTest.createStudy();
        stTest.modifyStudy();
        stTest.info();
        stTest.getAllStudies();
    }

//


}
