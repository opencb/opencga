package org.opencb.opencga.server.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.QueryResult;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.*;

import org.opencb.datastore.core.QueryResponse;
import static org.junit.Assert.assertEquals;

public class OpenCGAWSServerTest {

    WebResource webResource;
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
        Client client = Client.create();
        webResource = client.resource("http://localhost:8080/opencga/rest/");
//        objectMapper = new ObjectMapper();

    }

    /** First echo message to test Server connectivity **/
    @Test
    public void testConnectivity(){
        String message = "Test";
        String s = webResource.path("test").path("echo").path(message).get(String.class);
        assertEquals("Expected [" + message + "], actual [" + s + "]", message, s);
    }

    /** User tests **/
    @Test
    public void tests(){
        UserWSServerTest userTest = new UserWSServerTest(webResource);
        userTest.createUser();
        userTest.loginUser();

        ProjectWSServerTest prTest = new ProjectWSServerTest(userTest);
        prTest.createProject();

        StudyWSServerTest stTest = new StudyWSServerTest(prTest);
        stTest.createStudy();
    }

//


}
