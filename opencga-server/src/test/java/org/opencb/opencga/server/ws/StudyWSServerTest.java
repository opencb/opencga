package org.opencb.opencga.server.ws;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.opencb.datastore.core.QueryResponse;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class StudyWSServerTest {

    private ProjectWSServerTest projectTest;

    public StudyWSServerTest(ProjectWSServerTest prTest){

        projectTest = prTest;
    }

    public void createStudy(){
        ObjectMapper objectMapper = new ObjectMapper();
        String stName = "st_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
        System.out.println("\nTesting study creation...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tprojectId: " + String.valueOf(projectTest.getProjectId()));
        System.out.println("\tsid: " + projectTest.getUserTest().getSessionId());
        System.out.println("\tname: " + stName);
        System.out.println("\talias: " + stName);
        System.out.println("\ttype: type");
        System.out.println("\tstatus: status");
        System.out.println("\tdescription: description");

        MultivaluedMap queryParams = new MultivaluedMapImpl();
        queryParams.add("projectId", String.valueOf(projectTest.getProjectId()));
        queryParams.add("sid", projectTest.getUserTest().getSessionId());
        queryParams.add("name", stName);
        queryParams.add("alias", stName);
        queryParams.add("type", "type");
        queryParams.add("status", "status");
        queryParams.add("description", "description");
        String s = projectTest.getUserTest().getWebResource().path("study").path("create").queryParams(queryParams).get(String.class);
        try {
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

    }
}
