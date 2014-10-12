package org.opencb.opencga.server.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.RandomStringUtils;
import org.opencb.datastore.core.QueryResponse;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ProjectWSServerTest {

    private UserWSServerTest userTest;
    private int projectId;

    public ProjectWSServerTest(UserWSServerTest uTest ){
        userTest = uTest;
    }

    public void createProject() {
        ObjectMapper objectMapper = new ObjectMapper();
        String prName = "pr_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));

        System.out.println("\nTesting project creation...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: "+ userTest.getUserId());
        System.out.println("\tsid: "+ userTest.getSessionId());
        System.out.println("\tname: "+ prName);
        System.out.println("\talias: "+ prName);
        System.out.println("\tdescription: description");
        System.out.println("\tstatus: status");
        System.out.println("\torganization: organization");
//        MultivaluedMap queryParams = new MultivaluedHashMap();
//        queryParams.add("userId", userTest.getUserId());
//        queryParams.add("sid", userTest.getSessionId());
//        queryParams.add("name", prName);
//        queryParams.add("alias", prName);
//        queryParams.add("description", "description");
//        queryParams.add("status", "status");
//        queryParams.add("organization", "organization");
        String s = userTest.getWebTarget().path("projects").path("create")
                .queryParam("userId", userTest.getUserId())
                .queryParam("sid", userTest.getSessionId())
                .queryParam("name", prName)
                .queryParam("alias", prName)
                .queryParam("description", "description")
                .queryParam("status", "status")
                .queryParam("organization", "organization")
                .request().get(String.class);
        try {
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
            System.out.println("\nOUTPUT PARAMS");
            projectId = Integer.parseInt(WSServerTestUtils.getField(queryResponse,"id"));
            System.out.println("\tprojectId: "+ projectId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

    }
    public void info(){
        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println("\nTesting project info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + userTest.getSessionId());
        System.out.println("\tprojectId: " + projectId);

//        MultivaluedMap queryParams = new MultivaluedMapImpl();
//        queryParams.add("sid", userTest.getSessionId());

        String s = userTest.getWebTarget().path("projects").path(String.valueOf(projectId)).path("info")
                .queryParam("sid", userTest.getSessionId()).request().get(String.class);
        try {
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
            System.out.println("\nOUTPUT PARAMS");
            String name = WSServerTestUtils.getField(queryResponse, "name");
            System.out.println("\nname: "+ name);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

    }
    public void getAllProjects(){
        ObjectMapper objectMapper = new ObjectMapper();
        String ownerId = userTest.getUserId();
        System.out.println("\nTesting all projects info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: "+ userTest.getSessionId());
        System.out.println("\townerId: "+ ownerId);

//        MultivaluedMap queryParams = new MultivaluedMapImpl();
//        queryParams.add("sid", userTest.getSessionId());

        String s = userTest.getWebTarget().path("projects").path(String.valueOf(ownerId)).path("all-projects")
                .queryParam("sid", userTest.getSessionId()).request().get(String.class);
        try {
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
            System.out.println("\nOUTPUT PARAMS");
            String name = WSServerTestUtils.getField(queryResponse,"name");
            System.out.println("\nname: "+ name);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

    }
    public void modifyProject() {
        String name = projectId + "-mod";
        String description = "desc-mod";
        String organization = "org-mod";
        System.out.println("\nTesting project modification...");
        System.out.println("------------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tprojectId: " + projectId);
        System.out.println("\tsessionId: " + userTest.getSessionId());
        System.out.println("\tname: " + name);
        System.out.println("\tdescription: " + description);
        System.out.println("\torganization: " + organization);

//        MultivaluedMap queryParams = new MultivaluedMapImpl();
//        queryParams.add("sid", userTest.getSessionId());
//        queryParams.add("name", name);
//        queryParams.add("description", description);
//        queryParams.add("organization", organization);
        String s = userTest.getWebTarget().path("projects").path(String.valueOf(projectId)).path("modify")
                .queryParam("sid", userTest.getSessionId())
                .queryParam("name", name)
                .queryParam("description", description)
                .queryParam("organization", organization)
                .request().get(String.class);

        System.out.println("\nJSON RESPONSE");
        System.out.println(s);
        try {
            org.codehaus.jackson.map.ObjectMapper objectMapper = new org.codehaus.jackson.map.ObjectMapper();
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse.class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Testing project modification finished");
    }
    public int getProjectId(){
        return projectId;
    }
    public UserWSServerTest getUserTest(){
        return userTest;
    }
}
