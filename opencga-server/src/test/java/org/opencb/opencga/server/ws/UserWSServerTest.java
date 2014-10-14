package org.opencb.opencga.server.ws;

import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.datastore.core.QueryResponse;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class UserWSServerTest {

    private String userId;
    private String sessionId;
    Client client = ClientBuilder.newClient();

//    private WebResource webTarget;
    private WebTarget webTarget;
    //private ObjectMapper objectMapper;

    public UserWSServerTest(WebTarget webTarget){
        userId = "user_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
        this.webTarget = webTarget;
    }

    public void createUser(){
        System.out.println("\nTesting user creation...");
        System.out.println("------------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: " + userId);
        System.out.println("\tpassword:" + userId);
        System.out.println("\tname: " + userId);
        System.out.println("\temail: email@cipf.es");
        System.out.println("\torganization: cipf");
        System.out.println("\trole: none");
        System.out.println("\tstatus: none");

//        MultivaluedMap queryParams = new MultivaluedMapImpl();
        MultivaluedMap queryParams = new MultivaluedHashMap();
        queryParams.add("userId", userId);
        queryParams.add("password", userId);
        queryParams.add("name", userId);
        queryParams.add("email", "email@cipf.es");
        queryParams.add("organization", "cipf");
        queryParams.add("role", "none");
        queryParams.add("status", "none");

//        String s = webTarget.path("users").path("create").queryParams(queryParams).get(String.class);
        String s =this.webTarget.path("users").path("create").queryParam("userId", userId)
                .queryParam("password", userId)
                .queryParam("name", userId)
                .queryParam("email", "email@cipf.es")
                .queryParam("organization", "cipf")
                .queryParam("role", "none")
                .queryParam("status", "none")
                .request().get(String.class);

        System.out.println("\nJSON RESPONSE");
        System.out.println(s);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Testing user creation finished");
    }

    public void loginUser(){
        System.out.println("\nTesting user login...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: " + userId);
        System.out.println("\tpassword: " + userId);
        ObjectMapper objectMapper = new ObjectMapper();
//        MultivaluedMap queryParams = new MultivaluedMapImpl();
//        queryParams.add("password", userId);
        String s = webTarget.path("users").path(userId).path("login").queryParam("password", userId).request().get(String.class);
        try {
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
            sessionId = WSServerTestUtils.getField(queryResponse,"sessionId");
            System.out.println("\nOUTPUT PARAMS");
            System.out.println("\tsessionId: " + sessionId);

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);
        System.out.println("Testing user login finished");
    }

    public void modifyUser(){
        String name = userId + "-mod";
        String email = "email@cipf-mod.es";
        String organization = "cipf-mod";
        System.out.println("\nTesting user modification...");
        System.out.println("------------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: " + userId);
        System.out.println("\tsessionId: " + sessionId);
        System.out.println("\tname: " + name);
        System.out.println("\temail: " + email);
        System.out.println("\torganization: " + organization);

//        MultivaluedMap queryParams = new MultivaluedMapImpl();
//        queryParams.add("sid", sessionId);
//        queryParams.add("name", name);
//        queryParams.add("email", email);
//        queryParams.add("organization", organization);
        String s = webTarget.path("users").path(userId).path("modify").queryParam("sid", sessionId)
                .queryParam("name", name).queryParam("email", email).queryParam("organization", organization)
                .request().get(String.class);

        System.out.println("\nJSON RESPONSE");
        System.out.println(s);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
            //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
            assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Testing user creation finished");


    }
    public String getUserId(){
        return userId;
    }
    public String getSessionId(){
        return sessionId;
    }
    public WebTarget getWebTarget(){
        return webTarget;
    }
}
