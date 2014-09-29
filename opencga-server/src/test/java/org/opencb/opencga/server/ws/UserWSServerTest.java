package org.opencb.opencga.server.ws;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.opencb.datastore.core.QueryResponse;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class UserWSServerTest {

    private String userId;
    private String sessionId;
    private WebResource webResource;
    //private ObjectMapper objectMapper;

    public UserWSServerTest(WebResource webResource){
        userId = "user_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
        this.webResource = webResource;
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

        MultivaluedMap queryParams = new MultivaluedMapImpl();
        queryParams.add("userId", userId);
        queryParams.add("password", userId);
        queryParams.add("name", userId);
        queryParams.add("email", "email@cipf.es");
        queryParams.add("organization", "cipf");
        queryParams.add("role", "none");
        queryParams.add("status", "none");
        String s = webResource.path("users").path("create").queryParams(queryParams).get(String.class);

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
        MultivaluedMap queryParams = new MultivaluedMapImpl();
        queryParams.add("password", userId);
        String s = webResource.path("users").path(userId).path("login").queryParams(queryParams).get(String.class);
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

        MultivaluedMap queryParams = new MultivaluedMapImpl();
        queryParams.add("sid", sessionId);
        queryParams.add("name", name);
        queryParams.add("email", email);
        queryParams.add("organization", organization);
        String s = webResource.path("users").path(userId).path("modify").queryParams(queryParams).get(String.class);

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
    public WebResource getWebResource(){
        return webResource;
    }
}
