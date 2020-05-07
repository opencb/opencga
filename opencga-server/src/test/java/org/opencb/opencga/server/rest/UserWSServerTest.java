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

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.user.User;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class UserWSServerTest {

    private WebTarget webTarget;

    public UserWSServerTest(WebTarget webTarget){
        this.webTarget = webTarget;
    }

    public User createUser(String userId) throws IOException {
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

        WebTarget webTarget = this.webTarget.path("users").path("create").queryParam("userId", userId)
                .queryParam("password", userId)
                .queryParam("name", userId)
                .queryParam("email", "email@cipf.es")
                .queryParam("organization", "cipf")
                .queryParam("status", "none");
        System.out.println("webTarget.getUri() = " + webTarget.getUri());
        String s = webTarget.request().get(String.class);
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);
        QueryResponse<User> queryResponse = WSServerTestUtils.parseResult(s, User.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        User user = queryResponse.getResponse().get(0).first();
        assertEquals(userId, user.getId());

        System.out.println("Testing user creation finished");
        return user;
    }

    public String loginUser(String userId) throws IOException {
        System.out.println("\nTesting user login...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: " + userId);
        System.out.println("\tpassword: " + userId);

        String json = webTarget.path("users").path(userId).path("login").queryParam("password", userId).request().get(String.class);
        QueryResponse<ObjectMap> queryResponse = WSServerTestUtils.parseResult(json, ObjectMap.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        String sessionId = queryResponse.getResponse().get(0).first().getString("sessionId");
        System.out.println("\nOUTPUT PARAMS");
        System.out.println("\tsessionId: " + sessionId);
        System.out.println("\nJSON RESPONSE");
        System.out.println(json);
        System.out.println("Testing user login finished");
        return sessionId;
    }

    public void updateUser(String userId, String sessionId) throws IOException {
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


        String s = webTarget.path("users").path(userId).path("update").queryParam("sid", sessionId)
                .queryParam("name", name).queryParam("email", email).queryParam("organization", organization)
                .request().get(String.class);

        System.out.println("\nJSON RESPONSE");
        System.out.println(s);
        ObjectMapper objectMapper = new ObjectMapper();
        QueryResponse queryResponse = objectMapper.readValue(s, QueryResponse .class);
        //Map<String,Object> userData = objectMapper.readValue(s, Map.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("Testing user creation finished");


    }

    public void getAllProjects(String userId, String sessionId) throws IOException {
        System.out.println("\nTesting all projects info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: "+ sessionId);
        System.out.println("\townerId: "+ userId);


        String s = webTarget.path("users").path(String.valueOf(userId)).path("projects")
                .queryParam("sid", sessionId).request().get(String.class);
        QueryResponse<Project> queryResponse = WSServerTestUtils.parseResult(s, Project.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        String name = queryResponse.getResponse().get(0).first().getName();
        System.out.println("\nname: "+ name);
        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

    }

    public User info(String userId, String sessionId) throws IOException {

        System.out.println("\nTesting user info...");
        System.out.println("------------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: " + userId);
        System.out.println("\tsid: " + sessionId);

        String json = this.webTarget.path("users").path(userId).path("info")
                .queryParam("sid", sessionId).request().get(String.class);
        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        QueryResponse<User> queryResponse = WSServerTestUtils.parseResult(json, User.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        User user = queryResponse.getResponse().get(0).first();
        assertEquals(userId, user.getId());

        System.out.println("Testing user info finished");
        return user;
    }

}
