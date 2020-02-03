/*
 * Copyright 2015-2017 OpenCB
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.models.project.Project;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ProjectWSServerTest {

    public static final String PROJECT_ALIAS = "def_pr";
    private WebTarget webTarget;

    public ProjectWSServerTest(WebTarget webTarget){
        this.webTarget = webTarget;
    }

    public Project createProject(String userId, String sessionId) throws IOException {
        String prName = "pr_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
        String prAlias = PROJECT_ALIAS;

        System.out.println("\nTesting project creation...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tuserId: "+ userId);
        System.out.println("\tsid: "+ sessionId);
        System.out.println("\tname: "+ prName);
        System.out.println("\talias: "+ prAlias);
        System.out.println("\tdescription: description");
        System.out.println("\tstatus: status");
        System.out.println("\torganization: organization");

        String json = webTarget.path("projects").path("create")
                .queryParam("userId", userId)
                .queryParam("sid", sessionId)
                .queryParam("name", prName)
                .queryParam("alias", prAlias)
                .queryParam("description", "description")
                .queryParam("status", "status")
                .queryParam("organization", "organization")
                .request().get(String.class);

        QueryResponse<Project> queryResponse = WSServerTestUtils.parseResult(json, Project.class);

        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        Project project = queryResponse.getResponse().get(0).first();
        long projectId = project.getUid();
        System.out.println("\tprojectId: "+ projectId);

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return project;
    }
    public Project info(long projectId, String sessionId) throws IOException {
        System.out.println("\nTesting project info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tprojectId: " + projectId);

        String json = webTarget.path("projects").path(String.valueOf(projectId)).path("info")
                .queryParam("sid", sessionId).request().get(String.class);

        QueryResponse<Project> queryResponse = WSServerTestUtils.parseResult(json, Project.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        Project project = queryResponse.getResponse().get(0).first();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return project;
    }

    public void getAllStudies(long projectId, String sessionId){
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println("\nTesting all studies info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tprojectId: " + String.valueOf(projectId));
        System.out.println("\tsid: " + sessionId);

//        MultivaluedMap queryParams = new MultivaluedMapImpl();
//        queryParams.add("sid", sessionId);
        String s = webTarget.path("projects").path(String.valueOf(projectId))
                .path("studies")
                .queryParam("sid", sessionId)
                .request().get(String.class);
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
    public void modifyProject(long projectId, String sessionId) {
        String name = projectId + "-mod";
        String description = "desc-mod";
        String organization = "org-mod";
        System.out.println("\nTesting project modification...");
        System.out.println("------------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tprojectId: " + projectId);
        System.out.println("\tsessionId: " + sessionId);
        System.out.println("\tname: " + name);
        System.out.println("\tdescription: " + description);
        System.out.println("\torganization: " + organization);


        String s = webTarget.path("projects").path(String.valueOf(projectId)).path("update")
                .queryParam("sid", sessionId)
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

}
