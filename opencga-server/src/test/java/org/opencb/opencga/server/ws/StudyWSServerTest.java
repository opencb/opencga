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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.RandomStringUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Study;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class StudyWSServerTest {

    private WebTarget webTarget;

    public StudyWSServerTest(WebTarget webTarget) {
        this.webTarget = webTarget;
    }

    public Study createStudy(int projectId, String sessionId) throws IOException {
        String stName = "st_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
        System.out.println("\nTesting study creation...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tprojectId: " + projectId);
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tname: " + stName);
        System.out.println("\talias: " + stName);
        System.out.println("\ttype: type");
        System.out.println("\tstatus: status");
        System.out.println("\tdescription: description");

        String s = webTarget.path("studies").path("create")
                .queryParam("projectId", projectId)
                .queryParam("sid", sessionId)
                .queryParam("name", stName)
                .queryParam("alias", stName)
                .queryParam("type", Study.Type.CASE_CONTROL)
                .queryParam("status", "status")
                .queryParam("description", "description")
                .request().get(String.class);
        QueryResponse<QueryResult<Study>> queryResponse = WSServerTestUtils.parseResult(s, Study.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());

        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

        return queryResponse.getResponse().get(0).first();
    }

    public Study info(int studyId, String sessionId) throws IOException {
        System.out.println("\nTesting study info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tstudyId: " + String.valueOf(studyId));
        System.out.println("\tsid: " + sessionId);

        String json = webTarget.path("studies").path(String.valueOf(studyId)).path("info")
                .queryParam("sid", sessionId)
                .request().get(String.class);
        QueryResponse<QueryResult<Study>> queryResponse = WSServerTestUtils.parseResult(json, Study.class);
        Study study = queryResponse.getResponse().get(0).first();
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return study;
    }

    public void modifyStudy(int studyId, String sessionId) throws IOException {
        String name = studyId + "-mod";
        Study.Type type = Study.Type.CASE_SET;
        String description = "desc-mod";
        String status = "status-mod";
        //String attr = "attr-mod";
        //String stats = "stats-mod";


        System.out.println("\nTesting study modification...");
        System.out.println("------------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tstudyId: " + studyId);
        System.out.println("\tsessionId: " + sessionId);
        System.out.println("\tname: " + name);
        System.out.println("\ttype: " + type);
        System.out.println("\tdescription: " + description);
        System.out.println("\tstatus: " + status);

        String json = webTarget.path("studies").path(String.valueOf(studyId))
                .path("update")
                .queryParam("sid", sessionId)
                .queryParam("name", name)
                .queryParam("type", type)
                .queryParam("description", description)
                .queryParam("status", status)
                .request().get(String.class);

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        QueryResponse<QueryResult<ObjectMap>> queryResponse = WSServerTestUtils.parseResult(json, ObjectMap.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("Testing study modification finished");
    }
}
