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

import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.study.Study;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StudyWSServerTest {

    private WebTarget webTarget;

    public StudyWSServerTest(WebTarget webTarget) {
        this.webTarget = webTarget;
    }

    public Study createStudy(long projectId, String sessionId) throws IOException {
        String stName = "st_" + RandomStringUtils.random(8, String.valueOf(System.currentTimeMillis()));
        System.out.println("\nTesting study creation...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tprojectId: " + projectId);
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tname: " + stName);
        System.out.println("\talias: " + stName);
        System.out.println("\ttype: " + Enums.CohortType.CASE_CONTROL);
        System.out.println("\tstatus: " + Status.READY);
        System.out.println("\tdescription: description");

        String s = webTarget.path("studies").path("create")
                .queryParam("projectId", projectId)
                .queryParam("sid", sessionId)
                .queryParam("name", stName)
                .queryParam("alias", stName)
                .queryParam("type", Enums.CohortType.CASE_CONTROL)
                .queryParam("status", Status.READY)
                .queryParam("description", "description")
                .request().get(String.class);
        QueryResponse<Study> queryResponse = WSServerTestUtils.parseResult(s, Study.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());

        System.out.println("\nJSON RESPONSE");
        System.out.println(s);

        return queryResponse.getResponse().get(0).first();
    }

    public Study info(long studyId, String sessionId) throws IOException {
        System.out.println("\nTesting study info...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tstudyId: " + String.valueOf(studyId));
        System.out.println("\tsid: " + sessionId);

        String json = webTarget.path("studies").path(String.valueOf(studyId)).path("info")
                .queryParam("sid", sessionId)
                .request().get(String.class);
        QueryResponse<Study> queryResponse = WSServerTestUtils.parseResult(json, Study.class);
        Study study = queryResponse.getResponse().get(0).first();
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return study;
    }

    public void modifyStudy(long studyId, String sessionId) throws IOException {
        String name = studyId + "-mod";
        Enums.CohortType type = Enums.CohortType.CASE_SET;
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

        QueryResponse<ObjectMap> queryResponse = WSServerTestUtils.parseResult(json, ObjectMap.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("Testing study modification finished");
    }


    public List<Variant> fetchVariants(long studyId, String sessionId, QueryOptions queryOptions) throws IOException {
        System.out.println("\nTesting file fetch variants...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tstudyId: " + studyId);

        WebTarget webTarget = this.webTarget.path("studies").path(String.valueOf(studyId)).path("variants")
                .queryParam("sid", sessionId);
        for (Map.Entry<String, Object> entry : queryOptions.entrySet()) {
            webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
            System.out.println("\t" + entry.getKey() + ": " + entry.getValue());

        }
        System.out.println("webTarget = " + webTarget);
        String json = webTarget.request().get(String.class);
        System.out.println("json = " + json);


        QueryResponse<Variant> queryResponse = WSServerTestUtils.parseResult(json, Variant.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        List<Variant> variants = queryResponse.getResponse().get(0).getResult();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return variants;
    }

}
