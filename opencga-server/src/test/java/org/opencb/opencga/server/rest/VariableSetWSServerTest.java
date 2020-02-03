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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 11/04/16.
 */
public class VariableSetWSServerTest {

    private WebTarget webTarget;
    private static WSServerTestUtils serverTestUtils;
    private String sessionId;
    private String studyId = "user@1000G:phase1";
    private long variableSetId;
    private ObjectMapper objectMapper;

    public VariableSetWSServerTest() {
    }

    void setWebTarget(WebTarget webTarget) {
        this.webTarget = webTarget;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    static public void initServer() throws Exception {
        serverTestUtils = new WSServerTestUtils();
        serverTestUtils.setUp();
        serverTestUtils.initServer();
    }

    @AfterClass
    static public void shutdownServer() throws Exception {
        serverTestUtils.shutdownServer();
    }

    @Before
    public void init() throws Exception {
//        serverTestUtils.setUp();
        webTarget = serverTestUtils.getWebTarget();
        variableSetId = OpenCGAWSServer.catalogManager.getStudyManager().searchVariableSets(studyId, null,null, sessionId)
                .first().getUid();

    }

    @After
    public void after() throws Exception {
        // It is here to avoid restarting the server again and again
        serverTestUtils.setUp();
    }

    @Test
    public void addFieldToVariableSet() throws Exception {
        objectMapper = new ObjectMapper();
        Variable variable = new Variable("MY_VARIABLE", "", Variable.VariableType.STRING, "whatever", true, false, null, 6L, "", "", null,
                null);
        String json = webTarget.path("variableSet").path(String.valueOf(variableSetId)).path("field").path("add")
                .queryParam("sid", sessionId).request().post(Entity.json(variable), String.class);

        QueryResponse<VariableSet> response = WSServerTestUtils.parseResult(json, VariableSet.class);

        Set<Variable> variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertTrue("The field of the variableSet was not inserted",
                variables
                        .stream()
                        .filter(variable1 -> variable.getId().equals(variable1.getId()))
                        .findAny()
                        .isPresent());

        // Check that the insertion has been propagated properly to the annotations
        json = webTarget.path("samples").path("search")
                .queryParam("sid", sessionId)
                .queryParam("variableSetId", variableSetId)
                .queryParam("studyId", studyId)
                .request().get(String.class);

        QueryResponse<Sample> responseSample = WSServerTestUtils.parseResult(json, Sample.class);

        List<Sample> samples = responseSample.getResponse().get(0).getResult();
        assertEquals("The number of samples that should be retrieved is 8", 8, samples.size());

        for (Sample sample : samples) {
            for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                assertTrue("The field name has not been properly propagated", annotationSet.getAnnotations().entrySet()
                        .stream()
                        .filter(annotation -> variable.getId().equals(annotation.getKey()))
                        .findAny()
                        .isPresent());
            }
        }


        // Now the variable is not mandatory
        objectMapper = new ObjectMapper();
        variable.setRequired(false);
        variable.setId("OTHER_ID");
        variable.setDefaultValue("other default value");

        json = webTarget.path("variableSet").path(String.valueOf(variableSetId)).path("field").path("add")
                .queryParam("sid", sessionId).request().post(Entity.json(variable), String.class);

        response = WSServerTestUtils.parseResult(json, VariableSet.class);

        variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertTrue("The field of the variableSet was not inserted",
                variables
                        .stream()
                        .filter(variable1 -> variable.getId().equals(variable1.getId()))
                        .findAny()
                        .isPresent());

        // Check that the insertion has been propagated properly to the annotations
        json = webTarget.path("samples").path("search")
                .queryParam("sid", sessionId)
                .queryParam("variableSetId", variableSetId)
                .queryParam("studyId", studyId)
                .request().get(String.class);

        responseSample = WSServerTestUtils.parseResult(json, Sample.class);

        samples = responseSample.getResponse().get(0).getResult();

        for (Sample sample : samples) {
            for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                assertFalse("The field name has been unnecessarily propagated", annotationSet.getAnnotations().entrySet()
                        .stream()
                        .filter(annotation -> variable.getId().equals(annotation.getKey()))
                        .findAny()
                        .isPresent());
            }
        }
    }

    @Test
    public void renameFieldFromVariableSet() throws Exception {
        String json = webTarget.path("variableSet").path(String.valueOf(variableSetId)).path("field").path("rename")
                .queryParam("sid", sessionId)
                .queryParam("oldName", "PHEN")
                .queryParam("newName", "PHEN_renamed")
                .request().get(String.class);

        QueryResponse<VariableSet> response = WSServerTestUtils.parseResult(json, VariableSet.class);

        Set<Variable> variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertTrue("The field of the variableSet was not renamed",
                variables
                        .stream()
                        .filter(variable -> "PHEN_renamed".equals(variable.getId()))
                        .findAny()
                        .isPresent());
        assertFalse("The old field of the variableSet is still present",
                variables
                        .stream()
                        .filter(variable -> "PHEN".equals(variable.getId()))
                        .findAny()
                        .isPresent());

        // Check that the renaming has been propagated properly to the annotations
        json = webTarget.path("samples").path("search")
                .queryParam("sid", sessionId)
                .queryParam("variableSetId", variableSetId)
                .queryParam("studyId", studyId)
                .request().get(String.class);

        QueryResponse<Sample> responseSample = WSServerTestUtils.parseResult(json, Sample.class);

        List<Sample> samples = responseSample.getResponse().get(0).getResult();
        assertEquals("The number of samples that should be retrieved is 8", 8, samples.size());

        for (Sample sample : samples) {
            for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                assertTrue("The field name of the annotations was not properly propagated", annotationSet.getAnnotations().entrySet()
                        .stream()
                        .filter(annotation -> "PHEN_renamed".equals(annotation.getKey()))
                        .findAny()
                        .isPresent());
                assertFalse("The old field name is still present in the annotations", annotationSet.getAnnotations().entrySet()
                        .stream()
                        .filter(annotation -> "PHEN".equals(annotation.getKey()))
                        .findAny()
                        .isPresent());
            }
        }

    }

    @Test
    public void deleteFieldFromVariableSet() throws Exception {
        String json = webTarget.path("variableSet").path(String.valueOf(variableSetId)).path("field").path("delete")
                .queryParam("sid", sessionId)
                .queryParam("name", "PHEN")
                .request().get(String.class);

        QueryResponse<VariableSet> response = WSServerTestUtils.parseResult(json, VariableSet.class);

        Set<Variable> variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertFalse("The field of the variableSet was not removed",
                variables
                        .stream()
                        .filter(variable -> "PHEN".equals(variable.getId()))
                        .findAny()
                        .isPresent());

        // Check that the remove has been propagated properly to the annotations
        json = webTarget.path("samples").path("search")
                .queryParam("sid", sessionId)
                .queryParam("variableSetId", variableSetId)
                .queryParam("studyId", studyId)
                .request().get(String.class);

        QueryResponse<Sample> responseSample = WSServerTestUtils.parseResult(json, Sample.class);

        List<Sample> samples = responseSample.getResponse().get(0).getResult();
        assertEquals("The number of samples that should be retrieved is 8", 8, samples.size());

        for (Sample sample : samples) {
            for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                assertFalse("The field name is still present in the annotations", annotationSet.getAnnotations().entrySet()
                        .stream()
                        .filter(annotation -> "PHEN".equals(annotation.getKey()))
                        .findAny()
                        .isPresent());
            }
        }

    }

}
