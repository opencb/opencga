package org.opencb.opencga.server.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.VariableSet;

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
    private long studyId;
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
        sessionId = OpenCGAWSServer.catalogManager.login("user", CatalogManagerTest.PASSWORD, "localhost").first().getString("sessionId");
        studyId = OpenCGAWSServer.catalogManager.getStudyId("user@1000G:phase1");
        variableSetId = OpenCGAWSServer.catalogManager.getAllVariableSet(studyId, null, sessionId).first().getId();

    }

    @Test
    public void addFieldToVariableSet() throws Exception {
        objectMapper = new ObjectMapper();
        Variable variable = new Variable("MY_VARIABLE", "", Variable.VariableType.TEXT, "whatever", true, false, null, 6L, "", "", null,
                null);
        String json = webTarget.path("variableSet").path(String.valueOf(variableSetId)).path("field").path("add")
                .queryParam("sid", sessionId).request().post(Entity.json(variable), String.class);

        QueryResponse<VariableSet> response = WSServerTestUtils.parseResult(json, VariableSet.class);

        Set<Variable> variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertTrue("The field of the variableSet was not inserted",
                variables
                        .stream()
                        .filter(variable1 -> variable.getName().equals(variable1.getName()))
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
                assertTrue("The field name has not been properly propagated", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> variable.getName().equals(annotation.getName()))
                        .findAny()
                        .isPresent());
            }
        }


        // Now the variable is not mandatory
        objectMapper = new ObjectMapper();
        variable.setRequired(false);
        variable.setName("OTHER_ID");
        variable.setDefaultValue("other default value");

        json = webTarget.path("variableSet").path(String.valueOf(variableSetId)).path("field").path("add")
                .queryParam("sid", sessionId).request().post(Entity.json(variable), String.class);

        response = WSServerTestUtils.parseResult(json, VariableSet.class);

        variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertTrue("The field of the variableSet was not inserted",
                variables
                        .stream()
                        .filter(variable1 -> variable.getName().equals(variable1.getName()))
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
                assertFalse("The field name has been unnecessarily propagated", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> variable.getName().equals(annotation.getName()))
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
                        .filter(variable -> "PHEN_renamed".equals(variable.getName()))
                        .findAny()
                        .isPresent());
        assertFalse("The old field of the variableSet is still present",
                variables
                        .stream()
                        .filter(variable -> "PHEN".equals(variable.getName()))
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
                assertTrue("The field name of the annotations was not properly propagated", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> "PHEN_renamed".equals(annotation.getName()))
                        .findAny()
                        .isPresent());
                assertFalse("The old field name is still present in the annotations", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> "PHEN".equals(annotation.getName()))
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
                        .filter(variable -> "PHEN".equals(variable.getName()))
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
                assertFalse("The field name is still present in the annotations", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> "PHEN".equals(annotation.getName()))
                        .findAny()
                        .isPresent());
            }
        }

    }

}
