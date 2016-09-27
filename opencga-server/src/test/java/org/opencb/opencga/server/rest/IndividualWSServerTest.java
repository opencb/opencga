package org.opencb.opencga.server.rest;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Individual;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by jacobo on 22/06/15.
 */
public class IndividualWSServerTest {

    private static WSServerTestUtils serverTestUtils;
    private WebTarget webTarget;
    private String sessionId;
    private long studyId;
    private long in1;
    private long in2;
    private long in3;
    private long in4;

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
        in1 = OpenCGAWSServer.catalogManager.createIndividual(studyId, "in1", "f1", -1, -1, null, null, sessionId).first().getId();
        in2 = OpenCGAWSServer.catalogManager.createIndividual(studyId, "in2", "f1", -1, -1, null, null, sessionId).first().getId();
        in3 = OpenCGAWSServer.catalogManager.createIndividual(studyId, "in3", "f2", -1, -1, null, null, sessionId).first().getId();
        in4 = OpenCGAWSServer.catalogManager.createIndividual(studyId, "in4", "f2", -1, -1, null, null, sessionId).first().getId();
    }


    @Test
    public void createIndividualTest() throws IOException {
        String json = webTarget.path("individuals").path("create")
                .queryParam("studyId", studyId)
                .queryParam("name", "new_individual1")
                .queryParam("sex", "FEMALE")
                .queryParam("family", "The Family Name")
                .queryParam("sid", sessionId).request().get(String.class);

        QueryResponse<Individual> response = WSServerTestUtils.parseResult(json, Individual.class);

        Individual individual = response.getResponse().get(0).first();
        assertEquals(Individual.Sex.FEMALE, individual.getSex());
        assertEquals("The Family Name", individual.getFamily());
        assertEquals("new_individual1", individual.getName());
        assertTrue(individual.getId() > 0);
    }

    @Test
    public void getIndividualTest() throws IOException {
        String json = webTarget.path("individuals").path(Long.toString(in1)).path("info")
                .queryParam("studyId", studyId)
                .queryParam("exclude", "projects.studies.individuals.sex")
                .queryParam("sid", sessionId).request().get(String.class);

        QueryResponse<Individual> response = WSServerTestUtils.parseResult(json, Individual.class);

        Individual individual = response.getResponse().get(0).first();
        assertEquals("f1", individual.getFamily());
        assertEquals(null, individual.getSex());
        assertTrue(individual.getId() > 0);
    }

    @Test
    public void searchIndividualTest() throws IOException {
        String json = webTarget.path("individuals").path("search")
                .queryParam("studyId", studyId)
                .queryParam("family", "f1")
                .queryParam("exclude", "projects.studies.individuals.sex")
                .queryParam("sid", sessionId).request().get(String.class);

        QueryResponse<Individual> response = WSServerTestUtils.parseResult(json, Individual.class);

        List<Individual> result = response.getResponse().get(0).getResult();

        assertEquals(2, result.size());
        for (Individual individual : result) {
            assertEquals("f1", individual.getFamily());
            assertEquals(null, individual.getSex());
            assertTrue(individual.getId() > 0);
        }
    }

    @Test
    public void updateIndividualTest() throws IOException {
        String json = webTarget.path("individuals").path(Long.toString(in1)).path("update")
                .queryParam("family", "f3")
                .queryParam("sid", sessionId).request().get(String.class);

        QueryResponse<Individual> response = WSServerTestUtils.parseResult(json, Individual.class);

        Individual individual = response.getResponse().get(0).first();
        assertEquals("f3", individual.getFamily());
    }

    @Test
    public void deleteIndividualTest() throws IOException, CatalogException {
        String json = webTarget.path("individuals").path(Long.toString(in1)).path("delete")
                .queryParam("sid", sessionId).request().get(String.class);

        QueryResponse<Individual> response = WSServerTestUtils.parseResult(json, Individual.class);

        Individual individual = response.getResponse().get(0).first();

        // FIXME: This will have to return an exception once we improve the delete behaviour
//        thrown.expect(CatalogException.class);
        OpenCGAWSServer.catalogManager.getIndividual(individual.getId(), null, sessionId);

    }

}
