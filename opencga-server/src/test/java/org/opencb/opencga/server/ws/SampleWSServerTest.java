package org.opencb.opencga.server.ws;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Created by jacobo on 25/06/15.
 */
public class SampleWSServerTest {

    private static WSServerTestUtils serverTestUtils;
    private WebTarget webTarget;
    private String sessionId;
    private int studyId;
    private int in1;
    private int s1;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    static public void initServer() throws Exception {
        serverTestUtils = new WSServerTestUtils();
        serverTestUtils.initServer();
    }

    @AfterClass
    static public void shutdownServer() throws Exception {
        serverTestUtils.shutdownServer();
    }

    @Before
    public void init() throws Exception {
        serverTestUtils.setUp();
        webTarget = serverTestUtils.getWebTarget();
        sessionId = OpenCGAWSServer.catalogManager.login("user", CatalogManagerTest.PASSWORD, "localhost").first().getString("sessionId");
        studyId = OpenCGAWSServer.catalogManager.getStudyId("user@1000G:phase1");
        in1 = OpenCGAWSServer.catalogManager.createIndividual(studyId, "in1", "f1", -1, -1, null, null, sessionId).first().getId();
        s1 = OpenCGAWSServer.catalogManager.createSample(studyId, "s1", "f1", "", null, null, sessionId).first().getId();
    }

    @Test
    public void updateGet() throws IOException {
        String json = webTarget.path("samples").path(Integer.toString(s1)).path("update")
                .queryParam("individualId", in1).queryParam("sid", sessionId)
                .request().get(String.class);

        Sample sample = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0).first();
        assertEquals(in1, sample.getIndividualId());
    }

    @Test
    public void updatePost() throws IOException {
        SampleWSServer.UpdateSample entity = new SampleWSServer.UpdateSample();
        entity.individualId = in1;
        entity.attributes = Collections.singletonMap("key", "value");
        String json = webTarget.path("samples").path(Integer.toString(s1)).path("update").queryParam("sid", sessionId)
                .request().post(Entity.json(entity), String.class);

        Sample sample = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0).first();
        assertEquals(entity.individualId, sample.getIndividualId());
        assertEquals(entity.attributes, sample.getAttributes());
    }


}
