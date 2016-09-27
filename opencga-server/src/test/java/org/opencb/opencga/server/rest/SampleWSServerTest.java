package org.opencb.opencga.server.rest;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by jacobo on 25/06/15.
 */
public class SampleWSServerTest {

    private static WSServerTestUtils serverTestUtils;
    private WebTarget webTarget;
    private String sessionId;
    private long studyId;
    private long in1;
    private long s1, s2, s3, s4;

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
        s1 = OpenCGAWSServer.catalogManager.createSample(studyId, "s1", "f1", "", null, null, sessionId).first().getId();
        s2 = OpenCGAWSServer.catalogManager.createSample(studyId, "s2", "f1", "", null, null, sessionId).first().getId();
        s3 = OpenCGAWSServer.catalogManager.createSample(studyId, "s3", "f1", "", null, null, sessionId).first().getId();
        s4 = OpenCGAWSServer.catalogManager.createSample(studyId, "s4", "f1", "", null, null, sessionId).first().getId();
    }

    @Test
    public void search() throws IOException {
        String json = webTarget.path("samples").path("search").queryParam("sid", sessionId)
                .queryParam(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .queryParam(SampleDBAdaptor.QueryParams.NAME.key(), "s1")
                .request().get(String.class);

        QueryResult<Sample> queryResult = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0);
        assertEquals(1, queryResult.getNumTotalResults());
        Sample sample = queryResult.first();
        assertEquals("s1", sample.getName());

//        json = webTarget.path("samples").path("search").queryParam("sid", sessionId)
//                .queryParam(CatalogSampleDBAdaptor.SampleFilterOption.studyId.toString(), studyId)
//                .queryParam(MongoDBCollection.SKIP, 1).queryParam(MongoDBCollection.LIMIT, 2)
//                .request().get(String.class);
//
//        queryResult = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0);
    }

    @Test
    public void updateGet() throws IOException {
        String json = webTarget.path("samples").path(Long.toString(s1)).path("update")
                .queryParam("individualId", in1).queryParam("sid", sessionId).request().get(String.class);

        Sample sample = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0).first();
        assertEquals(in1, sample.getIndividualId());
    }

    @Test
    public void updatePost() throws IOException {
        SampleWSServer.UpdateSample entity = new SampleWSServer.UpdateSample();
        entity.individualId = in1;
        entity.attributes = Collections.singletonMap("key", "value");
        String json = webTarget.path("samples").path(Long.toString(s1)).path("update").queryParam("sid", sessionId)
                .request().post(Entity.json(entity), String.class);

        Sample sample = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0).first();
        assertEquals(entity.individualId, sample.getIndividualId());
        assertEquals(entity.attributes, sample.getAttributes());
    }

    /*       COHORT TESTS !!        */
    @Test
    public void createEmptyCohort() throws IOException {
        String json = webTarget.path("cohorts").path("create")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("name", "Name")
                .queryParam("type", Study.Type.COLLECTION)
                .request().get(String.class);
        Cohort c = WSServerTestUtils.parseResult(json, Cohort.class).getResponse().get(0).first();
        assertEquals(0, c.getSamples().size());
        assertEquals(Study.Type.COLLECTION, c.getType());
    }

    @Test
    public void createCohort() throws IOException {
        String json = webTarget.path("cohorts").path("create")
                .queryParam("sid", sessionId).queryParam("studyId", studyId).queryParam("name", "Name")
                .queryParam("type", Study.Type.FAMILY).queryParam("sampleIds", s1 + "," + s2 + "," + s3 + "," + s4)
                .request().get(String.class);
        Cohort c = WSServerTestUtils.parseResult(json, Cohort.class).getResponse().get(0).first();
        assertEquals(4, c.getSamples().size());
        assertEquals(Study.Type.FAMILY, c.getType());
    }

}
