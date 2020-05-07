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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by jacobo on 25/06/15.
 */
public class SampleWSServerTest {

    private static WSServerTestUtils serverTestUtils;
    private WebTarget webTarget;
    private String sessionId;
    private String studyId = "user@1000G:phase1";
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
        sessionId = OpenCGAWSServer.catalogManager.getUserManager().login("user", CatalogManagerTest.PASSWORD).getToken();
        in1 = OpenCGAWSServer.catalogManager.getIndividualManager().create(studyId, new Individual().setId("in1"), null,
                sessionId).first().getUid();
        s1 = OpenCGAWSServer.catalogManager.getSampleManager().create(studyId, new Sample().setId("s1"), null, sessionId).first().getUid();
        s2 = OpenCGAWSServer.catalogManager.getSampleManager().create(studyId, new Sample().setId("s2"), null, sessionId).first().getUid();
        s3 = OpenCGAWSServer.catalogManager.getSampleManager().create(studyId, new Sample().setId("s3"), null, sessionId).first().getUid();
        s4 = OpenCGAWSServer.catalogManager.getSampleManager().create(studyId, new Sample().setId("s4"), null, sessionId).first().getUid();
    }

    @After
    public void after() throws Exception {
        // It is here to avoid restarting the server again and again
        serverTestUtils.setUp();
    }

    @Test
    public void search() throws IOException {
        String json = webTarget.path("samples").path("search").queryParam("sid", sessionId)
                .queryParam(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .queryParam(SampleDBAdaptor.QueryParams.ID.key(), "s1")
                .request().get(String.class);

        QueryResult<Sample> queryResult = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0);
        assertEquals(1, queryResult.getNumTotalResults());
        Sample sample = queryResult.first();
        assertEquals("s1", sample.getId());

//        json = webTarget.path("samples").path("search").queryParam("sid", sessionId)
//                .queryParam(CatalogSampleDBAdaptor.SampleFilterOption.studyId.toString(), studyId)
//                .queryParam(MongoDBCollection.SKIP, 1).queryParam(MongoDBCollection.LIMIT, 2)
//                .request().get(String.class);
//
//        queryResult = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0);
    }

//    @Test
//    public void updateGet() throws IOException {
//        String json = webTarget.path("samples").path(Long.toString(s1)).path("update")
//                .queryParam("individualId", in1).queryParam("sid", sessionId).request().get(String.class);
//
//        Sample sample = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0).first();
//        assertEquals(in1, sample.getIndividual().getUid());
//    }
//
//    @Test
//    public void updatePost() throws IOException {
//        SampleWSServer.UpdateSamplePOST entity = new SampleWSServer.UpdateSamplePOST();
//        entity.individualId = Long.toString(in1);
//        entity.attributes = Collections.singletonMap("key", "value");
//        String json = webTarget.path("samples").path(Long.toString(s1)).path("update").queryParam("sid", sessionId)
//                .request().post(Entity.json(entity), String.class);
//
//        Sample sample = WSServerTestUtils.parseResult(json, Sample.class).getResponse().get(0).first();
//        assertEquals(entity.individualId, sample.getIndividual().getUid());
//        assertEquals(entity.attributes, sample.getAttributes());
//    }

    /*       COHORT TESTS !!        */
    @Test
    public void createEmptyCohort() throws IOException {
        String json = webTarget.path("cohorts").path("create")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("name", "Name")
                .queryParam("type", Enums.CohortType.COLLECTION)
                .request().get(String.class);
        Cohort c = WSServerTestUtils.parseResult(json, Cohort.class).getResponse().get(0).first();
        assertEquals(0, c.getSamples().size());
        assertEquals(Enums.CohortType.COLLECTION, c.getType());
    }

    @Test
    public void createCohort() throws IOException {
        String json = webTarget.path("cohorts").path("create")
                .queryParam("sid", sessionId).queryParam("studyId", studyId).queryParam("name", "Name")
                .queryParam("type", Enums.CohortType.FAMILY).queryParam("sampleIds", s1 + "," + s2 + "," + s3 + "," + s4)
                .request().get(String.class);
        Cohort c = WSServerTestUtils.parseResult(json, Cohort.class).getResponse().get(0).first();
        assertEquals(4, c.getSamples().size());
        assertEquals(Enums.CohortType.FAMILY, c.getType());
    }

}
