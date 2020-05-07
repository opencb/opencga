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

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.junit.*;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.models.user.User;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class OpenCGAWSServerTest {

    public static final String TEST_SERVER_USER = "test_server_user";
    private static WSServerTestUtils serverTestUtils;
    private WebTarget webTarget;

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

        //Drop default user mongoDB database.
        String databaseName = WSServerTestUtils.DATABASE_PREFIX + TEST_SERVER_USER + "_" + ProjectWSServerTest.PROJECT_ALIAS;
        MongoDataStoreManager dataStoreManager = new MongoDataStoreManager("localhost", 27017);
        dataStoreManager.get(databaseName);
        dataStoreManager.drop(databaseName);

//        serverTestUtils.setUp();
        webTarget = serverTestUtils.getWebTarget();

    }

    @After
    public void after() throws Exception {
        // It is here to avoid restarting the server again and again
        serverTestUtils.setUp();
    }

    /** First echo message to test Server connectivity **/
    @Test
    public void testConnectivity() throws InterruptedException, IOException {
        String message = "Test";
        WebTarget testPath = webTarget.path("test").path("echo").path(message);
        System.out.println("testPath = " + testPath);
        String s = testPath.request().get(String.class);
        assertEquals("Expected [" + message + "], actual [" + s + "]", message, s);

        testPath = webTarget.path("test").path("echo");
        System.out.println("testPath = " + testPath);
        MultiPart multiPart = new MultiPart();
        multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);
        FormDataBodyPart bodyPart = new FormDataBodyPart("message", message);
        multiPart.bodyPart(bodyPart);

        s = testPath.request().post(Entity.entity(multiPart, multiPart.getMediaType()), String.class);
        assertEquals("Expected [" + message + "], actual [" + s + "]", message, s);
    }

    /** User tests **/
    @Test
    public void userTests() throws IOException {
        UserWSServerTest userTest = new UserWSServerTest(webTarget);
        User user = userTest.createUser(TEST_SERVER_USER);
        String sessionId = userTest.loginUser(user.getId());
        userTest.updateUser(user.getId(), sessionId);
    }

    @Test
    public void workflowCreation() throws Exception {
//        UserWSServerTest userTest = new UserWSServerTest(webTarget);
//        User user = userTest.createUser(TEST_SERVER_USER);
//        String sessionId = userTest.loginUser(user.getId());
//        user = userTest.info(user.getId(), sessionId);
//
//        ProjectWSServerTest prTest = new ProjectWSServerTest(webTarget);
//        Project project = prTest.createProject(user.getId(), sessionId);
//        prTest.modifyProject(project.getId(), sessionId);
//        project = prTest.info(project.getId(), sessionId);
//        userTest.getAllProjects(user.getId(), sessionId);
//
//        StudyWSServerTest stTest = new StudyWSServerTest(webTarget);
//        Study study = stTest.createStudy(project.getId(), sessionId);
//        stTest.modifyStudy(study.getId(), sessionId);
//        study = stTest.info(study.getId(), sessionId);
//        prTest.getAllStudies(project.getId(), sessionId);
//
//        FileWSServerTest fileTest = new FileWSServerTest();
//        fileTest.setWebTarget(webTarget);
//        File fileVcf = fileTest.uploadVcf(study.getId(), sessionId);
//        assertEquals(File.FileStatus.READY, fileVcf.getStatus().getName());
//        assertEquals(File.Bioformat.VARIANT, fileVcf.getBioformat());
//        Job indexJobVcf = fileTest.index(fileVcf.getId(), sessionId);
//
//        /* Emulate DAEMON working */
//        indexJobVcf = runStorageJob(sessionId, indexJobVcf);
//        assertEquals(Job.JobStatus.READY, indexJobVcf.getStatus().getName());
//
//        QueryOptions queryOptions = new QueryOptions(QueryOptions.LIMIT, 10);
//        queryOptions.put("region", "1");
//        List<Sample> samples = OpenCGAWSServer.catalogManager.getAllSamples(study.getId(),
//                new Query(CatalogSampleDBAdaptor.QueryParams.ID.key(), fileVcf.getSampleIds()), new QueryOptions(), sessionId).getResult();
//        List<String> sampleNames = samples.stream().map(Sample::getName).collect(Collectors.toList());
//        List<Variant> variants = fileTest.fetchVariants(fileVcf.getId(), sessionId, queryOptions);
//        assertEquals(10, variants.size());
//        for (Variant variant : variants) {
//            for (StudyEntry sourceEntry : variant.getStudies()) {
//                assertEquals(sampleNames.size(), sourceEntry.getSamplesData().size());
//                assertNotNull("Stats must be calculated", sourceEntry.stats(StudyEntry.DEFAULT_COHORT));
//            }
//            assertNotNull("Must be annotated", variant.getAnnotation());
//        }
//
//        //Create a new user with permissions just over 2 samples.
//        String userTest2 = OpenCGAWSServer.catalogManager.createUser("userTest2", "userTest2", "my@email.com", "1234", "ACME", null, new QueryOptions()).first().getId();
//        String sessionId2 = OpenCGAWSServer.catalogManager.login(userTest2, "1234", "127.0.0.1").first().getString("sessionId");
//        OpenCGAWSServer.catalogManager.addUsersToGroup(study.getId(), AuthorizationManager.MEMBERS_ROLE, userTest2, sessionId);
//
//        QueryResult<Sample> allSamples = OpenCGAWSServer.catalogManager.getAllSamples(study.getId(),
//                new Query(CatalogSampleDBAdaptor.QueryParams.NAME.key(), "NA19685,NA19661"), new QueryOptions(), sessionId);
//        OpenCGAWSServer.catalogManager.shareSample(allSamples.getResult().get(0).getId() + "", "@" + AuthorizationManager.MEMBERS_ROLE,
//                new AclEntry("@" + AuthorizationManager.MEMBERS_ROLE, true, false, false, false), sessionId);
//        OpenCGAWSServer.catalogManager.shareSample(allSamples.getResult().get(1).getId() + "", userTest2,
//                new AclEntry(userTest2, true, false, false, false), sessionId);
//
//        variants = stTest.fetchVariants(study.getId(), sessionId2, queryOptions);
//        assertEquals(10, variants.size());
//        for (Variant variant : variants) {
//            for (StudyEntry sourceEntry : variant.getStudies()) {
//                assertEquals(2, sourceEntry.getSamplesData().size());
//                assertNotNull("Stats must be calculated", sourceEntry.stats(StudyEntry.DEFAULT_COHORT));
//            }
//            assertNotNull("Must be annotated", variant.getAnnotation());
//        }
//
//
//        Cohort myCohort = OpenCGAWSServer.catalogManager.createCohort(study.getId(), "MyCohort", Study.Type.FAMILY, "", samples.stream().map(Sample::getId).collect(Collectors.toList()), null, sessionId).first();
//        assertEquals(Cohort.CohortStatus.NONE, OpenCGAWSServer.catalogManager.getCohort(myCohort.getId(), null, sessionId).first().getStatus().getName());
//
//        long outputId = OpenCGAWSServer.catalogManager.getFileParent(fileVcf.getId(), null, sessionId).first().getId();
//        Job calculateVariantStatsJob = fileTest.calculateVariantStats(myCohort.getId(), outputId, sessionId);
//
//        /* Emulate DAEMON working */
//        calculateVariantStatsJob = runStorageJob(sessionId, calculateVariantStatsJob);
//        assertEquals(Job.JobStatus.READY, calculateVariantStatsJob.getStatus().getName());
//        assertEquals(Cohort.CohortStatus.READY, OpenCGAWSServer.catalogManager.getCohort(myCohort.getId(), null, sessionId).first().getStatus().getName());
//
//
//
//        File fileBam = fileTest.uploadBam(study.getId(), sessionId);
//        assertEquals(File.FileStatus.READY, fileBam.getStatus().getName());
//        assertEquals(File.Bioformat.ALIGNMENT, fileBam.getBioformat());
//        Job indexJobBam = fileTest.index(fileBam.getId(), sessionId);
//
//        /* Emulate DAEMON working */
//        indexJobBam = runStorageJob(sessionId, indexJobBam);
//        assertEquals(Job.JobStatus.READY, indexJobBam.getStatus().getName());
//
//        queryOptions = new QueryOptions(QueryOptions.LIMIT, 10);
//        queryOptions.put("region", "20:60000-60200");
//        queryOptions.put(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, false);
//        fileTest.fetchAlignments(fileBam.getId(), sessionId, queryOptions);
//        assertEquals(10, alignments.size());

    }

}
