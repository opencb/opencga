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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;

import javax.ws.rs.client.WebTarget;

/**
 * Created by jacobo on 23/06/15.
 */
public class JobWSServerTest {

    private static WSServerTestUtils serverTestUtils;
    private WebTarget webTarget;
    private String studyId = "user@1000G:phase1";
    private String sessionId;

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
        sessionId = OpenCGAWSServer.catalogManager.getUserManager().login("user", CatalogManagerTest.PASSWORD);
    }

    @After
    public void after() throws Exception {
        // It is here to avoid restarting the server again and again
        serverTestUtils.setUp();
    }

//    @Test
//    public void createReadyJobPostTest() throws CatalogException, IOException {
//        File folder = OpenCGAWSServer.catalogManager.getFileManager().search(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams
//                .TYPE.key(), File.Type.DIRECTORY), new QueryOptions(), sessionId).first();
//        String jobName = "MyJob";
//        String toolName = "samtools";
//        String description = "A job";
//        String commandLine = "samtools --do-magic";
//        String json = webTarget.path("jobs").path("create")
//                .queryParam("studyId", studyId)
//                .queryParam("sid", sessionId)
//                .request().post(Entity.json(new JobWSServer.InputJob(jobName, jobName, description, commandLine, Collections.emptyMap(),
//                        new Job.JobStatus(Job.JobStatus.DONE))), String.class);
//
//        RestResponse<Job> response = WSServerTestUtils.parseResult(json, Job.class);
//        Job job = response.getResponses().get(0).first();
//
//        assertEquals(jobName, job.getName());
//        assertEquals(description, job.getDescription());
//        assertEquals(commandLine, job.getCommandLine());
//        assertEquals(status.toString(), job.getStatus().getName());
//        assertEquals(outDirId, job.getOutDir().getUid());
//    }
//
//    @Test
//    public void createErrorJobPostTest() throws CatalogException, IOException {
//        File folder = OpenCGAWSServer.catalogManager.getFileManager().search(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams
//                .TYPE.key(), File.Type.DIRECTORY), new QueryOptions(), sessionId).first();
//        String jobName = "MyJob";
//        String toolName = "samtools";
//        String description = "A job";
//        String commandLine = "samtools --do-magic";
//        JobWSServer.InputJob.Status status = JobWSServer.InputJob.Status.ERROR;
//        long outDirId = folder.getUid();
//        String json = webTarget.path("jobs").path("create")
//                .queryParam("studyId", studyId)
//                .queryParam("sid", sessionId)
//                .request().post(Entity.json(new JobWSServer.InputJob(jobName, jobName, toolName, description, 10, 20, commandLine,
//                        status, Long.toString(outDirId), Collections.emptyList(), null, null)), String.class);
//
//        QueryResponse<Job> response = WSServerTestUtils.parseResult(json, Job.class);
//        Job job = response.getResponse().get(0).first();
//
//        assertEquals(jobName, job.getName());
//        assertEquals(toolName, job.getToolId());
//        assertEquals(description, job.getDescription());
//        assertEquals(10, job.getStartTime());
//        assertEquals(20, job.getEndTime());
//        assertEquals(commandLine, job.getCommandLine());
//        assertEquals(status.toString(), job.getStatus().getName());
//        assertEquals(outDirId, job.getOutDir().getUid());
//    }
//
//    @Test
//    public void createBadJobPostTest() throws CatalogException, IOException {
//        File folder = OpenCGAWSServer.catalogManager.getFileManager().search(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams
//                .TYPE.key(), File.Type.DIRECTORY), new QueryOptions(), sessionId).first();
//        String toolName = "samtools";
//        String description = "A job";
//        String commandLine = "samtools --do-magic";
//        JobWSServer.InputJob.Status status = JobWSServer.InputJob.Status.READY;
//        long outDirId = folder.getUid();
//
//        thrown.expect(Exception.class);
//        webTarget.path("jobs").path("create")
//                .queryParam("studyId", studyId)
//                .queryParam("sid", sessionId)
//                .request().post(Entity.json(new JobWSServer.InputJob(null, null, toolName, description, 10, 20, commandLine,
//                        status, Long.toString(outDirId), Collections.emptyList(), null, null)), String.class);
//    }

}
