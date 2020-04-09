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

package org.opencb.opencga.analysis;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.analysis.old.JobFactory;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;

/**
 * Created by hpccoll1 on 21/07/15.
 */
public class JobFactoryTest {
    private CatalogManager catalogManager;
    private String sessionId;
    private final String userId = "user";
    private long projectId;
    private long studyId;
    private File output;
    private URI temporalOutDirUri;

    @Rule
    public CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    @Before
    public void before() throws Exception {
        catalogManager = catalogManagerExternalResource.getCatalogManager();

        User user = catalogManager.getUserManager().create(userId, "User", "user@email.org", "user", "ACME", null, Account.Type.FULL, null).first();

        sessionId = catalogManager.getUserManager().login(userId, "user").getToken();
        projectId = catalogManager.getProjectManager().create("p1", "p1", "Project 1", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        studyId = catalogManager.getStudyManager().create(String.valueOf(projectId), "s1", "s1", Study.Type.CASE_CONTROL, null, "Study " +
                "1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", DB_NAME)), null,
                null, null, sessionId).first().getId();
        output = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("data", "index").toString(), null, false,
                null, QueryOptions.empty(), sessionId).first();

        temporalOutDirUri = catalogManager.getJobManager().createJobOutDir(studyId, "JOB_TMP", sessionId);
    }

    @Test
    public void executeLocalSuccess() throws Exception {
        String helloWorld = "Hello World!";
        Job job = new JobFactory(catalogManager).createJob(studyId, "myJob", "bash", "A simple success job", output, Collections.emptyList(), sessionId,
                StringUtils.randomString(5), temporalOutDirUri, "echo " + helloWorld, true, false, Collections.emptyMap(), Collections.emptyMap()).first();

        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
        assertEquals(2, job.getOutput().size());
        for (File fileAux : job.getOutput()) {
            File file = catalogManager.getFileManager().get(fileAux.getId(), null, sessionId).first();
            if (file.getName().contains("out")) {
                String contentFile = new BufferedReader(new InputStreamReader(catalogManager.getFileManager().download(fileAux.getId(),
                        -1, -1, null, sessionId))).readLine();
                assertEquals(helloWorld, contentFile);
            }
        }
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }

    @Test
    public void executeLocalError1() throws Exception {
        Job job = new JobFactory(catalogManager).createJob(studyId, "myJob", "bash", "A simple success job", output, Collections.emptyList(), sessionId,
                StringUtils.randomString(5), temporalOutDirUri, "unexisting_tool ", true, false, Collections.emptyMap(), Collections.emptyMap()).first();

        assertEquals(Job.JobStatus.ERROR, job.getStatus().getName());
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }


    @Test
    public void executeLocalError2() throws Exception {
        Job job = new JobFactory(catalogManager).createJob(studyId, "myJob", "bash", "A simple success job", output, Collections.emptyList(), sessionId,
                StringUtils.randomString(5), temporalOutDirUri, "false ", true, false, Collections.emptyMap(), Collections.emptyMap()).first();

        assertEquals(Job.JobStatus.ERROR, job.getStatus().getName());
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }

    @Test
    public void executeLocalInterrupt() throws Exception {
        Thread thread = new Thread(() -> {
            try {
                new JobFactory(catalogManager).createJob(studyId, "myJob", "bash", "A simple success job", output, Collections.emptyList(), sessionId,
                        StringUtils.randomString(5), temporalOutDirUri, "sleep 20 ", true, false, Collections.emptyMap(), Collections.emptyMap()).first();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        Thread.sleep(1000);
        thread.interrupt();
        thread.join();
        QueryResult<Job> allJobs = catalogManager.getJobManager().get(String.valueOf(studyId), (Query) null, null, sessionId);
        assertEquals(1, allJobs.getNumResults());
        Job job = allJobs.first();
        assertEquals(Job.JobStatus.ERROR, job.getStatus().getName());
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }

}