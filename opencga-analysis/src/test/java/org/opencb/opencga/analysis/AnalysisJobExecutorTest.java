package org.opencb.opencga.analysis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.utils.StringUtils;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.execution.AnalysisJobExecutor;
import org.opencb.opencga.analysis.execution.model.Manifest;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.models.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;

/**
 * Created by hpccoll1 on 21/07/15.
 */
public class AnalysisJobExecutorTest {
    private CatalogManager catalogManager;
    private String sessionId;
    private final String userId = "user";
    private int projectId;
    private int studyId;
    private File output;
    private URI temporalOutDirUri;


    @Before
    public void before() throws Exception {
        Properties properties = new Properties();
        properties.load(CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties"));

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject(userId, "p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, "Study 1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", DB_NAME)), null, null, null, sessionId).first().getId();
        output = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first();

        temporalOutDirUri = catalogManager.createJobOutDir(studyId, "JOB_TMP", sessionId);
    }

    @Test
    public void loadManifestTest() throws Exception {
        Path inputPath = Paths.get(getClass().getResource("/manifest.json").toURI());
        ObjectMapper objectMapper = new ObjectMapper();
        Manifest manifest = objectMapper.readValue(inputPath.toFile(), Manifest.class);
        assertNotNull("Manifest object is null", manifest);
    }

    @Test
    public void executeLocalSuccess() throws Exception {
        String helloWorld = "Hello World!";
        Job job = AnalysisJobExecutor.createJob(catalogManager, studyId, "myJob", "bash", "A simple success job", output, Collections.<Integer>emptyList(), sessionId,
                StringUtils.randomString(5), temporalOutDirUri, "echo " + helloWorld, true, false, Collections.emptyMap(), Collections.emptyMap()).first();

        assertEquals(Job.Status.READY, job.getStatus());
        assertEquals(2, job.getOutput().size());
        for (Integer fileId : job.getOutput()) {
            File file = catalogManager.getFile(fileId, sessionId).first();
            if (file.getName().contains("out")) {
                String contentFile = new BufferedReader(new InputStreamReader(catalogManager.downloadFile(fileId, sessionId))).readLine();
                assertEquals(helloWorld, contentFile);
            }
        }
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }

    @Test
    public void executeLocalError1() throws Exception {
        Job job = AnalysisJobExecutor.createJob(catalogManager, studyId, "myJob", "bash", "A simple success job", output, Collections.<Integer>emptyList(), sessionId,
                StringUtils.randomString(5), temporalOutDirUri, "unexisting_tool ", true, false, Collections.emptyMap(), Collections.emptyMap()).first();

        assertEquals(Job.Status.ERROR, job.getStatus());
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }


    @Test
    public void executeLocalError2() throws Exception {
        Job job = AnalysisJobExecutor.createJob(catalogManager, studyId, "myJob", "bash", "A simple success job", output, Collections.<Integer>emptyList(), sessionId,
                StringUtils.randomString(5), temporalOutDirUri, "false ", true, false, Collections.emptyMap(), Collections.emptyMap()).first();

        assertEquals(Job.Status.ERROR, job.getStatus());
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }

    @Test
    public void executeLocalInterrupt() throws Exception {
        Thread thread = new Thread(() -> {
            try {
                AnalysisJobExecutor.createJob(catalogManager, studyId, "myJob", "bash", "A simple success job", output, Collections.<Integer>emptyList(), sessionId,
                        StringUtils.randomString(5), temporalOutDirUri, "sleep 20 ", true, false, Collections.emptyMap(), Collections.emptyMap()).first();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        Thread.sleep(1000);
        thread.interrupt();
        thread.join();
        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionId);
        assertEquals(1, allJobs.getNumResults());
        Job job = allJobs.first();
        assertEquals(Job.Status.ERROR, job.getStatus());
        assertFalse(catalogManager.getCatalogIOManagerFactory().get(temporalOutDirUri).exists(temporalOutDirUri));
    }

}