package org.opencb.opencga.analysis.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Study;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ToolExecutorTest extends GenericTest {

//    public final static String PASSWORD = "asdf";
//    public final static String STUDY = "user@1000G:phase1";
//    @Rule
//    public ExpectedException thrown = ExpectedException.none();
//
//    @Rule
//    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();
//
//    protected CatalogManager catalogManager;
//    protected String sessionIdUser;
//
//    @Before
//    public void setUp() throws IOException, CatalogException {
//        catalogManager = catalogManagerResource.getCatalogManager();
//        setUpCatalogManager(catalogManager);
//    }
//
//    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
//        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
//        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);
//
//        String projectStr = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens", null,
//              null, "GRCh38", new QueryOptions(), sessionIdUser).first().getFqn();
//        catalogManager.getStudyManager().create(projectStr, "phase1", null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null, null, null, null, null, sessionIdUser);
//    }
//
//
//    @After
//    public void tearDown() throws Exception {
//    }
//
//
//    @Test
//    public void execute() throws Exception {
//        Path testBam = Paths.get(getClass().getResource("/test.bam").toURI());
//        Path toolDir = Paths.get(getClass().getResource("/tools").toURI());
//        Path tmp = Paths.get("/tmp");
//
//        catalogManager.getFileManager().link(STUDY, testBam.toUri(), "bams/", new ObjectMap("parents", true), sessionIdUser);
//
//        Map<String, String> params = new HashMap<>();
//        params.put("input", "test.bam");
//        params.put("output", "test.bam.bai");
//
//        DataResult<Job> jobQueryResult = catalogManager.getJobManager().create(STUDY, "jobName", "", "samtools", "index",
//                "bams/", params, sessionIdUser);
//
//        catalogManager.getConfiguration().setToolDir(toolDir.toString());
//        ToolAnalysis toolAnalysis = new ToolAnalysis(catalogManager.getConfiguration());
//        toolAnalysis.execute(jobQueryResult.first().getUid(), sessionIdUser);
//
//        ObjectReader reader = new ObjectMapper().reader(Status.class);
//        Status status = reader.readValue(tmp.resolve("status.json").toFile());
//        assertEquals(Status.DONE, status.getName());
//
//        assertTrue(tmp.resolve("test.bam.bai").toFile().exists());
//    }

}