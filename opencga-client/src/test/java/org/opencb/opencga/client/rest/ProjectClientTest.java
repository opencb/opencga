package org.opencb.opencga.client.rest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by swaathi on 10/05/16.
 */
public class ProjectClientTest {
    private OpenCGAClient openCGAClient;
    private ProjectClient projectClient;
    private ClientConfiguration clientConfiguration;

    private static WSTestServer wsTestServer;

    public ProjectClientTest() throws CatalogException {
        try {
            clientConfiguration = ClientConfiguration.load(getClass().getResourceAsStream("/client-configuration.yml"));
//            clientConfiguration.getRest().setHost("http://localhost:8890/opencga/webservices/rest");
            openCGAClient = new OpenCGAClient("swaathi", "swaathi", clientConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    static public void initServer() throws Exception {
        wsTestServer = new WSTestServer();
        wsTestServer.initServer();

//        wsTestServer.setUp();
//        webTarget = serverTestUtils.getWebTarget();
//        sessionId = OpenCGAWSServer.catalogManager.login("user", CatalogManagerTest.PASSWORD, "localhost").first().getString("sessionId");
//        studyId = OpenCGAWSServer.catalogManager.getStudyId("user@1000G:phase1");
//        in1 = OpenCGAWSServer.catalogManager.createIndividual(studyId, "in1", "f1", -1, -1, null, null, sessionId).first().getId();
//        s1 = OpenCGAWSServer.catalogManager.createSample(studyId, "s1", "f1", "", null, null, sessionId).first().getId();
//        s2 = OpenCGAWSServer.catalogManager.createSample(studyId, "s2", "f1", "", null, null, sessionId).first().getId();
//        s3 = OpenCGAWSServer.catalogManager.createSample(studyId, "s3", "f1", "", null, null, sessionId).first().getId();
//        s4 = OpenCGAWSServer.catalogManager.createSample(studyId, "s4", "f1", "", null, null, sessionId).first().getId();
    }

    @AfterClass
    static public void shutdownServer() throws Exception {
        wsTestServer.shutdownServer();
    }

    @Before
    public void init() throws Exception {
//        clientConfiguration = ClientConfiguration.load(getClass().getResourceAsStream("/client-configuration-test.yml"));
//        clientConfiguration.getRest().setHost("http://localhost:8890/opencga/webservices/rest/");
//        openCGAClient = new OpenCGAClient("imedina", "pepe", clientConfiguration);
    }

    @Test
    public void get() throws Exception {
        projectClient = openCGAClient.getProjectClient();
        QueryResponse<Project> info = projectClient.get("25", null);
        assertNotNull(info.firstResult());
    }

    @Test
    public void getStudies() throws Exception {
        projectClient = openCGAClient.getProjectClient();
        QueryResponse<Study> info = projectClient.getStudies("25", null);
        assertNotNull(info.firstResult());
    }

    @Test
    public void delete() throws Exception {
        projectClient = openCGAClient.getProjectClient();
        QueryResponse<Project> delete = projectClient.delete("28", null);
        assertEquals("PENDING", delete.getError());
    }

}
