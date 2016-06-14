package org.opencb.opencga.client.rest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by swaathi on 10/05/16.
 */
public class StudyClientTest {
    private OpenCGAClient openCGAClient;
    private StudyClient studyClient;
    private ClientConfiguration clientConfiguration;

    private static WSTestServer wsTestServer;

    public StudyClientTest() throws CatalogException {
        try {
            clientConfiguration = ClientConfiguration.load(getClass().getResourceAsStream("/client-configuration.yml"));
//            clientConfiguration.getRest().setHost("http://localhost:8890/opencga/webservices/rest");
            openCGAClient = new OpenCGAClient("hgva", "hgva_cafeina", clientConfiguration);
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
        studyClient = openCGAClient.getStudyClient();
        QueryResponse<Study> info = studyClient.get("2", null);
        assertNotNull(info.firstResult());
    }

    @Test
    public void getSamples() throws Exception {
        studyClient = openCGAClient.getStudyClient();
        QueryOptions queryOptions = new QueryOptions(QueryOptions.LIMIT, 1361);
        queryOptions.put(QueryOptions.SKIP, 11);
        QueryResponse<Sample> info = studyClient.getSamples("2", queryOptions);
        System.out.println(info.first().getNumResults());
        System.out.println(info.first().getResult().size());
        System.out.println(info.first().getNumTotalResults());
        assertNotNull(info.firstResult());
    }

    @Test
    public void getFiles() throws Exception {
        studyClient = openCGAClient.getStudyClient();
        QueryResponse<File> info = studyClient.getFiles("2", null);
        assertNotNull(info.firstResult());
    }

}
