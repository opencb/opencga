package org.opencb.opencga.server.ws;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 11/04/16.
 */
public class VariableSetWSServerTest {

    private WebTarget webTarget;
    private static WSServerTestUtils serverTestUtils;
    private String sessionId;
    private long studyId;
    private long variableSetId;

    public VariableSetWSServerTest() {
    }

    void setWebTarget(WebTarget webTarget) {
        this.webTarget = webTarget;
    }

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
        variableSetId = OpenCGAWSServer.catalogManager.getAllVariableSet(studyId, null, sessionId).first().getId();

    }

    @Test
    public void renameFieldFromVariableSet() throws Exception {
        String json = webTarget.path("variables").path(String.valueOf(variableSetId)).path("field").path("rename")
                .queryParam("sid", sessionId)
                .queryParam("oldName", "PHEN")
                .queryParam("newName", "PHEN_renamed")
                .request().get(String.class);

        QueryResponse<QueryResult<VariableSet>> response = WSServerTestUtils.parseResult(json, VariableSet.class);

        Set<Variable> variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertTrue("The field of the variableSet was not renamed",
                variables
                        .stream()
                        .filter(variable -> "PHEN_renamed".equals(variable.getId()))
                        .findAny()
                        .isPresent());
        assertFalse("The old field of the variableSet is still present",
                variables
                        .stream()
                        .filter(variable -> "PHEN".equals(variable.getId()))
                        .findAny()
                        .isPresent());

        // Check that the renaming has been propagated properly to the annotations
        json = webTarget.path("samples").path("search")
                .queryParam("sid", sessionId)
                .queryParam("variableSetId", variableSetId)
                .queryParam("studyId", studyId)
                .request().get(String.class);

        QueryResponse<QueryResult<Sample>> responseSample = WSServerTestUtils.parseResult(json, Sample.class);

        List<Sample> samples = responseSample.getResponse().get(0).getResult();
        assertEquals("The number of samples that should be retrieved is 8", 8, samples.size());

        for (Sample sample : samples) {
            for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                assertTrue("The field name of the annotations was not properly propagated", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> "PHEN_renamed".equals(annotation.getId()))
                        .findAny()
                        .isPresent());
                assertFalse("The old field name is still present in the annotations", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> "PHEN".equals(annotation.getId()))
                        .findAny()
                        .isPresent());
            }
        }

    }

    @Test
    public void deleteFieldFromVariableSet() throws Exception {
        String json = webTarget.path("variables").path(String.valueOf(variableSetId)).path("field").path("delete")
                .queryParam("sid", sessionId)
                .queryParam("name", "PHEN")
                .request().get(String.class);

        QueryResponse<QueryResult<VariableSet>> response = WSServerTestUtils.parseResult(json, VariableSet.class);

        Set<Variable> variables = response.getResponse().get(0).getResult().get(0).getVariables();
        assertFalse("The field of the variableSet was not removed",
                variables
                        .stream()
                        .filter(variable -> "PHEN".equals(variable.getId()))
                        .findAny()
                        .isPresent());

        // Check that the remove has been propagated properly to the annotations
        json = webTarget.path("samples").path("search")
                .queryParam("sid", sessionId)
                .queryParam("variableSetId", variableSetId)
                .queryParam("studyId", studyId)
                .request().get(String.class);

        QueryResponse<QueryResult<Sample>> responseSample = WSServerTestUtils.parseResult(json, Sample.class);

        List<Sample> samples = responseSample.getResponse().get(0).getResult();
        assertEquals("The number of samples that should be retrieved is 8", 8, samples.size());

        for (Sample sample : samples) {
            for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                assertFalse("The field name is still present in the annotations", annotationSet.getAnnotations()
                        .stream()
                        .filter(annotation -> "PHEN".equals(annotation.getId()))
                        .findAny()
                        .isPresent());
            }
        }

    }

}
