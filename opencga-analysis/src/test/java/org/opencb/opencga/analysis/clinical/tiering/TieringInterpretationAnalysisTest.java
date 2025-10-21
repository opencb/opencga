package org.opencb.opencga.analysis.clinical.tiering;

import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.clinical.exomiser.ExomiserInterpretationAnalysis;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest.TIERING_MODE;

@Category(MediumTests.class)
public class TieringInterpretationAnalysisTest {

    private static AbstractClinicalManagerTest clinicalTest;
    private static ResourceManager resourceManager;

    Path outDir;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(true);

    @BeforeClass
    public static void setUp() throws Exception {
        opencga.clear();
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(opencga, TIERING_MODE);
        resourceManager = new ResourceManager(opencga.getOpencgaHome());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        opencga.clear();
    }

    @Test
    public void tieringAnalysis() throws IOException, CatalogException, ToolException {
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis_tiering"));

        OpenCGAResult<ClinicalAnalysis> caResult = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token);
        assertEquals(1, caResult.getNumResults());
        assertEquals(0, caResult.first().getSecondaryInterpretations().size());

        // Create the list of panel IDs from the clinical analysis
        List<String> panelIds = caResult.first().getPanels().stream().map(panel -> panel.getId()).collect(Collectors.toList());

        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());
        System.out.println("outDir = " + outDir);

        TieringInterpretationAnalysis tiering = new TieringInterpretationAnalysis();

        tiering.setUp(opencga.getOpencgaHome().toAbsolutePath().toString(), new ObjectMap(), outDir, clinicalTest.token);
        tiering.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.CA_OPA)
                .setDiseasePanelIds(panelIds);
        ExecutionResult result = tiering.start();
        System.out.println(result);


        // Refresh clinical analysis
        ClinicalAnalysis clinicalAnalysis = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn,
                clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token).first();
        System.out.println("clinicalAnalysis.getId() = " + clinicalAnalysis.getId());
//        assertEquals(1, clinicalAnalysis.getSecondaryInterpretations().size());
//        assertTrue(clinicalAnalysis.getSecondaryInterpretations().get(0).getPrimaryFindings().size() > 0);
//
//        // Check Exomiser docker CLI
//        boolean pedFound = false;
//        for (Event event : result.getEvents()) {
//            if (event.getType() == Event.Type.WARNING && StringUtils.isNotEmpty(event.getMessage())
//                    && event.getMessage().startsWith(DockerWrapperAnalysisExecutor.DOCKER_CLI_MSG)) {
//                List<String> splits = Arrays.asList(event.getMessage().split(" "));
//                pedFound = splits.contains("--ped") && splits.contains("/jobdir/" + clinicalTest.PROBAND_ID2 + ".ped");
//            }
//        }
//        assertFalse(pedFound);
//
//        // Only proband sample is returned in primary findings
//        for (ClinicalVariant cv : clinicalAnalysis.getInterpretation().getPrimaryFindings()) {
//            assertEquals(1, cv.getStudies().get(0).getSamples().size());
//        }
    }
}
