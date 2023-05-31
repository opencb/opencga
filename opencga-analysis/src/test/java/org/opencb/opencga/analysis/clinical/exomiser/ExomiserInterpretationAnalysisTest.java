package org.opencb.opencga.analysis.clinical.exomiser;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.junit.*;
import org.eclipse.jetty.util.Scanner;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

@Category(MediumTests.class)
public class ExomiserInterpretationAnalysisTest  {

    private static AbstractClinicalManagerTest clinicalTest;

    Path outDir;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(true);

    @BeforeClass
    public static void setUp() throws Exception {
        opencga.clearStorageDB();
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(opencga);
    }

    @After
    public void tearDown() throws Exception {
        opencga.clear();
    }

    @Test
    public void testNormalization() throws NonStandardCompliantSampleField {
        Variant normalized = new VariantNormalizer().normalize(Collections.singletonList(new Variant("12:878367:N:NT")), false).get(0);
        System.out.println("normalized = " + normalized.toStringSimple());
        assertEquals(878368, (long) normalized.getStart());
        assertEquals("", normalized.getReference());
        assertEquals("T", normalized.getAlternate());

        normalized = new VariantNormalizer().normalize(Collections.singletonList(new Variant("18:9887391:NGAAGCCATCCAGCCCAAGGAGGGTGACATCCCCAAGTCCCCAGAA:N")), false).get(0);
        System.out.println("normalized + " + normalized.toStringSimple());
        assertEquals(9887392, (long) normalized.getStart());
        assertEquals("GAAGCCATCCAGCCCAAGGAGGGTGACATCCCCAAGTCCCCAGAA", normalized.getReference());
        assertEquals("", normalized.getAlternate());
    }

    @Test
    public void singleExomiserAnalysis() throws IOException, CatalogException, ToolException {
        assumeThat(Paths.get("/opt/opencga/analysis/resources/exomiser").toFile().exists(), is(true));

        prepareExomiserData();
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis_single"));

        OpenCGAResult<ClinicalAnalysis> caResult = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_ID2, QueryOptions.empty(), clinicalTest.token);
        ClinicalAnalysis clinicalAnalysis = caResult.getResults().get(0);
        assertEquals(0, clinicalAnalysis.getSecondaryInterpretations().size());

        ExomiserInterpretationAnalysis exomiser = new ExomiserInterpretationAnalysis();

        exomiser.setUp(opencga.getOpencgaHome().toAbsolutePath().toString(), new ObjectMap(), outDir, clinicalTest.token);
        exomiser.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.CA_ID2);

        ExecutionResult result = exomiser.start();

        // Refresh clinical analysis
        clinicalAnalysis = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_ID2, QueryOptions.empty(), clinicalTest.token).first();
        assertEquals(1, clinicalAnalysis.getSecondaryInterpretations().size());
        assertEquals(22, clinicalAnalysis.getSecondaryInterpretations().get(0).getPrimaryFindings().size());
    }

    @Test
    public void familyExomiserAnalysis() throws IOException, CatalogException, ToolException {
        assumeThat(Paths.get("/opt/opencga/analysis/resources/exomiser").toFile().exists(), is(true));

        prepareExomiserData();
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis_family"));

        ClinicalAnalysis clinicalAnalysis = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_ID3, QueryOptions.empty(), clinicalTest.token).first();
        assertEquals(0, clinicalAnalysis.getSecondaryInterpretations().size());

        ExomiserInterpretationAnalysis exomiser = new ExomiserInterpretationAnalysis();

        exomiser.setUp(opencga.getOpencgaHome().toAbsolutePath().toString(), new ObjectMap(), outDir, clinicalTest.token);
        exomiser.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.CA_ID3);

        ExecutionResult result = exomiser.start();

        // Refresh clinical analysis
        clinicalAnalysis = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_ID3, QueryOptions.empty(), clinicalTest.token).first();
        assertEquals(1, clinicalAnalysis.getSecondaryInterpretations().size());
        assertEquals(2, clinicalAnalysis.getSecondaryInterpretations().get(0).getPrimaryFindings().size());
        System.out.println("results at out dir = " + outDir.toAbsolutePath());
    }

    private void prepareExomiserData() throws IOException {
        Path opencgaHome = opencga.getOpencgaHome();
        Path exomiserDataPath = opencgaHome.resolve("analysis/resources");
        if (!exomiserDataPath.toFile().exists()) {
            exomiserDataPath.toFile().mkdirs();
        }
        if (!opencgaHome.resolve("analysis/resources/exomiser").toAbsolutePath().toFile().exists()) {
            if (Paths.get("/opt/opencga/analysis/resources/exomiser").toFile().exists()) {
                Path symbolicLink = Files.createSymbolicLink(opencgaHome.resolve("analysis/resources/exomiser").toAbsolutePath(), Paths.get("/opt/opencga/analysis/resources/exomiser"));
            }
        }
    }
}
