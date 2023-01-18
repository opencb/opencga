package org.opencb.opencga.analysis.clinical.exomiser;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    public void exomiserAnalysis() throws IOException, CatalogException, ToolException {
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis"));

        OpenCGAResult<ClinicalAnalysis> caResult = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_ID2, QueryOptions.empty(), clinicalTest.token);
        ClinicalAnalysis clinicalAnalysis = caResult.getResults().get(0);
        System.out.println(clinicalAnalysis.getProband().toString());

        ExomiserInterpretationAnalysis exomiser = new ExomiserInterpretationAnalysis();

        exomiser.setUp(opencga.getOpencgaHome().toAbsolutePath().toString(), new ObjectMap(), outDir, clinicalTest.token);
        exomiser.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.CA_ID2);

        ExecutionResult result = exomiser.start();

        System.out.println(result);

        System.out.println("Done!");
    }
}