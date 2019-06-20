package org.opencb.opencga.analysis.clinical;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.clinical.interpretation.InterpretationResult;
import org.opencb.opencga.analysis.clinical.interpretation.TeamInterpretationAnalysis;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.util.List;

public class TeamAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private AbstractClinicalManagerTest clinicalTest;

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();


    @Before
    public void setUp() throws Exception {
        clearDB("opencga_test_user_1000G");
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(catalogManagerResource, getVariantStorageEngine());
    }

    @Test
    public void teamTest() throws Exception {
        TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis(clinicalTest.clinicalAnalysis.getId(), clinicalTest.studyFqn, null,
                null, null, catalogManagerResource.getOpencgaHome().toString(), clinicalTest.token);
        InterpretationResult execute = teamAnalysis.execute();
        ClinicalAnalysisUtilsTest.displayReportedVariants(execute.getResult().getPrimaryFindings(), "Primary findings:");
        ClinicalAnalysisUtilsTest.displayReportedVariants(execute.getResult().getSecondaryFindings(), "Secondary findings:");
    }
}