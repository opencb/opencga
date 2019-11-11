package org.opencb.opencga.analysis.clinical;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.util.List;

public class DeNovoAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private AbstractClinicalManagerTest clinicalTest;

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();


    @Before
    public void setUp() throws Exception {
        clearDB("opencga_test_user_1000G");
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(catalogManagerResource, getVariantStorageEngine());
    }

    @Test
    public void denovoTest() throws Exception {
        DeNovoAnalysis deNovoAnalysis = new DeNovoAnalysis(clinicalTest.clinicalAnalysis.getId(), clinicalTest.studyFqn, null,
                null, catalogManagerResource.getOpencgaHome().toString(), clinicalTest.token);
        AnalysisResult<List<Variant>> execute = deNovoAnalysis.compute();
        for (Variant variant : execute.getResult()) {
            System.out.println("variant = " + variant);
        }
        System.out.println("Num. variants = " + execute.getResult().size());

    }
}