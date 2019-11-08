package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.clinical.interpretation.CustomInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.interpretation.FamilyInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.interpretation.InterpretationResult;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.util.ArrayList;
import java.util.List;

public class CustomAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private AbstractClinicalManagerTest clinicalTest;

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void setUp() throws Exception {
        clearDB("opencga_test_user_1000G");
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(catalogManagerResource, getVariantStorageEngine());
    }

    @Test
    public void customAnalysisFromClinicalAnalysisTest() throws Exception {
        //http://re-prod-opencgahadoop-tomcat-01.gel.zone:8080/opencga-test/webservices/rest/v1/analysis/clinical/interpretation/tools/custom?study=100k_genomes_grch38_germline%3ARD38&sid=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJpbWVkaW5hIiwiYXVkIjoiT3BlbkNHQSB1c2VycyIsImlhdCI6MTU1MjY1NTYyNCwiZXhwIjoxNTUyNjU3NDI0fQ.6VO2mI_MJn3fejtdqdNi5W8uFa3rVXM2501QzN--Th8&sample=LP3000468-DNA_G06%3BLP3000473-DNA_C10%3BLP3000469-DNA_F03&summary=false&exclude=annotation.geneExpression&approximateCount=false&skipCount=true&useSearchIndex=auto&unknownGenotype=0%2F0&limit=10&skip=0
//        for (Variant variant : variantStorageManager.iterable(clinicalTest.token)) {
//            System.out.println("variant = " + variant.toStringSimple());// + ", ALL:maf = " + variant.getStudies().get(0).getStats("ALL").getMaf());
//        }
        ObjectMap options = new ObjectMap();
        String param = FamilyInterpretationAnalysis.SKIP_UNTIERED_VARIANTS_PARAM;
        options.put(param, false);

//            Query query = new Query();
        //query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        CustomInterpretationAnalysis customAnalysis = new CustomInterpretationAnalysis(clinicalTest.clinicalAnalysis.getId(), clinicalTest.studyFqn, null,
                options, catalogManagerResource.getOpencgaHome().toString(), clinicalTest.token);
        InterpretationResult execute = customAnalysis.compute();
        ClinicalAnalysisUtilsTest.displayReportedVariants(execute.getResult().getPrimaryFindings(), "Primary findings:");
        ClinicalAnalysisUtilsTest.displayReportedVariants(execute.getResult().getSecondaryFindings(), "Secondary findings:");
    }

    @Test
    public void customAnalysisFromSamplesTest() throws Exception {
        //http://re-prod-opencgahadoop-tomcat-01.gel.zone:8080/opencga-test/webservices/rest/v1/analysis/clinical/interpretation/tools/custom?study=100k_genomes_grch38_germline%3ARD38&sid=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJpbWVkaW5hIiwiYXVkIjoiT3BlbkNHQSB1c2VycyIsImlhdCI6MTU1MjY1NTYyNCwiZXhwIjoxNTUyNjU3NDI0fQ.6VO2mI_MJn3fejtdqdNi5W8uFa3rVXM2501QzN--Th8&sample=LP3000468-DNA_G06%3BLP3000473-DNA_C10%3BLP3000469-DNA_F03&summary=false&exclude=annotation.geneExpression&approximateCount=false&skipCount=true&useSearchIndex=auto&unknownGenotype=0%2F0&limit=10&skip=0
//        for (Variant variant : variantStorageManager.iterable(clinicalTest.token)) {
//            System.out.println("variant = " + variant.toStringSimple());// + ", ALL:maf = " + variant.getStudies().get(0).getStats("ALL").getMaf());
//        }
        ObjectMap options = new ObjectMap();
        String param = FamilyInterpretationAnalysis.SKIP_UNTIERED_VARIANTS_PARAM;
        options.put(param, false);

        Query query = new Query();
        List<String> samples = new ArrayList();
        for (Individual member : clinicalTest.clinicalAnalysis.getFamily().getMembers()) {
            if (CollectionUtils.isNotEmpty(member.getSamples())) {
                samples.add(member.getSamples().get(0).getId());
            }
        }
        query.put(VariantQueryParam.SAMPLE.key(), samples);

        CustomInterpretationAnalysis customAnalysis = new CustomInterpretationAnalysis(null, clinicalTest.studyFqn, query,
                options, catalogManagerResource.getOpencgaHome().toString(), clinicalTest.token);
        InterpretationResult execute = customAnalysis.compute();
        ClinicalAnalysisUtilsTest.displayReportedVariants(execute.getResult().getPrimaryFindings(), "Primary findings:");
        ClinicalAnalysisUtilsTest.displayReportedVariants(execute.getResult().getSecondaryFindings(), "Secondary findings:");
    }
}