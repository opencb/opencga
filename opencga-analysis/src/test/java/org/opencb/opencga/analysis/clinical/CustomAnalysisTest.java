package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ReportedEvent;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.stream.Collectors;

public class CustomAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private AbstractClinicalManagerTest clinicalTest;

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void setUp() throws Exception {
        clearDB("opencga_test_user_1000G");

        clinicalTest = new AbstractClinicalManagerTest();

        clinicalTest.catalogManagerResource = catalogManagerResource;
        clinicalTest.setUp();

        // Copy config files in the OpenCGA home conf folder
        Files.createDirectory(catalogManagerResource.getOpencgaHome().resolve("conf"));
        catalogManagerResource.getConfiguration().serialize(
                new FileOutputStream(catalogManagerResource.getOpencgaHome().resolve("conf").resolve("configuration.yml").toString()));

        InputStream storageConfigurationStream = MongoDBVariantStorageTest.class.getClassLoader()
                .getResourceAsStream("storage-configuration-test.yml");
        Files.copy(storageConfigurationStream, catalogManagerResource.getOpencgaHome().resolve("conf").resolve("storage-configuration.yml"),
                StandardCopyOption.REPLACE_EXISTING);
    }

        @Test
    public void customAnalysisTest() throws Exception {
        //http://re-prod-opencgahadoop-tomcat-01.gel.zone:8080/opencga-test/webservices/rest/v1/analysis/clinical/interpretation/tools/custom?study=100k_genomes_grch38_germline%3ARD38&sid=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJpbWVkaW5hIiwiYXVkIjoiT3BlbkNHQSB1c2VycyIsImlhdCI6MTU1MjY1NTYyNCwiZXhwIjoxNTUyNjU3NDI0fQ.6VO2mI_MJn3fejtdqdNi5W8uFa3rVXM2501QzN--Th8&sample=LP3000468-DNA_G06%3BLP3000473-DNA_C10%3BLP3000469-DNA_F03&summary=false&exclude=annotation.geneExpression&approximateCount=false&skipCount=true&useSearchIndex=auto&unknownGenotype=0%2F0&limit=10&skip=0
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine();

            ObjectMap storageOptions = new ObjectMap()
                    .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);

            StorageConfiguration configuration = variantStorageEngine.getConfiguration();
            configuration.setDefaultStorageEngineId(variantStorageEngine.getStorageEngineId());
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(configuration);
            storageEngineFactory.registerVariantStorageEngine(variantStorageEngine);

            VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManagerResource.getCatalogManager(), storageEngineFactory);

            variantStorageManager.index(clinicalTest.studyFqn, "family.vcf", ".", storageOptions, clinicalTest.token);
//        for (Variant variant : variantStorageManager.iterable(clinicalTest.token)) {
//            System.out.println("variant = " + variant.toStringSimple());// + ", ALL:maf = " + variant.getStudies().get(0).getStats("ALL").getMaf());
//        }

            ObjectMap options = new ObjectMap();
            String param = FamilyAnalysis.SKIP_UNTIERED_VARIANTS_PARAM;
            options.put(param, false);

            Query query = new Query();
            //query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

            CustomAnalysis customAnalysis = new CustomAnalysis(clinicalTest.clinicalAnalysis.getId(), query, clinicalTest.studyFqn, null,
                    null, options, catalogManagerResource.getOpencgaHome().toString(), clinicalTest.token);
            InterpretationResult execute = customAnalysis.execute();
            for (ReportedVariant variant : execute.getResult().getPrimaryFindings()) {
                System.out.println("variant = " + variant.toStringSimple());
                System.out.println("\tnum. reported events = " + variant.getReportedEvents().size());
                for (ReportedEvent reportedEvent : variant.getReportedEvents()) {
                    if (CollectionUtils.isEmpty(reportedEvent.getConsequenceTypes())) {
                        System.out.println("\tnum. ct = EMPTY");
                    } else {
                        System.out.println("\tnum. ct: " + reportedEvent.getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.joining(",")));
                    }
                }
            }
//            System.out.println("Num. variants = " + execute.getResult().size());
    }

}