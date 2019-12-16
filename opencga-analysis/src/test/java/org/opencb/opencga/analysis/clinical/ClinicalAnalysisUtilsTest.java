package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.biodata.models.clinical.interpretation.ReportedEvent;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalAnalysisUtilsTest {

    public static AbstractClinicalManagerTest getClinicalTest(CatalogManagerExternalResource catalogManagerResource,
                                                              MongoDBVariantStorageEngine variantStorageEngine) throws IOException, CatalogException, URISyntaxException, StorageEngineException, ToolException {

        AbstractClinicalManagerTest clinicalTest = new AbstractClinicalManagerTest();

        clinicalTest.catalogManagerResource = catalogManagerResource;
        clinicalTest.setUp();

        // Copy config files in the OpenCGA home conf folder
        Files.createDirectory(catalogManagerResource.getOpencgaHome().resolve("conf"));
        catalogManagerResource.getConfiguration().serialize(
                new FileOutputStream(catalogManagerResource.getOpencgaHome().resolve("conf").resolve("configuration.yml").toString()));

        InputStream storageConfigurationStream = MongoDBVariantStorageTest.class.getClassLoader()
                .getResourceAsStream("storage-configuration.yml");
        Files.copy(storageConfigurationStream, catalogManagerResource.getOpencgaHome().resolve("conf").resolve("storage-configuration.yml"),
                StandardCopyOption.REPLACE_EXISTING);

        ObjectMap storageOptions = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        StorageConfiguration configuration = variantStorageEngine.getConfiguration();
        configuration.getVariant().setDefaultEngine(variantStorageEngine.getStorageEngineId());
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(configuration);
        storageEngineFactory.registerVariantStorageEngine(variantStorageEngine);

        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManagerResource.getCatalogManager(), storageEngineFactory);

        Path outDir = Paths.get("target/test-data").resolve("junit_clinical_analysis_" + RandomStringUtils.randomAlphabetic(10));
        Files.createDirectories(outDir);

        variantStorageManager.index(clinicalTest.studyFqn, "family.vcf", outDir.toString(), storageOptions, clinicalTest.token);

        return clinicalTest;
    }

    public static void displayReportedVariants(List<ReportedVariant> reportedVariants, String msg) {
        System.out.println(msg);
        if (CollectionUtils.isNotEmpty(reportedVariants)) {
            System.out.println("\tNum. reported variants = " + reportedVariants.size());
            for (ReportedVariant variant : reportedVariants) {
                System.out.println("\t\tReported variant = " + variant.toStringSimple());
                System.out.println("\t\t\t\tNum. reported events = " + variant.getEvidences().size());
                for (ReportedEvent reportedEvent : variant.getEvidences()) {
                    System.out.print("\t\t\t\t\t(Tier, CT) = (" + reportedEvent.getClassification().getTier() + ", ");
                    if (CollectionUtils.isEmpty(reportedEvent.getConsequenceTypes())) {
                        System.out.print("EMPTY");
                    } else {
                        System.out.print(reportedEvent.getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.joining(",")));
                    }
                    System.out.println(")");
                }
            }
        } else {
            System.out.println("\tNum. variants = 0");
        }
    }
}
