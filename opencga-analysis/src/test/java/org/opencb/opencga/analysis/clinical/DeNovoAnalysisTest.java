package org.opencb.opencga.analysis.clinical;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.results.VariantQueryResult;
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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class DeNovoAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


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
                .getResourceAsStream("storage-configuration.yml");
        Files.copy(storageConfigurationStream, catalogManagerResource.getOpencgaHome().resolve("conf").resolve("storage-configuration.yml"),
                StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    public void denovoTest() throws Exception {
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


        DeNovoAnalysis deNovoAnalysis = new DeNovoAnalysis(clinicalTest.clinicalAnalysis.getId(), null, null, null, null, null,
                clinicalTest.studyFqn, catalogManagerResource.getOpencgaHome().toString(), clinicalTest.token);
        AnalysisResult<List<Variant>> execute = deNovoAnalysis.execute();
        for (Variant variant : execute.getResult()) {
            System.out.println("variant = " + variant);
        }
        System.out.println("Num. variants = " + execute.getResult().size());

    }
}