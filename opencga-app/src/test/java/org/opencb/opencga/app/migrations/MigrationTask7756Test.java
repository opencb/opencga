package org.opencb.opencga.app.migrations;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.migrations.v5.v5_0_0.catalog.ClinicalMigrationTask7756;
import org.opencb.opencga.catalog.db.mongodb.MongoBackupUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.CatalogStudyConfiguration;

import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class MigrationTask7756Test {

    private static boolean initialized = false;
    private static CatalogManagerExternalResource externalResource;
    private static CatalogManager catalogManager;
    private static String token;

    @Before
    public void setup() throws Exception {
        if (initialized) {
            return;
        }

        externalResource = new CatalogManagerExternalResource();
        externalResource.before();

        URL resource = getClass().getResource("/datasets/opencga/v5.0.0-task-7756/");
        MongoBackupUtils.restore(externalResource.getCatalogManager(), externalResource.getOpencgaHome(),
                Paths.get(resource.toURI()).resolve("mongodb"));

        // Setup catalog manager with test database
        catalogManager = externalResource.getCatalogManager();

        token = catalogManager.getUserManager().login("test", "test", "Test_P4ss").first().getToken();

        // Run migration
        runMigration(ClinicalMigrationTask7756.class);

        initialized = true;
    }

    private void runMigration(Class<? extends MigrationTool> migration) throws CatalogException {
        Migration annotation = migration.getAnnotation(Migration.class);
        catalogManager.getMigrationManager()
                .runManualMigration(annotation.version(), annotation.id(), externalResource.getOpencgaHome(), new ObjectMap(),
                        externalResource.getAdminToken());
    }

    @Test
    public void testIndividualQCMigration() throws CatalogException {
        // Test Individual 1 (with QC data)
        Individual individual = catalogManager.getIndividualManager().get("testStudy", "ind1", QueryOptions.empty(), token).first();

        assertNotNull(individual);
        assertNotNull(individual.getQualityControl());
        assertNotNull(individual.getQualityControl().getInferredSexReports());
        assertEquals(1, individual.getQualityControl().getInferredSexReports().size());
        assertEquals("CoverageRatio", individual.getQualityControl().getInferredSexReports().get(0).getMethod());

        assertNotNull(individual.getQualityControl().getMendelianErrorReports());
        assertEquals(1, individual.getQualityControl().getMendelianErrorReports().size());
        assertEquals(0, individual.getQualityControl().getMendelianErrorReports().get(0).getNumErrors());

        // Test Individual 2 (without QC data)
        Individual individual2 = catalogManager.getIndividualManager().get("testStudy", "ind2", QueryOptions.empty(), token).first();

        assertNotNull(individual2);
        // After migration, should have an empty QC object instead of null
        assertNotNull(individual2.getQualityControl());
    }

    @Test
    public void testFamilyQCMigration() throws CatalogException {
        // Test Family 1 (with QC data)
        Family family = catalogManager.getFamilyManager().get("testStudy", "fam1", QueryOptions.empty(), token).first();

        assertNotNull(family);
        assertNotNull(family.getQualityControl());
        assertNotNull(family.getQualityControl().getRelatedness());
        assertEquals(1, family.getQualityControl().getRelatedness().size());
        assertEquals("PLINK/IBD", family.getQualityControl().getRelatedness().get(0).getMethod());

        // Test Family 2 (without QC data)
        Family family2 = catalogManager.getFamilyManager().get("testStudy", "fam2", QueryOptions.empty(), token).first();

        assertNotNull(family2);
        // After migration, should have an empty QC object instead of null
        assertNotNull(family2.getQualityControl());
        assertTrue(family2.getQualityControl().getRelatedness().isEmpty());
    }

    @Test
    public void testSampleQCMigration() throws CatalogException {
        // Test Sample 1 (with variant QC data)
        Sample sample1 = catalogManager.getSampleManager().get("testStudy", "sample1", QueryOptions.empty(), token).first();

        assertNotNull(sample1);
        assertNotNull(sample1.getQualityControl());
        assertNotNull(sample1.getQualityControl().getVariant());
        assertNotNull(sample1.getQualityControl().getVariant().getVariantStats());
        assertEquals(1, sample1.getQualityControl().getVariant().getVariantStats().size());
        assertEquals("stat1", sample1.getQualityControl().getVariant().getVariantStats().get(0).getId());

        assertNotNull(sample1.getQualityControl().getVariant().getSignatures());
        assertEquals(1, sample1.getQualityControl().getVariant().getSignatures().size());
        assertEquals("sig1", sample1.getQualityControl().getVariant().getSignatures().get(0).getId());

        assertNotNull(sample1.getQualityControl().getVariant().getGenomePlot());
        assertEquals("plot1", sample1.getQualityControl().getVariant().getGenomePlot().getId());

        // Test Sample 2 (with files QC data)
        Sample sample2 = catalogManager.getSampleManager().get("testStudy", "sample2", QueryOptions.empty(), token).first();

        assertNotNull(sample2);
        assertNotNull(sample2.getQualityControl());
        assertNotNull(sample2.getQualityControl().getFiles());
        assertEquals(1, sample2.getQualityControl().getFiles().size());
        assertEquals("file1", sample2.getQualityControl().getFiles().get(0));

        // Test Sample 3 (without QC data)
        Sample sample3 = catalogManager.getSampleManager().get("testStudy", "sample3", QueryOptions.empty(), token).first();

        assertNotNull(sample3);
        // After migration, should have an empty QC object instead of null
        assertNotNull(sample3.getQualityControl());
    }

    @Test
    public void testClinicalAnalysisCvdbIndexMigration() throws CatalogException {
        // Test Clinical Analysis 1 (with cvdbIndex)
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get("testStudy", "case1", QueryOptions.empty(), token).first();

        assertNotNull(clinicalAnalysis);
        assertNotNull(clinicalAnalysis.getInternal().getCvdbIndex().getStatus());
        assertTrue(StringUtils.isEmpty(clinicalAnalysis.getInternal().getCvdbIndex().getJobId()));
    }

    @Test
    public void testStudyConfigurationMigration() throws CatalogException {
        // Get a study after migration
        org.opencb.opencga.core.models.study.Study study = catalogManager.getStudyManager().get("testStudy", new QueryOptions(QueryOptions.INCLUDE, "internal"), token).first();

        assertNotNull("Study should not be null", study);
        assertNotNull("Study internal should not be null", study.getInternal());
        assertNotNull("Study configuration should not be null", study.getInternal().getConfiguration());
        assertNotNull("Study catalog configuration should not be null", study.getInternal().getConfiguration().getCatalog());

        // Verify the CVDB configuration exists
        assertNotNull("CVDB configuration should not be null",
                study.getInternal().getConfiguration().getCatalog().getCvdb());

        // Verify the Variant Quality Control configuration exists
        assertNotNull("Variant Quality Control configuration should not be null",
                study.getInternal().getConfiguration().getCatalog().getVariantQualityControl());

        // Check that default configuration was applied correctly by checking
        // that defaults match CatalogStudyConfiguration.defaultConfiguration()
        CatalogStudyConfiguration defaultConfig = CatalogStudyConfiguration.defaultConfiguration();
        assertEquals("CVDB configuration should match default",
                defaultConfig.getCvdb().isActive(),
                study.getInternal().getConfiguration().getCatalog().getCvdb().isActive());
        assertEquals("Variant Quality Control configuration should match default",
                defaultConfig.getVariantQualityControl().isActive(),
                study.getInternal().getConfiguration().getCatalog().getVariantQualityControl().isActive());
    }

}
