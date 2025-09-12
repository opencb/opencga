package org.opencb.opencga.app.migrations;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.app.migrations.v3.v3_1_0.UserBanMigration;
import org.opencb.opencga.app.migrations.v3.v3_2_0.VariantSetupMigration;
import org.opencb.opencga.app.migrations.v3.v3_2_1.MoveUserAccountToInternalMigration;
import org.opencb.opencga.app.migrations.v4.v4_0_0.catalog.AddNewNoteTypeMigration;
import org.opencb.opencga.app.migrations.v4.v4_0_0.storage.EnsureSampleIndexConfigurationIsAlwaysDefined;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.migration.MigrationRun;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(LongTests.class)
public class MigrationsTest {

    public OpenCGATestExternalResource opencga;

    @Test
    public void testUserBanMigration() throws Exception {
        setup("v3.0.0", false);

        runMigration(UserBanMigration.class);
    }

    @Test
    public void testMoveUserAccountToInternalMigration() throws Exception {
        setup("v3.1.0", false);

        runMigration(MoveUserAccountToInternalMigration.class);
    }

    @Test
    public void testAddNewNoteTypeMigration() throws Exception {
        setup("v3.2.1", false);

        runMigration(AddNewNoteTypeMigration.class);
    }

    @Test
    public void testVamoriantSetupMigration() throws Exception {
        setup("v3.1.0", false);
        String studyName = "test@1000G:phase1";

        VariantStorageMetadataManager metadataManager = opencga.getVariantStorageEngineByProject("test@1000G").getMetadataManager();
        metadataManager.getAndUpdateProjectMetadata(new ObjectMap());
        StudyMetadata studyMetadata = metadataManager.createStudy(studyName);
        int fileId = metadataManager.registerFile(studyMetadata.getId(), "folder/file.vcf", Arrays.asList("s1", "s2"));
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(fileId));

        for (Study study : opencga.getCatalogManager().getStudyManager().searchInOrganization("test", new Query(), new QueryOptions(), opencga.getAdminToken()).getResults()) {
            Assert.assertNull(study.getInternal().getConfiguration().getVariantEngine().getSetup());
        }

        runMigration(VariantSetupMigration.class);

        for (Study study : opencga.getCatalogManager().getStudyManager().searchInOrganization("test", new Query(), new QueryOptions(), opencga.getAdminToken()).getResults()) {
            if (study.getFqn().equals(studyName)) {
                Assert.assertNotNull(study.getInternal().getConfiguration().getVariantEngine().getSetup());
            } else {
                Assert.assertNull(study.getInternal().getConfiguration().getVariantEngine().getSetup());
            }
        }
    }

    @Test
    public void testEnsureSampleIndexConfigurationIsAlwaysDefined() throws Exception {
        setup("v3.2.1", false);
        String studyName = "test@1000G:phase1";

        VariantStorageMetadataManager metadataManager = opencga.getVariantStorageEngineByProject("test@1000G").getMetadataManager();
        metadataManager.getAndUpdateProjectMetadata(new ObjectMap());
        StudyMetadata studyMetadata = metadataManager.createStudy(studyName);
        int fileId = metadataManager.registerFile(studyMetadata.getId(), "folder/file.vcf", Arrays.asList("s1", "s2"));
        metadataManager.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(fileId));
        metadataManager.updateStudyMetadata(studyMetadata, sm -> {
            sm.setSampleIndexConfigurations(Collections.emptyList());
        });

        studyMetadata = metadataManager.getStudyMetadata(studyMetadata.getId());
        Assert.assertNull(studyMetadata.getSampleIndexConfigurationLatest());

        runMigration(EnsureSampleIndexConfigurationIsAlwaysDefined.class);

        studyMetadata = metadataManager.getStudyMetadata(studyMetadata.getId());
        Assert.assertNotNull(studyMetadata.getSampleIndexConfigurationLatest());
    }

    @After
    public void tearDown() throws Exception {
        if (opencga != null) {
            opencga.after();
            opencga = null;
        }
    }

    protected void testMigration(Class<? extends MigrationTool> migration, String dataset) throws Exception {
        setup(dataset);
        runMigration(migration);
    }

    private void setup(String dataset) throws Exception {
        setup(dataset, false);
    }

    private void setup(String dataset, boolean storageHadoop) throws Exception {
        if (opencga != null) {
            opencga.after();
            opencga = null;
        }
        opencga = new OpenCGATestExternalResource(storageHadoop);
        opencga.before();
        URL resource = getClass().getResource("/datasets/opencga/" + dataset + "/");
        opencga.restore(resource);
    }

    private void runMigration(Class<? extends MigrationTool> migration) throws CatalogException {
        Migration annotation = migration.getAnnotation(Migration.class);
        List<MigrationRun> runs = opencga.getCatalogManager().getMigrationManager()
                .runManualMigration(annotation.version(), annotation.id(), opencga.getOpencgaHome(), new ObjectMap(), opencga.getAdminToken());
        for (MigrationRun run : runs) {
            assertEquals("Migration " + migration + " failed", MigrationRun.MigrationStatus.DONE, run.getStatus());
        }
    }

}