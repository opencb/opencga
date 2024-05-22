package org.opencb.opencga.app.migrations;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.app.migrations.v3_2_0.VariantSetupMigration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

@Category(LongTests.class)
public class MigrationsTest {

    public OpenCGATestExternalResource opencga;

    @Test
    public void testVariantSetupMigration() throws Exception {
        setup("v3.0.0");
        VariantStorageMetadataManager mm = DummyVariantStorageEngine.getVariantMetadataManager();
        String studyName = "test@1000G:phase1";
        StudyMetadata studyMetadata = mm.createStudy(studyName);
        int fileId = mm.registerFile(studyMetadata.getId(), "folder/file.vcf", Arrays.asList("s1", "s2"));
        mm.addIndexedFiles(studyMetadata.getId(), Collections.singletonList(fileId));

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
        if (opencga != null) {
            opencga.after();
            opencga = null;
        }
        opencga = new OpenCGATestExternalResource(false);
        opencga.before();
        URL resource = getClass().getResource("/datasets/opencga/" + dataset + "/");
        opencga.restore(resource);
    }

    private void runMigration(Class<? extends MigrationTool> migration) throws CatalogException {
        Migration annotation = migration.getAnnotation(Migration.class);
        opencga.getCatalogManager().getMigrationManager()
                .runManualMigration(annotation.version(), annotation.id(), opencga.getOpencgaHome(), new ObjectMap(), opencga.getAdminToken());
    }

}