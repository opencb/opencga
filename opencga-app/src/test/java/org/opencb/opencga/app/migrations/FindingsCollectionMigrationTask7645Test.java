package org.opencb.opencga.app.migrations;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.app.migrations.v5.v5_0_0.catalog.FindingsCollectionMigrationTask7645;
import org.opencb.opencga.catalog.db.mongodb.MongoBackupUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class FindingsCollectionMigrationTask7645Test {

    private static boolean initialized = false;
    private static CatalogManagerExternalResource externalResource;
    private static CatalogManager catalogManager;
    private static String token;
    private final static String STUDY = "phase1";
    private final static String CLINICAL_ANALYSIS_ID = "Clinical";

    @Before
    public void setup() throws Exception {
        if (initialized) {
            return;
        }

        externalResource = new CatalogManagerExternalResource();
        externalResource.before();

        URL resource = getClass().getResource("/datasets/opencga/v5.0.0-task-7645/");
        MongoBackupUtils.restore(externalResource.getCatalogManager(), externalResource.getOpencgaHome(),
                Paths.get(resource.toURI()).resolve("mongodb"));

        // Setup catalog manager with test database
        catalogManager = externalResource.getCatalogManager();

        token = catalogManager.getUserManager().login("test", "owner", TestParamConstants.PASSWORD).first().getToken();

        // Run migration
        runMigration(FindingsCollectionMigrationTask7645.class);

        initialized = true;
    }

    private void runMigration(Class<? extends MigrationTool> migration) throws CatalogException {
        runMigration(migration, false);
    }

    private void runMigration(Class<? extends MigrationTool> migration, boolean force) throws CatalogException {
        Migration annotation = migration.getAnnotation(Migration.class);
        catalogManager.getMigrationManager()
                .runManualMigration(annotation.version(), annotation.id(), externalResource.getOpencgaHome(), force, true, new ObjectMap(),
                        externalResource.getAdminToken());
    }

    @Test
    public void obtainInterpretation() throws CatalogException {
        // Obtain last version of the interpretation
        Interpretation interpretation = catalogManager.getInterpretationManager().get(STUDY, CLINICAL_ANALYSIS_ID + ".1", QueryOptions.empty(), token).first();
        assertEquals(5, interpretation.getVersion());
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("method", interpretation.getPrimaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id1", interpretation.getPrimaryFindings().get(0).getId());
        assertEquals(4, interpretation.getPrimaryFindings().get(0).getVersion());
        assertEquals("AnotherMethodName", interpretation.getPrimaryFindings().get(1).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getPrimaryFindings().get(1).getId());
        assertEquals(5, interpretation.getPrimaryFindings().get(1).getVersion());
        assertEquals("YetAnotherMethodName",
                interpretation.getPrimaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals(3, interpretation.getPrimaryFindings().get(2).getVersion());
        assertEquals(3, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(3, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Obtain previous version of the interpretation
        Query query = new Query("version", 2);
        interpretation = catalogManager.getInterpretationManager().get(STUDY, Collections.singletonList(CLINICAL_ANALYSIS_ID + ".1"), query, QueryOptions.empty(), false, token).first();
        assertEquals(2, interpretation.getVersion());
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("id2", interpretation.getPrimaryFindings().get(0).getId());
        assertEquals(2, interpretation.getPrimaryFindings().get(0).getVersion());
        assertEquals("id1", interpretation.getPrimaryFindings().get(1).getId());
        assertEquals(2, interpretation.getPrimaryFindings().get(1).getVersion());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals(1, interpretation.getPrimaryFindings().get(2).getVersion());
    }

    @Test
    public void idempotentMigration() throws CatalogException {
        runMigration(FindingsCollectionMigrationTask7645.class, true);
    }

}
