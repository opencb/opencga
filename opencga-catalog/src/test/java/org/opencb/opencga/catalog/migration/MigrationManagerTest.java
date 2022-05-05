package org.opencb.opencga.catalog.migration;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobReferenceParam;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class MigrationManagerTest extends AbstractManagerTest {

    @Migration(id = "test-1", version = "0.0.1", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20180501)
    public static class Migration1 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test-2", version = "0.0.1", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20180502)
    public static class Migration2 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test2-1", version = "0.1.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20180501)
    public static class Migration3 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test2-2", version = "0.1.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20180501)
    public static class Migration4 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test3-1", version = "0.2.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20190401)
    public static class Migration5 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test3-2", version = "0.2.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20190403)
    public static class Migration6 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test4-1-manual", version = "0.2.1", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20200401, manual = true)
    public static class Migration7 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {
            if (!params.getString("key").equals("value")) {
                throw new MigrationException("param 'key' must value 'value' !");
            }
        }
    }

    @Migration(id = "test4-2", version = "0.2.2", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20200401)
    public static class Migration8 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test4-3", version = "0.2.3", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20200401)
    public static class Migration9 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id = "test-with-jobs", version = "1.0.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, date = 20200401)
    public static class MigrationWithJobs extends MigrationTool {
        @Override
        protected void run() throws Exception {
            String fqn = catalogManager.getProjectManager().search(new Query(), new QueryOptions(), token).first().getFqn();
            getMigrationRun().getJobs().clear();

            getMigrationRun().addJob(catalogManager.getJobManager().submitProject(fqn, "my-tool", null, Collections.emptyMap(), null, null, null, null, token).first());
        }
    }

    @Override
    @Before
    public void setUp() throws java.io.IOException, CatalogException {
        super.setUp();
        try (MongoDBAdaptorFactory mongoDBAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration())) {
            mongoDBAdaptorFactory.getMongoDataStore()
                    .getCollection(MongoDBAdaptorFactory.MIGRATION_COLLECTION)
                    .remove(new Document(), new QueryOptions(MongoDBCollection.MULTI, true));
        }
    }

    @Test
    public void testMigration() throws Exception {
        MigrationManager migrationManager = catalogManager.getMigrationManager();
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        List<Class<? extends MigrationTool>> pendingMigrations = migrationManager.getPendingMigrations("0.0.1", token);
        assertEquals(0, pendingMigrations.size());

        pendingMigrations = migrationManager.getPendingMigrations("0.1.0", token);
        assertEquals(2, pendingMigrations.size());
        for (Class<? extends MigrationTool> pendingMigration : pendingMigrations) {
            Migration annotation = pendingMigration.getAnnotation(Migration.class);
            assertTrue(Arrays.asList("test-1", "test-2").contains(annotation.id()));
        }
        // Run migrations up to 0.0.1
        migrationManager.runMigration("0.0.1", Collections.emptySet(), Collections.emptySet(), "", token);

        pendingMigrations = migrationManager.getPendingMigrations("0.1.0", token);
        assertEquals(0, pendingMigrations.size());

        pendingMigrations = migrationManager.getPendingMigrations("0.2.0", token);
        assertEquals(2, pendingMigrations.size());
        for (int i = 0; i < pendingMigrations.size(); i++) {
            Class<? extends MigrationTool> pendingMigration = pendingMigrations.get(i);
            Migration annotation = pendingMigration.getAnnotation(Migration.class);
            switch (i) {
                case 0:
                    assertEquals("test2-1", annotation.id());
                    break;
                case 1:
                    assertEquals("test2-2", annotation.id());
                    break;
                default:
                    fail();
            }
        }

        pendingMigrations = migrationManager.getPendingMigrations("0.2.1", token);
        assertEquals(4, pendingMigrations.size());
        for (int i = 0; i < pendingMigrations.size(); i++) {
            Class<? extends MigrationTool> pendingMigration = pendingMigrations.get(i);
            Migration annotation = pendingMigration.getAnnotation(Migration.class);
            switch (i) {
                case 0:
                    assertEquals("test2-1", annotation.id());
                    break;
                case 1:
                    assertEquals("test2-2", annotation.id());
                    break;
                case 2:
                    assertEquals("test3-1", annotation.id());
                    break;
                case 3:
                    assertEquals("test3-2", annotation.id());
                    break;
                default:
                    fail();
            }
        }
        migrationManager.runMigration("0.2.0", Collections.emptySet(), Collections.emptySet(), "", token);

        pendingMigrations = migrationManager.getPendingMigrations("0.2.3", token);
        assertEquals(2, pendingMigrations.size());
        for (int i = 0; i < pendingMigrations.size(); i++) {
            Class<? extends MigrationTool> pendingMigration = pendingMigrations.get(i);
            Migration annotation = pendingMigration.getAnnotation(Migration.class);
            switch (i) {
                case 0:
                    assertEquals("test4-1-manual", annotation.id());
                    break;
                case 1:
                    assertEquals("test4-2", annotation.id());
                    break;
                default:
                    fail();
            }
        }

        thrown.expectMessage("manual");
        thrown.expect(MigrationException.class);
        migrationManager.runMigration("0.2.2", Collections.emptySet(), Collections.emptySet(), "", token);
    }

    @Test
    public void testManualMigrations() throws CatalogException, IOException {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        MigrationRun migrationRun = catalogManager.getMigrationManager().runManualMigration("0.2.1", "test4-1-manual", Paths.get(""), new ObjectMap("key", "OtherValue"), token);
        assertEquals(MigrationRun.MigrationStatus.ERROR, migrationRun.getStatus());

        migrationRun = catalogManager.getMigrationManager().runManualMigration("0.2.1", "test4-1-manual", Paths.get(""), new ObjectMap("key", "value"), token);
        assertEquals(MigrationRun.MigrationStatus.DONE, migrationRun.getStatus());
    }

    @Test
    public void testMigrationsWithJobs() throws CatalogException, IOException {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        catalogManager.getMigrationManager().runManualMigration("0.2.1", "test4-1-manual", Paths.get(""), new ObjectMap("key", "value"), token);

        // RUN. New status ON_HOLD
        catalogManager.getMigrationManager().runMigration("1.0.0", Collections.emptySet(), Collections.emptySet(), "", token);

        MigrationRun migrationRun = catalogManager.getMigrationManager().getMigrationRuns(token).stream().filter(p1 -> p1.getKey().id().equals("test-with-jobs")).findFirst().get().getValue();
        assertEquals(MigrationRun.MigrationStatus.ON_HOLD, migrationRun.getStatus());
        Date start = migrationRun.getStart();
        JobReferenceParam j = migrationRun.getJobs().get(0);

        // RUN. Migration run does not get triggered
        catalogManager.getMigrationManager().runMigration("1.0.0", Collections.emptySet(), Collections.emptySet(), "", token);
        migrationRun = catalogManager.getMigrationManager().getMigrationRuns(token).stream().filter(p -> p.getKey().id().equals("test-with-jobs")).findFirst().get().getValue();
        assertEquals(MigrationRun.MigrationStatus.ON_HOLD, migrationRun.getStatus());
        assertEquals(start, migrationRun.getStart());
        assertEquals(j, migrationRun.getJobs().get(0));

        // Update job with ERROR. Migration gets updated to ERROR.
        Job job = catalogManager.getJobManager().get(j.getStudyId(), j.getId(), new QueryOptions(), token).first();
        catalogManager.getJobManager().update(job.getStudy().getId(), job.getId(), new ObjectMap("internal.status.id", "ERROR"), new QueryOptions(), token);

        migrationRun = catalogManager.getMigrationManager().getMigrationRuns(token)
                .stream().filter(p -> p.getKey().id().equals("test-with-jobs")).findFirst().get().getValue();
        assertEquals(MigrationRun.MigrationStatus.ERROR, migrationRun.getStatus());

        // RUN. Migration run triggered. Status: ON_HOLD
        catalogManager.getMigrationManager().runMigration("1.0.0", Collections.emptySet(), Collections.emptySet(), "", token);
        migrationRun = catalogManager.getMigrationManager().getMigrationRuns(token)
                .stream().filter(p -> p.getKey().id().equals("test-with-jobs")).findFirst().get().getValue();
        assertEquals(MigrationRun.MigrationStatus.ON_HOLD, migrationRun.getStatus());

        // Update job with DONE. Migration gets updated to DONE.
        j = migrationRun.getJobs().get(0);
        job = catalogManager.getJobManager().get(j.getStudyId(), j.getId(), new QueryOptions(), token).first();
        catalogManager.getJobManager().update(job.getStudy().getId(), job.getId(), new ObjectMap("internal.status.id", "DONE"), new QueryOptions(), token);

        migrationRun = catalogManager.getMigrationManager().getMigrationRuns(token)
                .stream().filter(p -> p.getKey().id().equals("test-with-jobs")).findFirst().get().getValue();
        assertEquals(MigrationRun.MigrationStatus.DONE, migrationRun.getStatus());
    }


    @Test
    public void testMigrationVersionOrder() {
        List<String> expected = Arrays.asList("0.0.0", "0.0.1", "0.0.10", "0.1.0", "0.1.10", "1.1.0",
                "2.0.0",
                "2.0.1",
                "2.1.0");

        ArrayList<String> actual = new ArrayList<>(expected);
        Collections.shuffle(actual);
        actual.sort(MigrationManager::compareVersion);

        assertEquals(expected, actual);
    }

}
