package org.opencb.opencga.catalog.migration;

import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MigrationManagerTest extends AbstractManagerTest {

    @Migration(id ="test-1", version = "0.0.1", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 1)
    public static class Migration1 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test-2", version = "0.0.1", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 2)
    public static class Migration2 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test2-1", version = "0.1.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 1)
    public static class Migration3 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test2-2", version = "0.1.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 2)
    public static class Migration4 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test3-1", version = "0.2.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 1)
    public static class Migration5 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test3-2", version = "0.2.0", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 2)
    public static class Migration6 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test4-1-manual", version = "0.2.1", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 1, manual = true)
    public static class Migration7 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test4-2", version = "0.2.2", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 1)
    public static class Migration8 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Migration(id ="test4-3", version = "0.2.3", description = "", domain = Migration.MigrationDomain.CATALOG,
            language = Migration.MigrationLanguage.JAVA, rank = 1)
    public static class Migration9 extends MigrationTool {
        @Override
        protected void run() throws MigrationException {

        }
    }

    @Test
    public void testMigration() throws Exception {
        MigrationManager migrationManager = catalogManager.getMigrationManager();
        String token = catalogManager.getUserManager().loginAsAdmin("admin").getToken();

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

}
