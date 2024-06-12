package org.opencb.opencga.app.migrations.v3.v3_0_0;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.config.Configuration;

@Migration(id = "add_organizations", description = "Add new Organization layer #TASK-4389", version = "3.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20231212,
        deprecatedSince = "3.1.0")
public class OrganizationMigration extends MigrationTool {

    public OrganizationMigration(Configuration configuration, String adminPassword, String userId) {
    }

    @Override
    protected void run() throws Exception {
    }

}
