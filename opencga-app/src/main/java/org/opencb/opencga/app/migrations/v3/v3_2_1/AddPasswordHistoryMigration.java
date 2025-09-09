package org.opencb.opencga.app.migrations.v3.v3_2_1;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_archivePasswords_array",
        description = "Add password history #6494", version = "3.2.1",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240723,
        deprecatedSince = "4.0.0")
public class AddPasswordHistoryMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {

    }

}
