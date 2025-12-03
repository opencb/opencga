package org.opencb.opencga.app.migrations.v3.v3_1_0;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "migrate_notes", description = "Migrate notes #TASK-5836", version = "3.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240315,
        deprecatedSince = "5.0.0")
public class NoteMigration extends MigrationTool {
    
    @Override
    protected void run() throws Exception {
    }

}
