package org.opencb.opencga.app.migrations.v2.v2_4_4.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_biotype_allowed_keys_TASK-1849",
        description = "Remove biotype and consequence type allowed keys from variable sets #TASK-1849", version = "2.4.4",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220905,
        deprecatedSince = "3.0.0")
public class RemoveBiotypeAllowedKeysMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
