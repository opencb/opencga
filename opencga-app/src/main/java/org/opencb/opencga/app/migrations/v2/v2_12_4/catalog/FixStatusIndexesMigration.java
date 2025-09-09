package org.opencb.opencga.app.migrations.v2.v2_12_4.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "fix_status_indexes" ,
        description = "Replace 'status.name' indexes for 'status.id'",
        version = "2.12.4",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20240328,
        deprecatedSince = "4.0.0"
)
public class FixStatusIndexesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
