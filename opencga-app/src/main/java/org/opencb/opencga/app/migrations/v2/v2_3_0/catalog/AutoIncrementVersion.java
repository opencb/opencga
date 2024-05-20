package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "autoincrement_version_TASK-323",
        description = "Reorganise data in new collections (autoincrement version) #TASK-323", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        offline = true,
        date = 20220512,
        deprecatedSince = "v3.0.0")
public class AutoIncrementVersion extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
