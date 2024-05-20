package org.opencb.opencga.app.migrations.v2.v2_6_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "add_cellbase_data_release" ,
        description = "Add default cellbase data release if missing",
        version = "2.6.0",
        domain = Migration.MigrationDomain.STORAGE,
        language = Migration.MigrationLanguage.JAVA,
        patch = 2,
        date = 20230104,
        deprecatedSince = "3.0.0"
)
public class AddCellbaseDataRelease extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
