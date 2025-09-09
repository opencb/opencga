package org.opencb.opencga.app.migrations.v2.v2_12_5.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "illegal_concurrent_file_loadings" ,
        description = "Detect illegal concurrent file loadings and fix them by setting 'status' to 'INVALID' or 'READY'",
        version = "2.12.5",
        domain = Migration.MigrationDomain.STORAGE,
        language = Migration.MigrationLanguage.JAVA,
        date = 20240424,
        deprecatedSince = "4.0.0"
)
public class DetectIllegalConcurrentFileLoadingsMigration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {

    }

}
