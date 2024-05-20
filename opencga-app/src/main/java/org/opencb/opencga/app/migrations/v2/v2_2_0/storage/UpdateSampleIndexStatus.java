package org.opencb.opencga.app.migrations.v2.v2_2_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "update_sample_index_status_1901",
        description = " Improve sample-index multi-schema management. #1901", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220302, deprecatedSince = "v3.0.0")
public class UpdateSampleIndexStatus extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
