package org.opencb.opencga.app.migrations.v2.v2_2_1.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "sample_index_configuration_add_missing_status",
        description = "Sample index configuration add missing status #TASK-512", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220328, deprecatedSince = "v3.0.0")
public class SampleIndexConfigurationAddMissingStatus extends StorageMigrationTool {
    @Override
    protected void run() throws Exception {
    }
}
