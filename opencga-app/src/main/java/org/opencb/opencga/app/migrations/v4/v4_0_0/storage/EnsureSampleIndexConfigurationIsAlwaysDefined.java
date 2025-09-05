package org.opencb.opencga.app.migrations.v4.v4_0_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "ensure_sample_index_configuration_is_defined",
        description = "Ensure that the SampleIndexConfiguration object is correctly defined. #TASK-6765", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 2,
        date = 20240910,
        deprecatedSince = "5.0.0")
public class EnsureSampleIndexConfigurationIsAlwaysDefined extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
