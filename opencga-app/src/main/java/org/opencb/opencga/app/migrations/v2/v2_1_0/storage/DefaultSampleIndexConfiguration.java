package org.opencb.opencga.app.migrations.v2.v2_1_0.storage;


import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "default_sample_index_configuration", description = "Add a default backward compatible sample index configuration", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 7,
        date = 20210721,
        deprecatedSince = "v3.0.0") // Needs to run after StudyClinicalConfigurationRelocation
public class DefaultSampleIndexConfiguration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
