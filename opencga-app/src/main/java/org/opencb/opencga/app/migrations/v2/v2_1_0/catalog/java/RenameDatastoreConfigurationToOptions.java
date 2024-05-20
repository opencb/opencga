package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.java;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_datastore_configuration_to_options", description = "Rename project.internal.datastores.variant.configuration to options", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210617,
        deprecatedSince = "v3.0.0")
public class RenameDatastoreConfigurationToOptions extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
