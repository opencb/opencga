package org.opencb.opencga.app.migrations.v2.v2_1_0.storage;


import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_cellbase_configuration_to_project", description = "Add cellbase configuration from storage-configuration.yml to project.internal.cellbase", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 3,
        date = 20210616,
        deprecatedSince = "3.0.0")
public class AddCellbaseConfigurationToProject extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
