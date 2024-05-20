package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.addInternalLastModified;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_modificationDate_to_project.internal", description = "Add internal modificationDate to Project #1810", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG,
        date = 20210812, deprecatedSince = "v3.0.0")
public class AddModificationDateToProjectInternal extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
