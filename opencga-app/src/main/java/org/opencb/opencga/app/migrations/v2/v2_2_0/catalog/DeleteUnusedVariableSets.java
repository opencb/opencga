package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "delete_unused_variablesets",
        description = "Delete unused VariableSets, #1859", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 2,
        date = 20211210, deprecatedSince = "v3.0.0")
public class DeleteUnusedVariableSets extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
