package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_individual_quality_control_fields",
        description = "Rename IndividualQualityControl fields #1844", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211119, deprecatedSince = "3.0.0")
public class RenameIndividualQualityControlFields extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
