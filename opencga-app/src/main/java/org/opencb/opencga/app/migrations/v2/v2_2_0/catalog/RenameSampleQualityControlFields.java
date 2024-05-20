package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_sample_quality_control_fields",
        description = "Rename SampleQualityControl fields #1844", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 2,
        date = 20211119, deprecatedSince = "v3.0.0")
public class RenameSampleQualityControlFields extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
