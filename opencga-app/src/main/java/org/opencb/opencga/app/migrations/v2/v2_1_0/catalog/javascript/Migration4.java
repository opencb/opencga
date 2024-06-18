package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "update_qc_file_sample_fields", description = "Update QC fields from Sample and File #1730", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, date = 20210531, patch = 4, deprecatedSince = "3.0.0")
public class Migration4 extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
    }
}
