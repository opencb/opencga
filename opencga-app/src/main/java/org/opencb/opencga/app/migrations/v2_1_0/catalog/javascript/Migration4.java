package org.opencb.opencga.app.migrations.v2_1_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.nio.file.Path;
import java.nio.file.Paths;

@Migration(id="update_qc_file_sample_fields", description = "Update QC fields from Sample and File #1730", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, rank = 4, patch = 4)
public class Migration4 extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        Path path = appHome.resolve("misc/migration/v2.1.0/migration4.js");
        runJavascript(path);
    }
}
