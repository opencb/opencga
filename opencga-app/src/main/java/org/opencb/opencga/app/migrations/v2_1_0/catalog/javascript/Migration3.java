package org.opencb.opencga.app.migrations.v2_1_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.nio.file.Path;

@Migration(id = "init_ca_panel_arrays", description = "Initialise panels array in Clinical Analysis #1759", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, date = 20210528)
public class Migration3 extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        Path path = appHome.resolve("misc/migration/v2.1.0/migration3.js");
        runJavascript(path);
    }
}