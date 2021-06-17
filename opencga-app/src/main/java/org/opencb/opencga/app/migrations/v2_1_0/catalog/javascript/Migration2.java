package org.opencb.opencga.app.migrations.v2_1_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.nio.file.Path;
import java.nio.file.Paths;

@Migration(id="init_userId_group_arrays", description = "Initialise all userIds arrays from groups #1735", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, rank = 2)
public class Migration2 extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        Path path = appHome.resolve("misc/migration/v2.1.0/migration2.js");
        runJavascript(path);
    }
}
