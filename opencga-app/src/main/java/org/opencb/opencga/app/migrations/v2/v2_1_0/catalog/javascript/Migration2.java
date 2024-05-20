package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "init_userId_group_arrays", description = "Initialise all userIds arrays from groups #1735", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, date = 20210528, deprecatedSince = "v3.0.0")
public class Migration2 extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
    }
}
