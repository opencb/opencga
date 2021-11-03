package org.opencb.opencga.app.migrations.v2_2_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.nio.file.Path;

@Migration(id = "add_familyIds_in_individual", description = "Add new list of familyIds in Individual #1795", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, date = 20210630)
public class addFamilyIdsInIndividual extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        Path path = appHome.resolve("misc/migration/v2.2.0/addFamilyIdsInIndividual.js");
        runJavascript(path);
    }
}

