package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issue_1795;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_familyIds_in_individual", description = "Add new list of familyIds in Individual #1795", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, date = 20210630, deprecatedSince = "v3.0.0")
public class AddFamilyIdsInIndividual extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
    }
}

