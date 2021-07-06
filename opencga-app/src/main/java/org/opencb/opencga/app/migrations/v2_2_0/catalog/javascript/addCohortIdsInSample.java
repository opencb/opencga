package org.opencb.opencga.app.migrations.v2_2_0.catalog.javascript;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.nio.file.Path;

@Migration(id = "add_cohortIds_in_sample", description = "Add new list of cohortIds in Sample #1796", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVASCRIPT, rank = 2)
public class addCohortIdsInSample extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        Path path = appHome.resolve("misc/migration/v2.2.0/addCohortIdsInSample.js");
        runJavascript(path);
    }
}

