package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issue_1796;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_cohortIds_in_sample", description = "Add new list of cohortIds in Sample #1796", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, date = 20210706, deprecatedSince = "3.0.0")
public class AddCohortIdsInSample extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
    }
}

