package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_name_field_in_cohort_1902",
        description = "Add new name field to Cohort #1902", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220228, deprecatedSince = "3.0.0")
public class AddNameFieldInCohort extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
