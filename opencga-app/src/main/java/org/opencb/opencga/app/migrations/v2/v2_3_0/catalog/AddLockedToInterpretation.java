package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_locked_to_interpretation_TASK-552",
        description = "Add new 'locked' field to Interpretation #TASK-552", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220401,
        deprecatedSince = "3.0.0")
public class AddLockedToInterpretation extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
