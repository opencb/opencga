package org.opencb.opencga.app.migrations.v2.v2_4_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_private_dueDate_TASK-803",
        description = "Add private _dueDate to ClinicalAnalysis #TASK-803", version = "2.4.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220530,
        deprecatedSince = "v3.0.0")
public class AddPrivateDueDate_803 extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
