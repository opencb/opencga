package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issue_1849;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "complete_user_status_models",
        description = "Complete User Status data models #1849", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211126, deprecatedSince = "3.0.0")
public class CompleteUserStatusDataModel extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
