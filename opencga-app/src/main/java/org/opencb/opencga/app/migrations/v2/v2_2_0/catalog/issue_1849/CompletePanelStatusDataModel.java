package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issue_1849;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "complete_panel_status_models",
        description = "Complete Panel Status data models #1849", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211126, deprecatedSince = "v3.0.0")
public class CompletePanelStatusDataModel extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
