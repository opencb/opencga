package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_panel_internal_field-TASK_734",
        description = "Add 'internal' to Panel data model #TASK-734", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220509,
        deprecatedSince = "v3.0.0")
public class AddPanelInternalField extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
