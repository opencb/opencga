package org.opencb.opencga.app.migrations.v2.v2_2_1.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_panel_gene_name_index",
        description = "Add Panel gene.name index #TASK-602", version = "2.2.1",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220406, deprecatedSince = "3.0.0")
public class AddPanelGeneNameIndex extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
