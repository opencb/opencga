package org.opencb.opencga.app.migrations.v2.v2_8_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "calculate_pedigree_graph" ,
        description = "Calculate Pedigree Graph for all the families",
        version = "2.8.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230313,
        deprecatedSince = "3.0.0"
)
public class CalculatePedigreeGraphMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
