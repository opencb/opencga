package org.opencb.opencga.app.migrations.v2.v2_9_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_cellbase_token_2_api_key" ,
        description = "Rename CellBase Token to ApiKey",
        version = "2.9.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230829,
        deprecatedSince = "3.0.0"
)
public class RenameCellBaseToken2ApiKey extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
