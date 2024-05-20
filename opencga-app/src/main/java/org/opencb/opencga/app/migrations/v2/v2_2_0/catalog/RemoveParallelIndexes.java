package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_parallel_indexes",
        description = "Remove parallel array indexes #CU-20jc4tx", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220310, deprecatedSince = "v3.0.0")
public class RemoveParallelIndexes extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
