package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_interpretation_stats_field",
        description = "Rename interpretation stats field #1819", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211115, deprecatedSince = "v3.0.0")
public class RenameInterpretationFindingStats extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
