package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_interpretation_stats",
        description = "Add interpretation stats #1819", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 2,
        date = 20210908, deprecatedSince = "v3.0.0")
public class addInterpretationStats extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
