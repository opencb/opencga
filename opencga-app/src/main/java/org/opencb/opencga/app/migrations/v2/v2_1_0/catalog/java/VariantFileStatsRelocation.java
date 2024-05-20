package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.java;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "move_variant_file_stats_to_qc", description = "Move opencga_file_variant_stats annotation set from variable sets to " +
        "FileQualityControl", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 1,
        date = 20210614,
        deprecatedSince = "3.0.0")
public class VariantFileStatsRelocation extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
