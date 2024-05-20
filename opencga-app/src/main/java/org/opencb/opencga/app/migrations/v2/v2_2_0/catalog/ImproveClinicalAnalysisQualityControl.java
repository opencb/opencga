package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "improve_ca_quality_control",
        description = "Quality control normalize comments and fields #1826", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211001, deprecatedSince = "v3.0.0")
public class ImproveClinicalAnalysisQualityControl extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
