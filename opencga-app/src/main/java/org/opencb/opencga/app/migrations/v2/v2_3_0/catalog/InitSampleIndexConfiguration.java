package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "init_sampleIndexConfiguration-TASK550",
        description = "Initialise SampleIndexConfiguration in Study internal #TASK-550", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220502,
        deprecatedSince = "v3.0.0")
public class InitSampleIndexConfiguration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
