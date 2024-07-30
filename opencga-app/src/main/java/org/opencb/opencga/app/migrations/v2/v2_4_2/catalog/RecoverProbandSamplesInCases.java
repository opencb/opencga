package org.opencb.opencga.app.migrations.v2.v2_4_2.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "recover_proband_samples_in_cases_TASK-1470",
        description = "Recover lost samples in clinical collection #TASK-1470", version = "2.4.2",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220725,
        patch = 2,
        deprecatedSince = "3.0.0")
public class RecoverProbandSamplesInCases extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
