package org.opencb.opencga.app.migrations.v2.v2_4_3.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "migrate_to_clinical_acmg_TASK-1194",
        description = "Migrate to ClinicalAcmg #TASK-1194", version = "2.4.3",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220809,
        deprecatedSince = "3.0.0")
public class MigrateToClinicalAcmg extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
