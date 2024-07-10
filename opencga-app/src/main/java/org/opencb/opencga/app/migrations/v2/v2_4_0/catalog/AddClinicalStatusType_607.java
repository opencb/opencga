package org.opencb.opencga.app.migrations.v2.v2_4_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_clinical_status_type_TASK-607",
        description = "Automatically close cases depending on the new clinical status type #TASK-607", version = "2.4.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220610,
        deprecatedSince = "3.0.0")
public class AddClinicalStatusType_607 extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
