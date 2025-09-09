package org.opencb.opencga.app.migrations.v3.v3_2_0.TASK_5964;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_new_clinical_status",
        description = "Add new ClinicalStatus to ClinicalAnalysis and Interpretation, #TASK-5964",
        version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240611,
        deprecatedSince = "4.0.0")
public class AddNewClinicalStatusValues extends MigrationTool {

    @Override
    protected void run() throws Exception {

    }

}
