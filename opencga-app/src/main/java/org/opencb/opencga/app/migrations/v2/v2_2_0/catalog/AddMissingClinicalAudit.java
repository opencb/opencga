package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_missing_create_interpretation_in_clinical_audit",
        description = "Add missing CREATE_INTERPRETATION audits in ClinicalAnalysis", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211227, deprecatedSince = "3.0.0")
public class AddMissingClinicalAudit extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
