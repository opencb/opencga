package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_clinical_variant_evidence_review",
        description = "Add new ClinicalVariantEvidenceReview object, #1874", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220112, deprecatedSince = "v3.0.0")
public class ClinicalVariantEvidenceMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
