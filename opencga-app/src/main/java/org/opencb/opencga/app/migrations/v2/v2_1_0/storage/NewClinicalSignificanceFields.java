package org.opencb.opencga.app.migrations.v2.v2_1_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;


@Migration(id = "new_clinical_significance_fields", description = "Add new clinical significance fields and combinations for variant storage and solr", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 1,
        date = 20210708,
        deprecatedSince = "3.0.0")
public class NewClinicalSignificanceFields extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
