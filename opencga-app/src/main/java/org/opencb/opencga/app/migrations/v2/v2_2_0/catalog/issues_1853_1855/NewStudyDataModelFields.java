package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issues_1853_1855;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "new_study_data_model_fields_#1853",
        description = "New Study 'sources', 'type' and 'additionalInfo' fields #1853", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211202, deprecatedSince = "3.0.0")
public class NewStudyDataModelFields extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
