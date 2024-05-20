package org.opencb.opencga.app.migrations.v2.v2_4_2.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_clinical_discussion_TASK-1472",
        description = "Add ClinicalDiscussion #TASK-1472", version = "2.4.2",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220727,
        deprecatedSince = "v3.0.0")
public class AddClinicalDiscussion extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
