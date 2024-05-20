package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.java;


import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "move_study_clinical_config_to_internal",
        description = "Move Study ClinicalConfiguration to internal.configuration", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210708,
        deprecatedSince = "v3.0.0")
public class StudyClinicalConfigurationRelocation extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
