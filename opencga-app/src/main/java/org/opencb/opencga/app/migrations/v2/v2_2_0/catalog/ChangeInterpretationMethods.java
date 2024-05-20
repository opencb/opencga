package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "change_interpretation_method",
        description = "Remove list of methods from Interpretations #1841", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211112, deprecatedSince = "3.0.0")
public class ChangeInterpretationMethods extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
