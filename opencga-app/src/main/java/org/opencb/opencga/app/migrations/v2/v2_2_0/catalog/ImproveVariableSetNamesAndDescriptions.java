package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "improveVariableSetNamesAndDescriptions",
        description = "Improve VariableSet names and descriptions", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211210)
public class ImproveVariableSetNamesAndDescriptions extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
