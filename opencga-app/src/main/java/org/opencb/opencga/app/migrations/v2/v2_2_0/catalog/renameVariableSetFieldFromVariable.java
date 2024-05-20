package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;


import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_variableset_field",
        description = "Rename Variable variableSet field to variables #1823", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20210920, deprecatedSince = "3.0.0")
public class renameVariableSetFieldFromVariable extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
