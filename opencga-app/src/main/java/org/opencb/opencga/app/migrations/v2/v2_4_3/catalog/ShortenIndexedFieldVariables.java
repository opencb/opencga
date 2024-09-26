package org.opencb.opencga.app.migrations.v2.v2_4_3.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "shorten_indexed_field_variables-1386",
        description = "Shorten indexed field variables #TASK-1386", version = "2.4.3",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220809,
        deprecatedSince = "3.0.0")
public class ShortenIndexedFieldVariables extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
