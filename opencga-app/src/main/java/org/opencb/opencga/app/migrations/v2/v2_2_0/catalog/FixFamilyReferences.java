package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "fix_family_references_in_individual",
        description = "Fix Family references, #TASK-489", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220324, deprecatedSince = "3.0.0")
public class FixFamilyReferences extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
