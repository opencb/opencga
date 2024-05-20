package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "fix_non_existing_mois_from_panels",
        description = "Remove non-existing MOIs from Panels", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 2,
        date = 20220111, deprecatedSince = "3.0.0")
public class FixNonExistingMoIFromPanels extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
