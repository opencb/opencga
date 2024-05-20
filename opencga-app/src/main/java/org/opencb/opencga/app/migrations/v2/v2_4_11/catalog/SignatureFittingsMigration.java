package org.opencb.opencga.app.migrations.v2.v2_4_11.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "signature_fittings" ,
        description = "Replace fitting for fittings in Signature",
        version = "2.4.11",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20221109,
        deprecatedSince = "3.0.0"
)
public class SignatureFittingsMigration extends MigrationTool {
    @Override
    protected void run() throws Exception {
    }
}
