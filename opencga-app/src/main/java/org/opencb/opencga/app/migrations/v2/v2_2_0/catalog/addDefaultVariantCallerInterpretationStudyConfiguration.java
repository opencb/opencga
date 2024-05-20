package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_variant_caller_interpretation_configuration",
        description = "Add default variant caller Interpretation Study configuration #1822", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20210916, deprecatedSince = "v3.0.0")
public class addDefaultVariantCallerInterpretationStudyConfiguration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
