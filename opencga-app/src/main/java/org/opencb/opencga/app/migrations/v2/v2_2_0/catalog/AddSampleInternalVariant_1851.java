package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_sample_internal_variant_1851",
        description = "Add new SampleInternalVariant #1851", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211129, deprecatedSince = "v3.0.0")
public class AddSampleInternalVariant_1851 extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
