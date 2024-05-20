package org.opencb.opencga.app.migrations.v2.v2_8_6.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "mark_unknown_largest_variant_length" ,
        description = "Mark as unknown largest variant length",
        version = "2.8.6",
        domain = Migration.MigrationDomain.STORAGE,
        language = Migration.MigrationLanguage.JAVA,
        patch = 1,
        date = 20230927,
        deprecatedSince = "v3.0.0"
)
public class MarkUnknownLargestVariantLength extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
