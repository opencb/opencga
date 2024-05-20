package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issues_1853_1855;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "sample_source_treatments_#1854",
        description = "Sample source, treatments, processing and collection changes #1854", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211201, patch = 2, deprecatedSince = "3.0.0")
public class SampleProcessingAndCollectionChanges extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
