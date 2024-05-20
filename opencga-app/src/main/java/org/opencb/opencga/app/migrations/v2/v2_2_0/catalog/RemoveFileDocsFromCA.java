package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_file_docs_from_clinical_analyses",
        description = "Store references of File in Clinical Analysis and not full File documents #1837", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211102, deprecatedSince = "v3.0.0")
public class RemoveFileDocsFromCA extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
