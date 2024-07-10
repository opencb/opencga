package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.addInternalLastModified;

import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "add_modificationDate_to_job.internal", description = "Add internal modificationDate to Job #1810", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG,
        date = 20210812, deprecatedSince = "3.0.0")
public class AddModificationDateToJobInternal extends AddInternalLastModified {

    @Override
    protected void run() throws Exception {
    }
}
