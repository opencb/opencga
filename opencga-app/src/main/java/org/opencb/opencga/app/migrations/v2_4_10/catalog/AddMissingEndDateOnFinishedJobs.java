package org.opencb.opencga.app.migrations.v2_4_10.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "AddMissingEndDateOnFinishedJobs" ,
        description = "Add missing end date on finished jobs",
        version = "2.4.10",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20221026
)
public class AddMissingEndDateOnFinishedJobs extends MigrationTool {
    @Override
    protected void run() throws Exception {
        throw new IllegalStateException("PENDING");
    }
}
