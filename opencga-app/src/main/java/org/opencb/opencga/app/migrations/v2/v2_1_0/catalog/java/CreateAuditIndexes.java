package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.java;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "create_audit_indexes", description = "Create Audit indexes", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210622,
        deprecatedSince = "3.0.0")
public class CreateAuditIndexes extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}

