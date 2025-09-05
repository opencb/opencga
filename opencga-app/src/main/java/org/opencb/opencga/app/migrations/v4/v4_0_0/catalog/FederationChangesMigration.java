package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "federationChanges__task_7192",
        description = "Federation changes, #TASK-7192", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250120,
        deprecatedSince = "5.0.0")
public class FederationChangesMigration extends MigrationTool {

    /*
     * [NEW] Organization -> federation: {clients: [], servers: []}
     * [NEW] Project -> federation: {id: "", description: "", version: ""}
     *               -> internal.federated: [true|false]
     * [NEW] Study -> federation: {id: "", description: "", version: ""}
     *               -> internal.federated: [true|false]
     * [NEW] User -> internal.account.authentication.federation: [true|false]
     */

    @Override
    protected void run() throws Exception {
    }
}
