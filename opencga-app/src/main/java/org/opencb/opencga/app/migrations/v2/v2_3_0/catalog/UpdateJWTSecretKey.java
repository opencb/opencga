package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "update_jwt_secret_key_TASK-807",
        description = "Update JWT secret key #TASK-807", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220513,
        deprecatedSince = "v3.0.0")
public class UpdateJWTSecretKey extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
