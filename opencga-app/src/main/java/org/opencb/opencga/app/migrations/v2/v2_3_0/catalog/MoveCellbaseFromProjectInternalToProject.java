package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "move_cellbase_from_project_internal_to_project_TASK-354",
        description = "Move cellbase from project.internal to project #TASK-354", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220422,
        deprecatedSince = "v3.0.0")

public class MoveCellbaseFromProjectInternalToProject extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
