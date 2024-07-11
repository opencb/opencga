package org.opencb.opencga.app.migrations.v2.v2_3_1.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "bugfix_move_cellbase_from_project_internal_to_project_TASK-1100",
        description = "Bugfix: Move cellbase from project.internal to project #TASK-1100", version = "2.3.1",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220620,
        deprecatedSince = "3.0.0")

public class MoveCellbaseFromProjectInternalToProjectBugFix extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
