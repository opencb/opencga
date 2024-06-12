package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_rga_index_summary_to_study_internal",
        description = "Add RGA Index information to Study Internal #", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210719, deprecatedSince = "3.0.0")
public class addRgaIndexToStudyInternal extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
