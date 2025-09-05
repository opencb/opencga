package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "associate_alignment_files__task_5662",
        description = "Associate BAM files with BAI and BIGWIG files and CRAM files with CRAI files #5662", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241028,
        deprecatedSince = "5.0.0")
public class AssociateAlignmentFiles extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
