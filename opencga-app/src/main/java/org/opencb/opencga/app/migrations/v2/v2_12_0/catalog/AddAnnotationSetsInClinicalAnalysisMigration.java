package org.opencb.opencga.app.migrations.v2.v2_12_0.catalog;


import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_annotation_sets_to_clinical_analysis" ,
        description = "Add private annotation fields to ClinicalAnalysis documents #TASK-5198",
        version = "2.12.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20231116,
        deprecatedSince = "v3.0.0"
)
public class AddAnnotationSetsInClinicalAnalysisMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
