package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.issues_1853_1855;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "individual_ontology_term_migration_#1853",
        description = "Change sex and ethnicity types for OntologyTermAnnotation #1855", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211203, deprecatedSince = "3.0.0")
public class IndividualOntologyTermMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
