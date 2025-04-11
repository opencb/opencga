package org.opencb.opencga.app.migrations.v2.v2_7_0.catalog;


import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "panel_roles_and_modesOfInheritance" ,
        description = "Change panel cancer.role and modeOfInheritance for cancer.roles[] and modesOfInheritance[]",
        version = "2.7.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230117,
        deprecatedSince = "3.0.0"
)
public class CancerRoleAndModeOfInheritanceMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
