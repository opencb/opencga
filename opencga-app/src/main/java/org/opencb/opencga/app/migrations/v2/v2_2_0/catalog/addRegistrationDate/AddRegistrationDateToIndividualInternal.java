package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog.addRegistrationDate;

import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "add_registrationDate_to_individual.internal", description = "Add registrationDate to Individual #1804", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG,
        date = 20210720, deprecatedSince = "3.0.0")
public class AddRegistrationDateToIndividualInternal extends AddRegistrationDate {

    @Override
    protected void run() throws Exception {
    }
}
