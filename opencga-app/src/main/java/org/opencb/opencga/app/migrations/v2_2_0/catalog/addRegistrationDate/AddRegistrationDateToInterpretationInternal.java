package org.opencb.opencga.app.migrations.v2_2_0.catalog.addRegistrationDate;

import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id="add_registrationDate_to_interpretation.internal", description = "Add registrationDate to Interpretation #1804",
        version = "2.2.0", language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG)
public class AddRegistrationDateToInterpretationInternal extends AddRegistrationDate {

    @Override
    protected void run() throws Exception {
        addRegistrationDate(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION);
    }
}
