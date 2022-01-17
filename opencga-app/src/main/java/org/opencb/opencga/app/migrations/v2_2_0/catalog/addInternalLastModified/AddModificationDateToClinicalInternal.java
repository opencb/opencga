package org.opencb.opencga.app.migrations.v2_2_0.catalog.addInternalLastModified;

import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "add_modificationDate_to_clinical.internal", description = "Add internal modificationDate to Clinical #1810",
        version = "2.2.0", language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG,
        date = 20210812)
public class AddModificationDateToClinicalInternal extends AddInternalLastModified {

    @Override
    protected void run() throws Exception {
        addInternalModificationDate(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION);
    }
}
