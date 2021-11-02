package org.opencb.opencga.app.migrations.v2_2_0.catalog.addInternalLastModified;

import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "add_modificationDate_to_cohort.internal", description = "Add internal modificationDate to Cohort #1810",
        version = "2.2.0", language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG,
        date = 20210812)
public class AddModificationDateToCohortInternal extends AddInternalLastModified {

    @Override
    protected void run() throws Exception {
        addInternalModificationDate(MongoDBAdaptorFactory.COHORT_COLLECTION);
    }
}
