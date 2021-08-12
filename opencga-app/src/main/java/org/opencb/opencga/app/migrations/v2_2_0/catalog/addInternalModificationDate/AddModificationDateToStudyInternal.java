package org.opencb.opencga.app.migrations.v2_2_0.catalog.addInternalModificationDate;

import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id="add_modificationDate_to_study.internal", description = "Add internal modificationDate to Study #1810", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG)
public class AddModificationDateToStudyInternal extends AddInternalModificationDate {

    @Override
    protected void run() throws Exception {
        addInternalModificationDate(MongoDBAdaptorFactory.STUDY_COLLECTION);
    }
}
