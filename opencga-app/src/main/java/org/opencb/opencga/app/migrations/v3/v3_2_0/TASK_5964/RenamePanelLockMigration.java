package org.opencb.opencga.app.migrations.v3.v3_2_0.TASK_5964;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "rename_panel_lock_from_clinical__task_5964", description = "Rename 'panelLock' to 'panelLocked' #TASK-5964",
        version = "3.2.0", language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240710)
public class RenamePanelLockMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = new Document();

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION)) {
            getMongoCollection(collection).updateMany(query, new Document("$rename", new Document("panelLock", "panelLocked")));
        }
    }

}
