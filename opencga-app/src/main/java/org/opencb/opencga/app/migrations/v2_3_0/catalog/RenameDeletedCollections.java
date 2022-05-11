package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory.*;

@Migration(id = "rename_deleted_collections_TASK-359",
        description = "Rename deleted collections #TASK-359", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220502)
public class RenameDeletedCollections extends MigrationTool {

    @Override
    protected void run() throws Exception {

        Map<String, String> collectionRename = new HashMap<>();
        collectionRename.put(OLD_DELETED_USER_COLLECTION, DELETED_USER_COLLECTION);
        collectionRename.put(OLD_DELETED_STUDY_COLLECTION, DELETED_STUDY_COLLECTION);
        collectionRename.put(OLD_DELETED_FILE_COLLECTION, DELETED_FILE_COLLECTION);
        collectionRename.put(OLD_DELETED_JOB_COLLECTION, DELETED_JOB_COLLECTION);
        collectionRename.put(OLD_DELETED_SAMPLE_COLLECTION, DELETED_SAMPLE_COLLECTION);
        collectionRename.put(OLD_DELETED_INDIVIDUAL_COLLECTION, DELETED_INDIVIDUAL_COLLECTION);
        collectionRename.put(OLD_DELETED_COHORT_COLLECTION, DELETED_COHORT_COLLECTION);
        collectionRename.put(OLD_DELETED_FAMILY_COLLECTION, DELETED_FAMILY_COLLECTION);
        collectionRename.put(OLD_DELETED_PANEL_COLLECTION, DELETED_PANEL_COLLECTION);
        collectionRename.put(OLD_DELETED_CLINICAL_ANALYSIS_COLLECTION, DELETED_CLINICAL_ANALYSIS_COLLECTION);
        collectionRename.put(OLD_DELETED_INTERPRETATION_COLLECTION, DELETED_INTERPRETATION_COLLECTION);

        for (Map.Entry<String, String> entry : collectionRename.entrySet()) {
            MongoCollection<Document> collection = getMongoCollection(entry.getKey());
            collection.renameCollection(new MongoNamespace(dbAdaptorFactory.getMongoDataStore().getDatabaseName(), entry.getValue()));
        }

    }
}
