package org.opencb.opencga.app.migrations.v3.v3_1_0;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.notes.Note;

@Migration(id = "migrate_notes", description = "Migrate notes #TASK-5836", version = "3.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240315)
public class NoteMigration extends MigrationTool {
    
    @Override
    protected void run() throws Exception {
        // First migrate to add the new values
        MongoCollection<Document> collection = getMongoCollection(organizationId, "notes");
        Bson query = Filters.exists(NoteDBAdaptor.QueryParams.STUDY_UID.key(), false);
        Bson update = Updates.combine(
                Updates.set(NoteDBAdaptor.QueryParams.STUDY_UID.key(), -1L),
                Updates.set(NoteDBAdaptor.QueryParams.SCOPE.key(), Note.Scope.ORGANIZATION.name()),
                Updates.set(NoteDBAdaptor.QueryParams.STUDY.key(), ""),
                Updates.set(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PRIVATE.name())
        );
        UpdateResult updateResult = collection.updateMany(query, update);
        if (updateResult.getModifiedCount() == 0) {
            logger.info("Note data model could not be updated. Notes found in organization '{}': {}", organizationId, updateResult.getMatchedCount());
        }

        // Rename Note collection
        RenameCollectionOptions options = new RenameCollectionOptions().dropTarget(true);
        // Rename collection
        String databaseName = dbAdaptorFactory.getMongoDataStore(organizationId).getDatabaseName();
        logger.info("Renaming notes collection for organization '{}' -> Database: '{}'", organizationId, databaseName);
        MongoDataStore mongoDataStore = dbAdaptorFactory.getMongoDataStore(organizationId);
        mongoDataStore.getDb().getCollection("notes").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.NOTE_COLLECTION), options);
        mongoDataStore.getDb().getCollection("notes_archive").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.NOTE_ARCHIVE_COLLECTION), options);
        mongoDataStore.getDb().getCollection("notes_deleted").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.DELETED_NOTE_COLLECTION), options);

        catalogManager.installIndexes(organizationId, token);
    }

}
