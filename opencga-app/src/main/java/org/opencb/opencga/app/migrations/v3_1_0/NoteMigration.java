package org.opencb.opencga.app.migrations.v3_1_0;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.notes.Note;

@Migration(id = "migrate_notes", description = "Migrate notes #TASK-5836", version = "3.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240315)
public class NoteMigration extends MigrationTool {
    
    @Override
    protected void run() throws Exception {
        IOManagerFactory ioManagerFactory = new IOManagerFactory();
        dbAdaptorFactory = new MongoDBAdaptorFactory(configuration, ioManagerFactory);
        // First migrate to add the new values
        MongoCollection<Document> collection = getMongoCollection(ParamConstants.ADMIN_ORGANIZATION, "notes");
        Bson query = Filters.exists(NoteDBAdaptor.QueryParams.STUDY_UID.key(), false);
        Bson update = Updates.combine(
                Updates.set(NoteDBAdaptor.QueryParams.STUDY_UID.key(), -1L),
                Updates.set(NoteDBAdaptor.QueryParams.SCOPE.key(), Note.Scope.ORGANIZATION.name()),
                Updates.set(NoteDBAdaptor.QueryParams.STUDY.key(), ""),
                Updates.set(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PRIVATE.name())
        );
        collection.updateMany(query, update);
        renameNoteCollection();

        dbAdaptorFactory.close();
        dbAdaptorFactory = new MongoDBAdaptorFactory(configuration, ioManagerFactory);
        // We run it a second time because the first time it will only rename the "opencga" org as OpenCGA will not be able to know
        // which other organizations are present in the installation (trying to fetch the information from "note" instead of old "notes")
        renameNoteCollection();

        // Reload catalog manager to install missing indexes
        catalogManager = new CatalogManager(configuration);
        catalogManager.installIndexes(token);
    }

    private void renameNoteCollection() throws CatalogDBException {
        // Rename collection
        for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
            String databaseName = dbAdaptorFactory.getMongoDataStore(organizationId).getDatabaseName();
            MongoDataStore mongoDataStore = dbAdaptorFactory.getMongoDataStore(organizationId);
            mongoDataStore.getDb().getCollection("notes").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.NOTE_COLLECTION));
            mongoDataStore.getDb().getCollection("notes_archive").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.NOTE_ARCHIVE_COLLECTION));
            mongoDataStore.getDb().getCollection("notes_deleted").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.DELETED_NOTE_COLLECTION));
        }
    }

}
