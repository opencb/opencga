package org.opencb.opencga.app.migrations.v3_1_0;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
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

import java.util.Collections;
import java.util.List;

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
        UpdateResult updateResult = collection.updateMany(query, update);
        if (updateResult.getModifiedCount() == 0) {
            // Check there are at least 2 organizations present
            logger.info("Note data model could not be updated. Detected organizations are: {}", StringUtils.join(dbAdaptorFactory.getOrganizationIds(), ","));
            if (dbAdaptorFactory.getOrganizationIds().size() == 2) {
                logger.info("Nothing to migrate");
                return;
            } else {
                throw new CatalogDBException("Notes could not be found to migrate.");
            }
        }
        renameNoteCollection(Collections.singletonList(ParamConstants.ADMIN_ORGANIZATION));

        dbAdaptorFactory.close();
        dbAdaptorFactory = new MongoDBAdaptorFactory(configuration, ioManagerFactory);
        // We run it a second time because the first time it will only rename the "opencga" org as OpenCGA will not be able to know
        // which other organizations are present in the installation (trying to fetch the information from "note" instead of old "notes")
        List<String> organizationIds = dbAdaptorFactory.getOrganizationIds();
        organizationIds.remove(ParamConstants.ADMIN_ORGANIZATION);
        renameNoteCollection(organizationIds);

        // Reload catalog manager to install missing indexes
        catalogManager = new CatalogManager(configuration);
        catalogManager.installIndexes(token);
    }

    private void renameNoteCollection(List<String> organizationIds) throws CatalogDBException {
        RenameCollectionOptions options = new RenameCollectionOptions().dropTarget(true);
        // Rename collection
        for (String organizationId : organizationIds) {
            String databaseName = dbAdaptorFactory.getMongoDataStore(organizationId).getDatabaseName();
            logger.info("Renaming notes collection for organization '{}' -> Database: '{}'", organizationId, databaseName);
            MongoDataStore mongoDataStore = dbAdaptorFactory.getMongoDataStore(organizationId);
            mongoDataStore.getDb().getCollection("notes").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.NOTE_COLLECTION), options);
            mongoDataStore.getDb().getCollection("notes_archive").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.NOTE_ARCHIVE_COLLECTION), options);
            mongoDataStore.getDb().getCollection("notes_deleted").renameCollection(new MongoNamespace(databaseName, OrganizationMongoDBAdaptorFactory.DELETED_NOTE_COLLECTION), options);
        }
    }

}
