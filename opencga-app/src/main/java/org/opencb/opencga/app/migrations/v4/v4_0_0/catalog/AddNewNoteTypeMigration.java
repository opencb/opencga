package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.notes.NoteType;

import java.util.Arrays;

@Migration(id = "addNewNoteType__task_7046",
        description = "Add new Note type #7046", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241030)
public class AddNewNoteTypeMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        NoteType type = NoteType.UNKNOWN;
        if (ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
            type = NoteType.ORGANIZATION;
        }
        MongoCollection<Document> collection = getMongoCollection(OrganizationMongoDBAdaptorFactory.NOTE_COLLECTION);
        Bson query = Filters.exists("type", false);
        Bson update = Updates.set("type", type);
        logger.info("Setting all notes from organization '{}' to type '{}'", organizationId, type);
        collection.updateMany(query, update);

        // Drop old index
        Document oldIndex = new Document()
                .append("id", 1)
                .append("studyUid", 1)
                .append("version", 1);
        dropIndex(Arrays.asList(OrganizationMongoDBAdaptorFactory.NOTE_COLLECTION, OrganizationMongoDBAdaptorFactory.NOTE_ARCHIVE_COLLECTION),
                oldIndex);
        // Generate new indexes
        catalogManager.installIndexes(organizationId, token);
    }
}
