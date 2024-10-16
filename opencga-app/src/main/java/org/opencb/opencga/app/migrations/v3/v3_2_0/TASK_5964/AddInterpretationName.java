package org.opencb.opencga.app.migrations.v3.v3_2_0.TASK_5964;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "add_interpretation_name", description = "Add Interpretation name #TASK-5964", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240610)
public class AddInterpretationName extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Add new Interpretation name field
        Bson nameDoesNotExistQuery = Filters.exists("name", false);
        Bson projection = Projections.include("id");
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION)) {
            migrateCollection(collection, nameDoesNotExistQuery, projection,
                    (document, bulk) -> {
                        Document updateDocument = new Document()
                                .append("name", document.get("id"));
                        bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), new Document("$set", updateDocument)));
                    });
        }
    }

}
