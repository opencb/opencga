package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_name_field_in_cohort_1902",
        description = "Add new name field to Cohort #1902", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220228)
public class AddNameFieldInCohort extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Add new index
        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.COHORT_COLLECTION);
        collection.createIndex(new Document().append("name", 1).append("studyUid", 1), new IndexOptions().background(true));

        // Set name = id
        migrateCollection(MongoDBAdaptorFactory.COHORT_COLLECTION,
                new Document("name", new Document("$exists", false)),
                Projections.include("_id", "id"),
                (doc, bulk) -> bulk.add(new UpdateOneModel<>(
                                eq("_id", doc.get("_id")),
                                new Document("$set", new Document("name", doc.get("id")))
                        )
                ));
    }
}
