package org.opencb.opencga.app.migrations.v2_2_0.catalog.issue_1795;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_familyIds_in_individual", description = "Add new list of familyIds in Individual #1795", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, date = 20210630)
public class AddFamilyIdsInIndividual extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        MongoCollection<Document> familyCollection = getMongoCollection(MongoDBAdaptorFactory.FAMILY_COLLECTION);

        migrateCollection(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                new Document("familyIds", new Document("$exists", false)),
                Projections.include("_id", "uid"),
                (individualDoc, bulk) -> {
                    MongoCursor<Document> familyIterator = familyCollection
                            .find(new Document("members.uid", individualDoc.get("uid")))
                            .projection(new Document("id", 1)).iterator();

                    List<String> familyIds = new ArrayList<>();
                    while (familyIterator.hasNext()) {
                        Document familyDoc = familyIterator.next();
                        familyIds.add(familyDoc.getString("id"));
                    }

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", individualDoc.get("_id")),
                            new Document("$set", new Document("familyIds", familyIds)))
                    );
                }
        );
    }
}

