package org.opencb.opencga.app.migrations.v2_2_0.catalog.addRegistrationDate;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

public abstract class AddRegistrationDate extends MigrationTool {

    protected void addRegistrationDate(String collection) {
        migrateCollection(collection,
                new Document("internal.registrationDate", new Document("$exists", false)),
                Projections.include("_id", "creationDate", "internal"),
                (doc, bulk) -> {
                    String creationDate = doc.getString("creationDate");

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", doc.get("_id")),
                                    new Document("$set", new Document("internal.registrationDate", creationDate))
                            )
                    );
                }
        );
    }

}
