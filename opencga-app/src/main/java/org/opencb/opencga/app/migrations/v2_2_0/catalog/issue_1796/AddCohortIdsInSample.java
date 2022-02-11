package org.opencb.opencga.app.migrations.v2_2_0.catalog.issue_1796;

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

@Migration(id = "add_cohortIds_in_sample", description = "Add new list of cohortIds in Sample #1796", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA, date = 20210706)
public class AddCohortIdsInSample extends MigrationTool {

    @Override
    protected void run() throws MigrationException {
        MongoCollection<Document> cohortCollection = getMongoCollection(MongoDBAdaptorFactory.COHORT_COLLECTION);

        migrateCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION,
                new Document("cohortIds", new Document("$exists", false)),
                Projections.include("_id", "uid"),
                (sampleDoc, bulk) -> {
                    MongoCursor<Document> cohortIterator = cohortCollection
                            .find(new Document("samples.uid", sampleDoc.get("uid")))
                            .projection(new Document("id", 1)).iterator();

                    List<String> cohortIds = new ArrayList<>();
                    while (cohortIterator.hasNext()) {
                        Document cohortDoc = cohortIterator.next();
                        cohortIds.add(cohortDoc.getString("id"));
                    }

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", sampleDoc.get("_id")),
                            new Document("$set", new Document("cohortIds", cohortIds)))
                    );
                }
        );
    }
}

