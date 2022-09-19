package org.opencb.opencga.app.migrations.v2_4_4.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.List;

@Migration(id = "avoidCaseStatusIdNull-1938",
        description = "Avoid nullable status id in Clinical Analysis #TASK-1938", version = "2.4.4",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220914)
public class AvoidCaseStatusIdNullMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        List<MongoCollection<Document>> collections = Arrays.asList(
                getMongoCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION),
                getMongoCollection(MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION)
        );

        for (MongoCollection<Document> collection : collections) {
            UpdateResult result = collection.updateMany(
                    Filters.or(Arrays.asList(Filters.exists("status.id", false), Filters.eq("status.id", null))),
                    Updates.set("status.id", "")
            );
            logger.info("Updated {} documents from '{}' collection", result.getModifiedCount(), collection.getNamespace().getCollectionName());
        }
    }

}
