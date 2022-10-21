package org.opencb.opencga.app.migrations.v2_5_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

@Migration(id = "initialise_identifiers_in_individuals-2214",
        description = "Initialise Identifiers in Individual #TASK-2214", version = "2.5.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20221021)
public class InitialiseIdentifiersInIndividuals extends MigrationTool {

    @Override
    protected void run() throws Exception {
        for (String col : Arrays.asList(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION,
                MongoDBAdaptorFactory.DELETED_INDIVIDUAL_COLLECTION)) {
            MongoCollection<Document> collection = getMongoCollection(col);
            UpdateResult result = collection.updateMany(Filters.exists("identifiers", false),
                    Updates.set("identifiers", Collections.emptyList()));
            logger.info("Updated {} documents in collection '{}'", result.getModifiedCount(), col);
        }
    }

}
