package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;

@Migration(id="add_variant_caller_interpretation_configuration",
        description = "Add default variant caller Interpretation Study configuration #1822", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        rank = 5)
public class addDefaultVariantCallerInterpretationStudyConfiguration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Get study collection instance
        MongoCollection<Document> studyCollection = getMongoCollection(MongoDBAdaptorFactory.STUDY_COLLECTION);

        // Update
        String field = "internal.configuration.clinical.interpretation.variantCallers";
        Document query = new Document(field, new Document("$exists", false));
        Document update = new Document("$set", new Document(field, Collections.emptyList()));
        studyCollection.updateMany(query, update);
    }
}
