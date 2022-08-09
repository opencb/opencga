package org.opencb.opencga.app.migrations.v2_4_3.catalog;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.List;

public class ShortenIndexedFieldVariables extends MigrationTool {

    @Override
    protected void run() throws Exception {
        List<String> collections = Arrays.asList(
                // Samples
                MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                MongoDBAdaptorFactory.DELETED_SAMPLE_COLLECTION,
                // Individuals
                MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION,
                MongoDBAdaptorFactory.DELETED_INDIVIDUAL_COLLECTION,
                // Files
                MongoDBAdaptorFactory.FILE_COLLECTION, MongoDBAdaptorFactory.DELETED_FILE_COLLECTION,
                // Families
                MongoDBAdaptorFactory.FAMILY_COLLECTION, MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION,
                MongoDBAdaptorFactory.DELETED_FAMILY_COLLECTION,
                // Cohorts
                MongoDBAdaptorFactory.COHORT_COLLECTION, MongoDBAdaptorFactory.DELETED_COHORT_COLLECTION);
        for (String collection : collections) {
            MongoCollection<Document> mongoCollection = getMongoCollection(collection);
            mongoCollection.updateMany(new Document(), new Document("$rename", new Document()
                    .append("customAnnotationSets", "_as")
                    .append("customInternalAnnotationSets", "_ias")
            ));
        }

        List<String> indexedCollections = Arrays.asList(
                // Samples
                MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                // Individuals
                MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION,
                // Files
                MongoDBAdaptorFactory.FILE_COLLECTION,
                // Families
                MongoDBAdaptorFactory.FAMILY_COLLECTION, MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION,
                // Cohorts
                MongoDBAdaptorFactory.COHORT_COLLECTION
        );
        // Drop old indexes
        dropIndex(indexedCollections, new Document("customAnnotationSets.as", 1).append("studyUid", 1));
        dropIndex(indexedCollections, new Document("customAnnotationSets.vs", 1).append("studyUid", 1));
        dropIndex(indexedCollections, new Document("customAnnotationSets.id", 1).append("customAnnotationSets.value", 1).append("studyUid", 1));

        dropIndex(indexedCollections, new Document("customInternalAnnotationSets.as", 1).append("studyUid", 1));
        dropIndex(indexedCollections, new Document("customInternalAnnotationSets.vs", 1).append("studyUid", 1));
        dropIndex(indexedCollections, new Document("customInternalAnnotationSets.id", 1).append("customInternalAnnotationSets.value", 1).append("studyUid", 1));

        // Build new indexes
        catalogManager.installIndexes(token);
    }
}
