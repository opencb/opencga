package org.opencb.opencga.app.migrations.v2_4_3.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class ShortenIndexedFieldVariables extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(Arrays.asList(
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
                MongoDBAdaptorFactory.COHORT_COLLECTION, MongoDBAdaptorFactory.DELETED_COHORT_COLLECTION),
                new Document("$or", Arrays.asList(
                        "customAnnotationSets", new Document("$exists", true),
                        "customInternalAnnotationSets", new Document("$exists", true)
                )),
                Projections.include("customAnnotationSets", "customInternalAnnotationSets"),
                ((document, bulk) -> {
                    Document replacedDotsDoc = GenericDocumentComplexConverter.replaceDots(document);
                    Document set = new Document()
                            .append("_as", replacedDotsDoc.get("customAnnotationSets"))
                            .append("_ias", replacedDotsDoc.get("customInternalAnnotationSets"));
                    Document unset = new Document()
                            .append("customAnnotationSets", "")
                            .append("customInternalAnnotationSets", "");

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", document.get("_id")),
                                    new Document()
                                            .append("$set", set)
                                            .append("$unset", unset)
                            )
                    );
                })
        );

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
