package org.opencb.opencga.app.migrations.v2_12_0.catalog;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.AnnotationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

@Migration(id = "add_annotation_sets_to_clinical_analysis" ,
        description = "Add private annotation fields to ClinicalAnalysis documents",
        version = "2.12.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20231116
)
public class AddAnnotationSetsInClinicalAnalysis extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists(AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key(), false);
        Document update = new Document("$set", new Document()
                .append(AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key(), Collections.emptyList())
                .append(AnnotationMongoDBAdaptor.AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key(), Collections.emptyList())
                .append(AnnotationMongoDBAdaptor.AnnotationSetParams.PRIVATE_VARIABLE_SET_MAP.key(), Collections.emptyMap())
                .append(AnnotationMongoDBAdaptor.AnnotationSetParams.PRIVATE_INTERNAL_VARIABLE_SET_MAP.key(), Collections.emptyMap())
        );
        // Initialise private fields in all Clinical Analysis documents
        for (String collection : Arrays.asList(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION)) {
            MongoCollection<Document> mongoCollection = getMongoCollection(collection);
            UpdateResult updateResult = mongoCollection.updateMany(query, update);
            logger.info("{} clinical analysis documents updated from the {} collection", updateResult.getModifiedCount(), collection);
        }
    }
}
