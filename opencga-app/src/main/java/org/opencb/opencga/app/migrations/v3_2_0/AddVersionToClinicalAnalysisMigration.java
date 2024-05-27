package org.opencb.opencga.app.migrations.v3_2_0;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;

@Migration(id = "add_version_to_clinicalAnalysis", description = "Add version to Clinical Analysis #TASK-5964", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240527)
public class AddVersionToClinicalAnalysisMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Add missing mandatory "versioning" fields to Clinical Analysis
        logger.info("Adding missing 'versioning' fields to Clinical Analysis...");
        Bson versionDoesNotExistQuery = Filters.exists("version", false);
        Bson projection = Projections.include("release");
        migrateCollection(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, versionDoesNotExistQuery, projection,
                (document, bulk) -> {
                    int release = document.get("release", Number.class).intValue();
                    Document updateDocument = new Document()
                            .append("version", 1)
                            .append("_releaseFromVersion", Collections.singletonList(release))
                            .append("_lastOfVersion", true)
                            .append("_lastOfRelease", true);
                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), new Document("$set", updateDocument)));
                });

        // Recalculate indexes
        logger.info("Installing new indexes...");
        catalogManager.installIndexes(organizationId, token);

        // Copy all Clinical Analyses to the archive collection
        logger.info("Copying all the data from the main collection to the archive one...");
        copyData(new Document(), OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION);


    }

}
