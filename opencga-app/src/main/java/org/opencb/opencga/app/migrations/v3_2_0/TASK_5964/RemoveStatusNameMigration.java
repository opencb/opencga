package org.opencb.opencga.app.migrations.v3_2_0.TASK_5964;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Migration(id = "remove_status_name", description = "Remove status name #TASK-5964", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240612)
public class RemoveStatusNameMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Remove from user
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.USER_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_USER_COLLECTION)) {
            removeStatusField(collection, Collections.singletonList("internal.status.name"));
        }

        // Remove from project
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_PROJECT_COLLECTION)) {
            removeStatusField(collection, Collections.singletonList("internal.status.name"));
        }

        // Remove from study
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from sample
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_SAMPLE_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name", "internal.variant.index.status.name",
                    "internal.variant.sampleGenotypeIndex.status.name", "internal.variant.sampleGenotypeIndex.familyStatus.name",
                    "internal.variant.secondarySampleIndex.status.name", "internal.variant.secondarySampleIndex.familyStatus.name",
                    "internal.variant.annotationIndex.status.name", "internal.variant.secondaryAnnotationIndex.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from file
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name", "internal.variant.index.status.name",
                    "internal.variant.annotationIndex.status.name", "internal.variant.secondaryAnnotationIndex.status.name",
                    "internal.alignment.index.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from individual
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_INDIVIDUAL_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from family
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_FAMILY_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from cohort
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_COHORT_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from panel
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION,
                OrganizationMongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_PANEL_COLLECTION)) {
            List<String> statusList = Arrays.asList("status.name", "internal.status.name");
            removeStatusField(collection, statusList);
        }

        // Remove from job
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION)) {
            removeStatusField(collection, Collections.singletonList("internal.status.name"));
        }

        // Remove from clinical
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION)) {
            removeStatusField(collection, Collections.singletonList("internal.status.name"));
        }

        // Remove from interpretation
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION)) {
            removeStatusField(collection, Collections.singletonList("internal.status.name"));
        }
    }

    private void removeStatusField(String collection, List<String> statusList) throws CatalogDBException {
        logger.info("Removing status.name fields from collection {}...", collection);
        List<Bson> exists = statusList.stream().map(Filters::exists).collect(Collectors.toList());
        List<Bson> unsetList = statusList.stream().map(Updates::unset).collect(Collectors.toList());
        getMongoCollection(collection).updateMany(Filters.or(exists), Updates.combine(unsetList));
    }

}
