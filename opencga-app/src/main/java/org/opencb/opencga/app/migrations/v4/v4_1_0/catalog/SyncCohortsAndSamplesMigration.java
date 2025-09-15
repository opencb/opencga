package org.opencb.opencga.app.migrations.v4.v4_1_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Migration(id = "syncCohortsAndSamplesMigration__task_7671" ,
        description = "Remove references in Sample from non-existing cohorts.",
        version = "4.1.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20250605
)
public class SyncCohortsAndSamplesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> sampleCollection = getMongoCollection(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION);
        MongoCollection<Document> sampleArchiveCollection = getMongoCollection(OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION);

        // Fill map study uid - fqn
        Map<Long, String> uidFqnMap = new HashMap<>();
        Bson studyProjection = Projections.include(StudyDBAdaptor.QueryParams.UID.key(), StudyDBAdaptor.QueryParams.FQN.key());
        queryMongo(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, new Document(), studyProjection, study -> {
            long studyUid = study.get(StudyDBAdaptor.QueryParams.UID.key(), Number.class).longValue();
            String studyFqn = study.getString(StudyDBAdaptor.QueryParams.FQN.key());
            uidFqnMap.put(studyUid, studyFqn);
        });

        for (Long studyUid : uidFqnMap.keySet()) {
            // Obtain all distinct cohortIds from the samples in this study
            List<?> cohortIdsInSamples = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).distinct(studyUid, "cohortIds",
                    new Query(), null).getResults();
            if (CollectionUtils.isEmpty(cohortIdsInSamples)) {
                logger.info("No cohortIds found in samples for study '{}'. Skipping.", uidFqnMap.get(studyUid));
                continue;
            }
            // Convert to a set of strings
            List<String> cohortIdStringInSamples = cohortIdsInSamples.stream().map(Object::toString).collect(Collectors.toList());

            // Query all existing cohort ids in the study
            List<?> cohortIds = dbAdaptorFactory.getCatalogCohortDBAdaptor(organizationId).distinct(studyUid, "id", new Query(), null)
                    .getResults();
            // Convert to a set of strings
            Set<String> cohortIdsSet = cohortIds.stream().map(Object::toString).collect(Collectors.toSet());

            // Obtain a list with the cohorts that are referenced in the samples but do not exist
            List<String> missingCohortIds = cohortIdStringInSamples.stream()
                    .filter(cohortId -> !cohortIdsSet.contains(cohortId))
                    .collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(missingCohortIds)) {
                // Remove the missing cohortIds from the samples
                logger.info("Removing {} missing cohortIds from samples in study '{}'.", missingCohortIds.size(), uidFqnMap.get(studyUid));

                Document query = new Document()
                        .append("studyUid", studyUid)
                        .append("cohortIds", new Document("$in", missingCohortIds));
                Bson update = new Document("$pullAll", new Document("cohortIds", missingCohortIds));
                // Update the samples in the main collection
                sampleCollection.updateMany(query, update);
                // Update the samples in the archive collection
                sampleArchiveCollection.updateMany(query, update);
            }
        }
    }

}
