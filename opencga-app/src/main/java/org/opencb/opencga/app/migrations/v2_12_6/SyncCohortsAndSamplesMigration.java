package org.opencb.opencga.app.migrations.v2_12_6;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.List;
import java.util.stream.Collectors;

@Migration(id = "syncCohortsAndSamplesMigration" ,
        description = "Sync array of samples from cohort with array of cohortIds from Sample",
        version = "2.12.6",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20240621
)
public class SyncCohortsAndSamplesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> sampleCollection = getMongoCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        MongoCollection<Document> sampleArchiveCollection = getMongoCollection(MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION);

        queryMongo(MongoDBAdaptorFactory.COHORT_COLLECTION, new Document(),
                Projections.include(CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.SAMPLES.key()),
                cohortDoc -> {
                    String cohortId = cohortDoc.getString(CohortDBAdaptor.QueryParams.ID.key());
                    List<Document> samples = cohortDoc.getList(CohortDBAdaptor.QueryParams.SAMPLES.key(), Document.class);
                    if (CollectionUtils.isNotEmpty(samples)) {
                        List<Long> sampleUids = samples
                                .stream()
                                .map(s -> s.get(SampleDBAdaptor.QueryParams.UID.key(), Number.class).longValue())
                                .collect(Collectors.toList());
                        // Ensure all those samples have a reference to the cohortId
                        Bson query = Filters.and(
                                Filters.in(SampleDBAdaptor.QueryParams.UID.key(), sampleUids),
                                Filters.eq(MongoDBAdaptor.LAST_OF_VERSION, true)
                        );
                        Bson update = Updates.addToSet(SampleDBAdaptor.QueryParams.COHORT_IDS.key(), cohortId);
                        long addedMissingCohort = sampleCollection.updateMany(query, update).getModifiedCount();
                        sampleArchiveCollection.updateMany(query, update);

                        // Ensure there aren't any samples pointing to this cohort that are not in the samples array
                        query = Filters.and(
                                Filters.nin(SampleDBAdaptor.QueryParams.UID.key(), sampleUids),
                                Filters.eq(SampleDBAdaptor.QueryParams.COHORT_IDS.key(), cohortId),
                                Filters.eq(MongoDBAdaptor.LAST_OF_VERSION, true)
                        );
                        update = Updates.pull(SampleDBAdaptor.QueryParams.COHORT_IDS.key(), cohortId);
                        long removedNonAssociatedCohort = sampleCollection.updateMany(query, update).getModifiedCount();
                        sampleArchiveCollection.updateMany(query, update);

                        if (addedMissingCohort > 0 || removedNonAssociatedCohort > 0) {
                            logger.info("Fixed cohort '{}' references. "
                                            + "Added missing reference to {} samples. "
                                            + "Removed non-associated reference from {} samples.",
                                    cohortId, addedMissingCohort, removedNonAssociatedCohort);
                        }
                    }
                });
    }

}
