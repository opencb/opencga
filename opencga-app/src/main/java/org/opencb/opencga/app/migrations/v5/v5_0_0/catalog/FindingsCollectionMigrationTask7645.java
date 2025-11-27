package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.opencga.catalog.db.api.InterpretationFindingsDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Migration(id = "new_findings_collection_TASK_7645", offline = true,
        description = "Extract findings to a separate collection #TASK-7645", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20251127)
public class FindingsCollectionMigrationTask7645 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        logger.info("Starting findings collection migration for organization: {}", organizationId);
        OrganizationMongoDBAdaptorFactory orgFactory = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId);

        // ------------ Try to create the findings collections ------------
        logger.info("Creating findings collections...");
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.FINDINGS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.FINDINGS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_FINDINGS_COLLECTION)) {
            try {
                orgFactory.getMongoDataStore().createCollection(collection);
                logger.info("Successfully created collection: {}", collection);
            } catch (Exception e) {
                logger.info("Collection {} already exists. Migration may have been run previously.", collection);
            }
        }

        // ------------ Create new indexes before trying to migrate data ------------
        logger.info("Creating indexes for findings collections...");
        orgFactory.createIndexes();
        logger.info("Indexes created successfully");

        // ------------ Migrate interpretations archive collection ------------
        // 1. Obtain all different interpretation uids from the interpretations archive collection
        // 2. For each interpretation:
        //    2.1. sort them by version
        //    2.2. obtain its findings
        //    2.3. insert the findings in the findings collection
        //    2.4. update the interpretation to only store references to the findings from the findings collection (id and version)
        //    2.5. repeat for all versions until the last one
        //    2.6. replicate the interpretation findings references from the last version into the current interpretations collection
        // 3. Empty array of findings from the delete interpretations collection

        // 1. Obtain all different interpretation uids from the interpretations archive collection
        Bson filter = Filters.or(
                Filters.and(
                        Filters.exists("primaryFindings", true),
                        Filters.ne("primaryFindings", null),
                        Filters.ne("primaryFindings", Collections.emptyList()),
                        Filters.not(Filters.elemMatch("primaryFindings", Filters.exists("version", true)))
                ),
                Filters.and(
                        Filters.exists("secondaryFindings", true),
                        Filters.ne("secondaryFindings", null),
                        Filters.ne("secondaryFindings", Collections.emptyList()),
                        Filters.not(Filters.elemMatch("secondaryFindings", Filters.exists("version", true)))
                )
        );
        MongoCollection<Document> interpretationCol = getMongoCollection(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION);
        MongoCollection<Document> interpretationArchCol = getMongoCollection(OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION);
        InterpretationConverter converter = new InterpretationConverter();

        logger.info("Starting migration of interpretation archive collection...");
        int totalInterpretations = 0;
        int totalVersionsProcessed = 0;

        try (MongoCursor<Long> iterable = interpretationArchCol.distinct("uid", filter, Long.class).cursor()) {
            while (iterable.hasNext()) {
                long interpretationUid = iterable.next();
                totalInterpretations++;
                logger.info("Processing interpretation UID: {} ({} of total)", interpretationUid, totalInterpretations);

                Bson query = Filters.eq("uid", interpretationUid);
                Bson projection = Projections.include("primaryFindings", "secondaryFindings", "id", "uid", "version", "studyUid");
                try (MongoCursor<Document> cursor = interpretationArchCol
                        .find(query)
                        .projection(projection)
                        .sort(Sorts.ascending("version"))
                        .cursor()) {
                    int versionCount = 0;
                    while (cursor.hasNext()) {
                        versionCount++;
                        totalVersionsProcessed++;
                        Document interpretationDoc = cursor.next();
                        orgFactory.getInterpretationDBAdaptor().runTransaction(session -> {
                            Interpretation interpretation = converter.convertToDataModelType(interpretationDoc);
                            int version = interpretation.getVersion();
                            String interpretationId = interpretation.getId();
                            logger.debug("Processing interpretation '{}' (ID: {}, version: {})", interpretationId, interpretationUid, version);
                            // Obtain findings
                            List<Document> primaryFindings = interpretationDoc.getList("primaryFindings", Document.class,
                                    Collections.emptyList());
                            List<Document> secondaryFindings = interpretationDoc.getList("secondaryFindings", Document.class,
                                    Collections.emptyList());

                            logger.debug("Found {} primary findings and {} secondary findings for interpretation '{}' version {}",
                                    primaryFindings.size(), secondaryFindings.size(), interpretationId, version);

                            MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                            if (!primaryFindings.isEmpty()) {
                                List<ClinicalVariant> clinicalVariants = orgFactory.getFindingsDBAdaptor().updateFindings(session,
                                        interpretation, Interpretation::getPrimaryFindings, primaryFindings, ParamUtils.UpdateAction.SET);
                                List<Document> findings = convertFindings(clinicalVariants);
                                updateDocument.getSet().put("primaryFindings", findings);
                                logger.debug("Migrated {} primary findings to findings collection for interpretation '{}'",
                                        clinicalVariants.size(), interpretationId);
                            }
                            if (!secondaryFindings.isEmpty()) {
                                List<ClinicalVariant> clinicalVariants = orgFactory.getFindingsDBAdaptor().updateFindings(session,
                                        interpretation, Interpretation::getSecondaryFindings, secondaryFindings, ParamUtils.UpdateAction.SET);
                                List<Document> findings = convertFindings(clinicalVariants);
                                updateDocument.getSet().put("secondaryFindings", findings);
                                logger.debug("Migrated {} secondary findings to findings collection for interpretation '{}'",
                                        clinicalVariants.size(), interpretationId);
                            }
                            if (!updateDocument.getSet().isEmpty()) {
                                Bson intQuery = Filters.and(
                                        Filters.eq("uid", interpretationUid),
                                        Filters.eq("version", interpretation.getVersion())
                                );
                                interpretationArchCol.updateOne(session, intQuery, updateDocument.toFinalUpdateDocument());
                                logger.debug("Updated interpretation '{}' version {} with finding references", interpretationId, version);
                            }
                            return null;
                        });
                    }
                    logger.info("Processed {} versions for interpretation UID: {}", versionCount, interpretationUid);

                    // Find last of version from the archive collection to replicate into the current interpretations collection
                    query = Filters.and(
                            Filters.eq(MongoDBAdaptor.LAST_OF_VERSION, true),
                            Filters.eq("uid", interpretationUid)
                    );
                    projection = Projections.exclude("_id");
                    Document lastVersionDoc = interpretationArchCol.find(query).projection(projection).first();
                    if (lastVersionDoc != null) {
                        interpretationCol.updateOne(query, new Document("$set", lastVersionDoc));
                        logger.debug("Replicated last version of interpretation UID {} to current interpretations collection", interpretationUid);
                    } else {
                        logger.warn("Could not find last version for interpretation UID: {}", interpretationUid);
                    }
                }
            }

            logger.info("Migration of interpretation archive collection completed. Total interpretations: {}, Total versions processed: {}",
                    totalInterpretations, totalVersionsProcessed);

            // 3. Empty array of findings from the delete interpretations collection
            logger.info("Emptying findings arrays from deleted interpretations collection...");
            MongoCollection<Document> deletedInterpretationCol = getMongoCollection(OrganizationMongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION);
            Bson update = Updates.combine(
                    Updates.set("primaryFindings", Collections.emptyList()),
                    Updates.set("secondaryFindings", Collections.emptyList())
            );
            long deletedUpdateCount = deletedInterpretationCol.updateMany(new Document(), update).getModifiedCount();
            logger.info("Emptied findings from {} deleted interpretations", deletedUpdateCount);
        }

        logger.info("Findings collection migration completed successfully for organization: {}", organizationId);
    }

    private List<Document> convertFindings(List<ClinicalVariant> clinicalVariants) {
        List<Document> findingsDocs = new ArrayList<>(clinicalVariants.size());
        for (ClinicalVariant clinicalVariant : clinicalVariants) {
            Document findingDoc = new Document()
                    .append(InterpretationFindingsDBAdaptor.QueryParams.ID.key(), clinicalVariant.getId())
                    .append(InterpretationFindingsDBAdaptor.QueryParams.VERSION.key(), clinicalVariant.getVersion());
            findingsDocs.add(findingDoc);
        }
        return findingsDocs;
    }

}
