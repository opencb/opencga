package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisInternal;
import org.opencb.opencga.core.models.clinical.CvdbIndex;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.study.CatalogStudyConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Migration(id = "add_clinical_configuration_migration_7756",
        description = "Add CVDB index status to Clinical Analysis #TASK-5610", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250625)
public class ClinicalMigrationTask7756 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        addNewQualityControlStatusField(Arrays.asList(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_SAMPLE_COLLECTION),
                Filters.or(
                        Filters.ne("qualityControl.files", Collections.emptyList()),
                        Filters.ne("qualityControl.variant.variantStats", Collections.emptyList()),
                        Filters.ne("qualityControl.variant.signatures", Collections.emptyList()),
                        Filters.ne("qualityControl.variant.genomePlot", null)
                ));

        addNewQualityControlStatusField(Arrays.asList(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                        OrganizationMongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_INDIVIDUAL_COLLECTION),
                Filters.or(
                        Filters.ne("qualityControl.inferredSexReports", Collections.emptyList()),
                        Filters.ne("qualityControl.mendelianErrorReports", Collections.emptyList())
                ));

        addNewQualityControlStatusField(Arrays.asList(OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION,
                        OrganizationMongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_FAMILY_COLLECTION),
                Filters.ne("qualityControl.relatedness", Collections.emptyList()));

        migrateClinicalAnalysisCvdbIndex();

        migrateStudyConfiguration();
    }


    private void migrateStudyConfiguration() throws CatalogDBException {
        //        Study:
        //        + internal.configuration.catalog: {
        //            cvdb: {},
        //            variantQualityControl: {}
        //        }
        logger.info("Starting migration of Study configuration catalog");

        List<String> studyCollections = Arrays.asList(
                OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION
        );
        logger.debug("Processing study collections: {}", studyCollections);

        CatalogStudyConfiguration catalogStudyConfiguration = CatalogStudyConfiguration.defaultConfiguration();
        Document studyConfigurationDoc = convertToDocument(catalogStudyConfiguration);

        migrateCollection(studyCollections, Filters.exists("internal.configuration.catalog", false),
                Projections.include("_id"), (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    updateDocument.getSet().put("internal.configuration.catalog", studyConfigurationDoc);
                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });

        logger.info("Finished migration of Study configuration catalog");
    }

    private void migrateClinicalAnalysisCvdbIndex() throws CatalogDBException {
        //        ClinicalAnalysis
        //                - internal.cvdbIndex
        //                + internal.cvdbIndex: {
        //                      status: // old internal.cvdbIndex
        //                      jobId: ""
        //                  }

        logger.info("Starting migration of clinical analysis CVDB index");

        List<String> clinicalCollections = Arrays.asList(
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION
        );
        logger.debug("Processing clinical collections: {}", clinicalCollections);

        migrateCollection(clinicalCollections, Filters.exists("internal.cvdbIndex.jobId", false), Projections.include("_id", "internal"), (document, bulk) -> {
            MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

            Document internalDoc = document.get("internal", Document.class);
            if (internalDoc == null) {
                updateDocument.getSet().put("internal", convertToDocument(ClinicalAnalysisInternal.init()));
            } else {
                Document cvdbIndexStatus = internalDoc.get("cvdbIndex", Document.class);
                if (cvdbIndexStatus == null) {
                    updateDocument.getSet().put("internal.cvdbIndex", convertToDocument(CvdbIndex.init()));
                } else {
                    Document cvdbIndex = new Document()
                            .append("jobId", "")
                            .append("status", cvdbIndexStatus);
                    updateDocument.getSet().put("internal.cvdbIndex", cvdbIndex);
                }
            }

            bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
            logger.trace("Added update for document with id: {}", document.get("_id"));
        });

        logger.info("Finished migration of clinical analysis CVDB index");
    }

    private void addNewQualityControlStatusField(List<String> collections, Bson qualityControlCalculatedFilter) throws CatalogDBException {
        //        Sample | Individual | Family
        //                + internal.qualityControlStatus

        Bson query = Filters.and(
                Filters.exists("internal.qualityControlStatus", false),
                qualityControlCalculatedFilter);
        QualityControlStatus controlStatus = QualityControlStatus.init();
        controlStatus.setId(QualityControlStatus.READY);
        Bson update = Updates.set("internal.qualityControlStatus", convertToDocument(controlStatus));

        Bson initQuery = Filters.exists("internal.qualityControlStatus", false);
        QualityControlStatus initControlStatus = QualityControlStatus.init();
        Bson initUpdate = Updates.set("internal.qualityControlStatus", convertToDocument(initControlStatus));

        for (String collection : collections) {
            long updatedDocs = getMongoCollection(collection).updateMany(query, update).getModifiedCount();
            logger.info("Updated {} documents in {} with qualityControlStatus=READY", updatedDocs, collection);

            long initUpdatedDocs = getMongoCollection(collection).updateMany(initQuery, initUpdate).getModifiedCount();
            logger.info("Updated {} documents in {} with initial qualityControlStatus", initUpdatedDocs, collection);
        }
    }

}
