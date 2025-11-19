package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantFilter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationRuntimeException;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;

import java.util.*;

@Migration(id = "clinical_model_changes_TASK_7645",
        description = "Clinical Analysis data model changes #TASK-7645", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250707)
public class ClinicalAnalysisMigrationTask7645 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.ne("_migrations", "task-7645-v1");
        Bson clinicalProjection = Projections.include("_id", "report", "qualityControl");
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                query,
                clinicalProjection,
                (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    // Delete ClinicalAnalysis.analyst
                    updateDocument.getUnset().add("analyst");

                    // Change ClinicalAnalysis.qualityControl.summary == NEEDS_REVIEW to ClinicalAnalysis.qualityControl.summary = UNKNOWN
                    // Change ClinicalAnalysis.qualityControl.summary == DISCARD to ClinicalAnalysis.qualityControl.summary = CRITICAL
                    Document qualityControl = document.get("qualityControl", Document.class);
                    if (qualityControl != null) {
                        String summary = qualityControl.getString("summary");
                        if ("NEEDS_REVIEW".equals(summary)) {
                            updateDocument.getSet().put("qualityControl.summary", "UNKNOWN");
                        } else if ("DISCARD".equals(summary)) {
                            updateDocument.getSet().put("qualityControl.summary", "CRITICAL");
                        }
                    }

                    // Check report data model
                    Document report = document.get("report", Document.class);
                    if (report != null) {
                        // Copy ClinicalAnalysis.report.files to ClinicalAnalysis.reportedFiles
                        updateDocument.getSet().put("reportedFiles", report.get("files"));

                        // Copy ClinicalAnalysis.report.signedBy and *.signature to ClinicalAnalysis.report.signatures
                        String signedBy = report.get("signedBy", String.class);
                        String signature = report.get("signature", String.class);
                        if (StringUtils.isNotEmpty(signedBy) || StringUtils.isNotEmpty(signature)) {
                            Document signatureDoc = new Document()
                                    .append("signedBy", signedBy)
                                    .append("signature", signature);
                            updateDocument.getSet().put("report.signatures", Collections.singletonList(signatureDoc));
                        }
                    } else {
                        updateDocument.getSet().put("reportedFiles", Collections.emptyList());
                    }

                    // Add migration tag to document
                    updateDocument.getAddToSet().put("_migrations", "task-7645-v1");

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });

        ClinicalAnalysisStudyConfiguration defaultClinicalConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();
        Document defaultClinicalConfigurationDoc = convertToDocument(defaultClinicalConfiguration);

        // Migrate Study configuration
        Bson studyProjection = Projections.include("_id", "fqn", "internal.configuration");
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                        OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION),
                query,
                studyProjection,
                (document, bulk) -> {
                    String studyFqn = document.getString("fqn");
                    try {
                        createMissingDefaultReportTemplateFile(studyFqn);
                    } catch (CatalogException e) {
                        logger.error("Could not create default report template file for study '{}'. Continuing...", studyFqn, e);
                    }

                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

                    Document internal = document.get("internal", Document.class);
                    if (internal == null) {
                        throw new MigrationRuntimeException("Study '" + document.get("fqn") + "' does not have 'internal' field. "
                                + "This migration requires the 'internal' field to be present.");
                    }
                    Document configuration = internal.get("configuration", Document.class);
                    if (configuration == null) {
                        throw new MigrationRuntimeException("Study '" + document.get("fqn") + "' does not have 'internal.configuration'"
                                + " field. This migration requires the 'internal.configuration' field to be present.");
                    }
                    Document clinical = configuration.get("clinical", Document.class);
                    if (clinical == null) {
                        throw new MigrationRuntimeException("Study '" + document.get("fqn") + "' does not have "
                                + "'internal.configuration.clinical' field. This migration requires the 'internal.configuration.clinical'"
                                + " field to be present.");
                    }
                    Document consent = clinical.get("consent", Document.class);
                    if (consent != null) {
                        // Move Study.internal.configuration.clinical.consent.consents to Study.internal.configuration.clinical.consents
                        updateDocument.getSet().put("internal.configuration.clinical.consents",
                                consent.get("consents", Collections.emptyList()));

                        // Remove Study.internal.configuration.clinical.consent
                        updateDocument.getUnset().add("internal.configuration.clinical.consent");
                    }

                    // Add new default Study.internal.configuration.clinical.tiers
                    updateDocument.getSet().put("internal.configuration.clinical.tiers",
                            defaultClinicalConfigurationDoc.get("tiers", Collections.emptyList()));

                    // Add new default Study.internal.configuration.clinical.report
                    updateDocument.getSet().put("internal.configuration.clinical.report",
                            defaultClinicalConfigurationDoc.get("report", Collections.emptyMap()));

                    // Add INCONCLUSIVE status to Study.internal.configuration.clinical.status
                    List<Document> clinicalStatus = clinical.getList("status", Document.class);
                    if (clinicalStatus == null) {
                        updateDocument.getSet().put("internal.configuration.clinical.status",
                                defaultClinicalConfigurationDoc.get("status", Collections.emptyList()));
                    } else {
                        List<Document> allClinicalStatuses = new ArrayList<>(clinicalStatus.size() + 1);
                        allClinicalStatuses.addAll(clinicalStatus);
                        for (ClinicalStatusValue status : defaultClinicalConfiguration.getStatus()) {
                            if (ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE == status.getType()) {
                                Document statusDoc = convertToDocument(status);
                                allClinicalStatuses.add(statusDoc);
                            }
                        }
                        updateDocument.getSet().put("internal.configuration.clinical.status", allClinicalStatuses);
                    }

                    // Add INCONCLUSIVE status to Study.internal.configuration.clinical.interpretation.status
                    Document interpretationDoc = clinical.get("interpretation", Document.class);
                    if (interpretationDoc == null) {
                        updateDocument.getSet().put("internal.configuration.clinical.interpretation",
                                defaultClinicalConfigurationDoc.get("interpretation", Collections.emptyMap()));
                    } else {
                        List<Document> interpretationStatus = interpretationDoc.getList("status", Document.class);
                        if (interpretationStatus == null) {
                            Document interpretation = defaultClinicalConfigurationDoc.get("interpretation", Document.class);
                            updateDocument.getSet().put("internal.configuration.clinical.interpretation.status",
                                    interpretation.get("status", Collections.emptyList()));
                        } else {
                            List<Document> allInterpretationStatuses = new ArrayList<>(interpretationStatus.size() + 1);
                            allInterpretationStatuses.addAll(interpretationStatus);
                            for (ClinicalStatusValue status : defaultClinicalConfiguration.getInterpretation().getStatus()) {
                                if (ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE == status.getType()) {
                                    Document statusDoc = convertToDocument(status);
                                    allInterpretationStatuses.add(statusDoc);
                                }
                            }
                            updateDocument.getSet().put("internal.configuration.clinical.interpretation.status", allInterpretationStatuses);
                        }
                    }

                    // Add migration tag to document
                    updateDocument.getAddToSet().put("_migrations", "task-7645-v1");

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });


        // Migrate Interpretation configuration
        Bson interpretationProjection = Projections.include("_id", "primaryFindings", "secondaryFindings", "stats");
        // Avoid batches because interpretations may be very big
        setBatchSize(1);
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                        OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION,
                        OrganizationMongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION),
                query,
                interpretationProjection,
                (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

                    List<Document> primaryFindings = document.getList("primaryFindings", Document.class);
                    List<Document> secondaryFindings = document.getList("secondaryFindings", Document.class);

                    migrateFindings(primaryFindings, updateDocument, "primaryFindings");
                    migrateFindings(secondaryFindings, updateDocument, "secondaryFindings");

                    Document stats = document.get("stats", Document.class);
                    migrateStats(stats, updateDocument, "primaryFindings");
                    migrateStats(stats, updateDocument, "secondaryFindings");

                    // Add migration tag to document
                    updateDocument.getAddToSet().put("_migrations", "task-7645-v1");

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }

    private void migrateStats(Document stats, MongoDBAdaptor.UpdateDocument updateDocument, String key) {
        if (stats == null) {
            return;
        }
        Document keyStats = stats.get(key, Document.class);
        if (keyStats == null) {
            return;
        }
        Document statusCount = keyStats.get("statusCount", Document.class);
        if (statusCount != null) {
            statusCount.put("CANDIDATE", 0);
            statusCount.put("UNDER_CONSIDERATION", statusCount.get("REVIEW_REQUESTED"));
            statusCount.remove("REVIEW_REQUESTED");
            updateDocument.getSet().put("stats." + key + ".statusCount", statusCount);
        }
    }

    private void createMissingDefaultReportTemplateFile(String studyFqn) throws CatalogException {
        // If the default resources template folder does not exist, create the default files
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), ParamConstants.RESOURCES_REPORT_TEMPLATE_FOLDER + "/");
        if (catalogManager.getFileManager().count(studyFqn, query, token).getNumMatches() == 0) {
            catalogManager.getStudyManager().createDefaultFiles(studyFqn, token);
        }
    }

    private void migrateFindings(List<Document> findings, MongoDBAdaptor.UpdateDocument updateDocument, String key) {
        if (CollectionUtils.isNotEmpty(findings)) {
            ClinicalVariantFilter defaultFilter = new ClinicalVariantFilter(new HashMap<>(), "", "");
            Document defaultFilterDoc = convertToDocument(defaultFilter);
            // Move Interpretation.xxxxFindings.filters to Interpretation.xxxxxFindings.filter.query
            for (Document finding : findings) {
                Document filters = finding.get("filters", Document.class);
                if (filters != null) {
                    Document docCopy = new Document(defaultFilterDoc);
                    docCopy.put("query", filters);
                    finding.put("filter", docCopy);
                    finding.remove("filters");
                } else {
                    finding.put("filter", defaultFilterDoc);
                }

                List<Document> references = finding.getList("references", Document.class);
                // Move Interpretation.xxxxFindings.references.name to Interpretation.xxxxFindings.references.title
                if (CollectionUtils.isNotEmpty(references)) {
                    for (Document reference : references) {
                        String name = reference.getString("name");
                        if (StringUtils.isNotEmpty(name)) {
                            reference.put("title", name);
                            reference.remove("name");
                        }
                    }
                }

                // Rename status REVIEW_REQUESTED to UNDER_CONSIDERATION
                if ("REVIEW_REQUESTED".equals(finding.getString("status"))) {
                    finding.put("status", "UNDER_CONSIDERATION");
                }
            }

            updateDocument.getSet().put(key, findings);
        }
    }
}
