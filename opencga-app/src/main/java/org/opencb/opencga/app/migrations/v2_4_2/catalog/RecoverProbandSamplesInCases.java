package org.opencb.opencga.app.migrations.v2_4_2.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;

@Migration(id = "recover_proband_samples_in_cases_TASK-1470",
        description = "Recover lost samples in clinical collection #TASK-1470", version = "2.4.2",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,

        date = 20220725,
        patch = 2)

public class RecoverProbandSamplesInCases extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> audit = getMongoCollection(MongoDBAdaptorFactory.AUDIT_COLLECTION);
        ClinicalAnalysisConverter converter = new ClinicalAnalysisConverter();

        queryMongo(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,

                new Document("$or", Arrays.asList(
                        new Document("proband.samples", null),
                        new Document("proband.samples", Collections.emptyList())
                )),
                Projections.include("uid", "id", "uuid", "studyUid", "proband", "type"), (doc) -> {
                    ClinicalAnalysis currentCase = converter.convertToDataModelType(doc);
                    logger.info("Trying to recover Clinical Analysis [id: {}, uid: {}, uuid: {}]", currentCase.getId(),
                            currentCase.getUid(), currentCase.getUuid());

                    if (currentCase.getProband() == null) {
                        logger.warn("Proband not defined in ClinicalAnalysis '{}'. Skipping...", currentCase.getId());
                        return;
                    }

                    ClinicalAnalysis clinicalAnalysis = findUpdatedCaseInAudit(currentCase.getUuid(), currentCase.getProband().getId(),
                            audit, converter);
                    if (clinicalAnalysis == null) {
                        clinicalAnalysis = findCreatedCaseInAudit(currentCase.getUuid(), currentCase.getProband().getId(), audit,
                                converter);
                    }
                    if (clinicalAnalysis == null) {
                        recoverWithoutAudit(currentCase);
                    } else {
                        recoverFromAudit(currentCase, clinicalAnalysis);
                    }
                });
    }

    private void recoverWithoutAudit(ClinicalAnalysis currentCase) {
        ClinicalAnalysis.Type type = currentCase.getType();

        // The proband from the audit matches the one used in the case currently :)
        Individual proband;
        try {
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.UID.key(), currentCase.getProband().getUid())
                    .append(IndividualDBAdaptor.QueryParams.VERSION.key(), currentCase.getProband().getVersion());
            proband = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(query,
                            new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.ID.key(),
                                    IndividualDBAdaptor.QueryParams.VERSION.key(), IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                                    IndividualDBAdaptor.QueryParams.STUDY_UID.key())))
                    .first();
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }

        List<Sample> sampleList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(proband.getSamples())) {
            if (type == ClinicalAnalysis.Type.CANCER) {
                if (proband.getSamples().size() == 2) {
                    sampleList = proband.getSamples();
                } else if (proband.getSamples().size() > 2){
                    sampleList = proband.getSamples().subList(0, 2);
                    logger.warn("Proband '{}' from study uid {} has more than 2 samples. Considering the first 2.", proband.getId(),
                            proband.getStudyUid());
                } else {
                    sampleList = proband.getSamples();
                    logger.warn("Proband '{}' from study uid {} has {} samples. Considering those.", proband.getId(), proband.getStudyUid(),
                            proband.getSamples().size());
                }
            } else {
                if (proband.getSamples().size() == 1) {
                    sampleList = proband.getSamples();
                } else {
                    sampleList = proband.getSamples().subList(0, 1);
                    logger.warn("Proband '{}' from study uid {} has more than 1 sample. Considering the first sample.", proband.getId(),
                            proband.getStudyUid());
                }
            }
        } else {
            logger.warn("Could not find samples for proband '{}' of study uid {}. Setting an empty list.", proband.getId(),
                    proband.getStudyUid());
        }

        proband.setSamples(sampleList);
        ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), proband);
        try {
            dbAdaptorFactory.getClinicalAnalysisDBAdaptor().update(currentCase.getUid(), params, null, QueryOptions.empty());
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }
    }

    private void recoverFromAudit(ClinicalAnalysis currentCase, ClinicalAnalysis clinicalAnalysis) {
        if (clinicalAnalysis.getProband() == null) {
            throw new RuntimeException("Proband from ClinicalAnalysis '" + currentCase.getId()
                    +  "' could not be found in the audit collection.");
        }
        if (CollectionUtils.isEmpty(clinicalAnalysis.getProband().getSamples())) {
            throw new RuntimeException("List of samples from proband of ClinicalAnalysis '" + currentCase.getId()
                    + "' could not be found in the audit collection.");
        }

        // The proband from the audit matches the one used in the case currently :)
        Individual proband;
        try {
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.UID.key(), currentCase.getProband().getUid())
                    .append(IndividualDBAdaptor.QueryParams.VERSION.key(), currentCase.getProband().getVersion());
            proband = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(query,
                            new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.ID.key(),
                                    IndividualDBAdaptor.QueryParams.VERSION.key(), IndividualDBAdaptor.QueryParams.SAMPLES.key())))
                    .first();
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }
        List<Sample> sampleList = new ArrayList<>(clinicalAnalysis.getProband().getSamples().size());

        Map<String, Sample> sampleMap = new HashMap<>();
        for (Sample sample : proband.getSamples()) {
            sampleMap.put(sample.getId(), sample);
        }
        for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
            if (sampleMap.containsKey(sample.getId())) {
                sampleList.add(sample);
            } else {
                logger.warn("Sample '{}' is no longer associated to the individual '{}'", sample.getId(), proband.getId());
            }
        }

        if (sampleList.isEmpty()) {
            logger.warn("The list of samples associated to the proband '{}' is empty after processing", proband.getId());
        }
        proband.setSamples(sampleList);

        ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), proband);
        try {
            dbAdaptorFactory.getClinicalAnalysisDBAdaptor().update(currentCase.getUid(), params, null, QueryOptions.empty());
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }
    }

    private ClinicalAnalysis findCreatedCaseInAudit(String uuid, String probandId, MongoCollection<Document> audit,
                                                    ClinicalAnalysisConverter converter) {
        // Fetch case from Audit collection
        try (MongoCursor<Document> iterator = audit.find(
                Filters.and(
                        Filters.eq("resource", Enums.Resource.CLINICAL_ANALYSIS.name()),
                        Filters.eq("resourceUuid", uuid),
                        Filters.eq("action", Enums.Action.CREATE.name())
                )
        ).iterator()) {
            if (iterator.hasNext()) {
                Document auditDoc = iterator.next();
                Document auditParams = auditDoc.get("params", Document.class);
                if (auditParams != null) {
                    Document caseDoc = auditParams.get("clinicalAnalysis", Document.class);
                    if (caseDoc != null) {
                        Document caseProbandOnly = new Document("proband", caseDoc.get("proband"));
                        converter.validateProbandToUpdate(caseProbandOnly);
                        ClinicalAnalysis clinicalAnalysis = converter.convertToDataModelType(caseProbandOnly);
                        if (!probandId.equals(clinicalAnalysis.getProband().getId())) {
                            logger.error("Proband '{}' from case '{}' does not match the proband '{}' used on create.", probandId, uuid,
                                    clinicalAnalysis.getProband().getId());
                        } else {
                            logger.debug("Found case in Audit create");
                            return clinicalAnalysis;
                        }
                    }
                }
            }
        }

        return null;
    }

    private ClinicalAnalysis findUpdatedCaseInAudit(String uuid, String probandId, MongoCollection<Document> audit,
                                                    ClinicalAnalysisConverter converter) {
        // Fetch case from Audit collection
        try (MongoCursor<Document> iterator = audit.find(
                Filters.and(
                        Filters.eq("resource", Enums.Resource.CLINICAL_ANALYSIS.name()),
                        Filters.eq("resourceUuid", uuid),
                        Filters.eq("action", Enums.Action.UPDATE.name())
                )
        ).sort(Sorts.descending("date")).iterator()) {
            while (iterator.hasNext()) {
                Document auditDoc = iterator.next();
                Document auditParams = auditDoc.get("params", Document.class);
                if (auditParams != null) {
                    Document caseDoc = auditParams.get("updateParams", Document.class);
                    if (caseDoc != null) {
                        Document caseProbandOnly = new Document("proband", caseDoc.get("proband"));
                        converter.validateProbandToUpdate(caseProbandOnly);
                        ClinicalAnalysis clinicalAnalysis = converter.convertToDataModelType(caseProbandOnly);
                        if (clinicalAnalysis != null && clinicalAnalysis.getProband() != null
                                && probandId.equals(clinicalAnalysis.getProband().getId())
                                && CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getSamples())) {
                            logger.debug("Found case in Audit update");
                            return clinicalAnalysis;
                        }
                    }
                }
            }
        }

        return null;
    }


}
