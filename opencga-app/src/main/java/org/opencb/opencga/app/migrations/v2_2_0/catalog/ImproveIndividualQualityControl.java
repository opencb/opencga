package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;
import org.opencb.biodata.models.clinical.qc.SampleRelatednessReport;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;

import java.util.Collections;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "improve_individual_quality_control",
        description = "Quality control normalize comments and fields #1826", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211001)
public class ImproveIndividualQualityControl extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // File

        improvementsIndividual();
    }

    private void improvementsIndividual() {
        ObjectMapper defaultObjectMapper = JacksonUtils.getDefaultObjectMapper();

        IndividualConverter individualConverter = new IndividualConverter();
        migrateCollection(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                new Document(IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key() + ".inferredSexReports.sampleId",
                        new Document("$exists", false)),
                Projections.include("_id", IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (individualDoc, bulk) -> {
                    Individual individual = individualConverter.convertToDataModelType(individualDoc);
                    IndividualQualityControl fqc = individual.getQualityControl();

                    if (fqc != null) {
                        Document qc = individualDoc.get(IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key(), Document.class);
                        if (qc != null) {
                            if (fqc.getInferredSexReports() != null) {
                                String sampleId = qc.get("sampleId", String.class);
                                for (InferredSexReport inferredSexReport : fqc.getInferredSexReports()) {
                                    inferredSexReport.setSampleId(sampleId);
                                }
                            }
                            if (CollectionUtils.isEmpty(fqc.getMendelianErrorReports())) {
                                Document mendelianErrorReportDoc = qc.get("mendelianErrorReport", Document.class);
                                if (mendelianErrorReportDoc != null) {
                                    try {
                                        String mString = defaultObjectMapper.writeValueAsString(mendelianErrorReportDoc);
                                        MendelianErrorReport mendelianErrorReport = defaultObjectMapper.readValue(mString,
                                                MendelianErrorReport.class);
                                        if (mendelianErrorReport.getNumErrors() == 0 && CollectionUtils.isEmpty(mendelianErrorReport.getSampleAggregation())) {
                                            // Default values of Mendelian Error Report
                                            fqc.setMendelianErrorReports(Collections.emptyList());
                                        } else {
                                            fqc.setMendelianErrorReports(Collections.singletonList(mendelianErrorReport));
                                        }
                                    } catch (JsonProcessingException e) {
                                        logger.error("Could not parse Mendelian Error Report properly");
                                    }
                                } else {
                                    fqc.setMendelianErrorReports(Collections.emptyList());
                                }
                            }
                        }
                        if (fqc.getSampleRelatednessReport() == null) {
                            fqc.setSampleRelatednessReport(new SampleRelatednessReport());
                        }

                        Document doc = individualConverter.convertToStorageType(individual);
                        bulk.add(new UpdateOneModel<>(
                                        eq("_id", individualDoc.get("_id")),
                                        new Document("$set", new Document(IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key(),
                                                doc.get(IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key())))
                                )
                        );
                    }
                }
        );
    }
}
