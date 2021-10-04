package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisQualityControl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "improve_quality_control",
        description = "Quality control normalize comments and fields #1826", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        rank = 10)
public class ImproveClinicalAnalysisQualityControl extends MigrationTool {

    @Override
    protected void run() throws Exception {

        improvementsClinicalAnalysis();
    }

    private void improvementsClinicalAnalysis() {
        migrateCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                new Document(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key() + ".comments", new Document("$exists", false)),
                Projections.include("_id", ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (clinicalAnalysisDoc, bulk) -> {
                    if (clinicalAnalysisDoc != null) {
                        Document qc = clinicalAnalysisDoc.get(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key(), Document.class);
                        if (qc != null) {
                            String summary = qc.get("summary", String.class);
                            String text = qc.get("comment", String.class);
                            String user = qc.get("user", String.class);
                            Date date = new Date(qc.get("date", Long.class));

//                            if EXCELLENT, GOOD, NORMAL, BAD, UNKNOWN
                            String newSummary = null;
                            switch (summary) {
                                case "EXCELLENT":
                                    newSummary = ClinicalAnalysisQualityControl.QualityControlSummary.HIGH.name();
                                    break;
                                case "GOOD":
                                    newSummary = ClinicalAnalysisQualityControl.QualityControlSummary.MEDIUM.name();
                                    break;
                                case "NORMAL":
                                    newSummary = ClinicalAnalysisQualityControl.QualityControlSummary.MEDIUM.name();
                                    break;
                                case "BAD":
                                    newSummary = ClinicalAnalysisQualityControl.QualityControlSummary.LOW.name();
                                    break;
                                case "UNKNOWN":
                                    newSummary = ClinicalAnalysisQualityControl.QualityControlSummary.UNKNOWN.name();
                                    break;
                                default:
                                    break;
                            }

                            List<Document> comments = new ArrayList<>();
                            if (StringUtils.isNotEmpty(text) && StringUtils.isNotEmpty(user)) {
                                Document comment = new Document()
                                        .append("author", user)
                                        .append("message", text)
                                        .append("tags", Collections.emptyList())
                                        .append("date", date != null ? TimeUtils.getTime(date) : TimeUtils.getTime());
                                comments.add(comment);
                            }

                            Document finalQC = new Document()
                                    .append("comments", comments)
                                    .append("summary", newSummary);
                            bulk.add(new UpdateOneModel<>(
                                    eq("_id", clinicalAnalysisDoc.get("_id")),
                                    new Document("$set",
                                            new Document(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key(), finalQC))));
                        }
                    }
                }
        );
    }
}
