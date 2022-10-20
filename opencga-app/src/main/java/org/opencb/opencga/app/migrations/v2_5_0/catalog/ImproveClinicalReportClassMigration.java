package org.opencb.opencga.app.migrations.v2_5_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "improve_clinical_report-2214",
        description = "Improve ClinicalReport class #TASK-2214", version = "2.5.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20221017)
public class ImproveClinicalReportClassMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(Arrays.asList(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                        MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                Filters.and(
                        Filters.exists("report"),
                        Filters.ne("report", null),
                        Filters.exists("report.notes", false)
                ),
                Projections.include("_id", "id", "studyUid", "report"),
                (doc, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    Document report = null;
                    try {
                        report = doc.get("report", Document.class);
                    } catch (Exception e) {
                        logger.warn("report not found in Case '{}' of study uid {}", doc.getString("id"), doc.get("studyUid"));
                    }

                    if (report != null) {
                        // Remove deprecated fields
                        report.remove("signedBy");
                        report.remove("signature");
                        report.remove("date");
                        report.put("methodology", "");
                        report.put("result", "");
                        report.put("notes", "");
                        report.put("disclaimer", "");
                        report.put("annexes", Collections.emptyList());
                        updateDocument.getSet().put("report", report);

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", doc.get("_id")),
                                updateDocument.toFinalUpdateDocument())
                        );
                    }
                }
        );
    }
}
