package org.opencb.opencga.app.migrations.v2_12_0.catalog;

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

@Migration(id = "complete_clinical_report_data_model" ,
        description = "Complete Clinical Report data model #TASK-5198",
        version = "2.12.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20231128
)
public class CompleteClinicalReportDataModelMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(
                Arrays.asList(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                Filters.exists("analyst"),
                Projections.include(Arrays.asList("analyst", "report")),
                (document, bulk) -> {
                    Document analyst = document.get("analyst", Document.class);
                    analyst.remove("assignedBy");
                    analyst.remove("date");

                    Document report = document.get("report", Document.class);
                    if (report != null) {
                        report.put("comments", Collections.emptyList());
                        report.put("supportingEvidences", Collections.emptyList());
                        report.put("files", Collections.emptyList());
                    }

                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    updateDocument.getSet().put("report", report);
                    updateDocument.getSet().put("request", new Document());
                    updateDocument.getSet().put("responsible", new Document()
                            .append("id", analyst.get("id"))
                            .append("name", analyst.get("name"))
                            .append("email", analyst.get("email"))
                    );
                    updateDocument.getSet().put("analysts", Collections.singletonList(analyst));
                    updateDocument.getUnset().add("analyst");

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", document.get("_id")),
                                    updateDocument.toFinalUpdateDocument()
                            )
                    );
                });
    }
}
