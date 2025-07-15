package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Arrays;
import java.util.Date;

@Migration(id = "clinicalChanges__task_7353",
        description = "Clinical changes - TASK-7353", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250715)
public class ClinicalChangesTask7353 extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                Filters.exists("report._date", false),
                Projections.include("id", "report"),
                (document, bulk) -> {
                    Document report = document.get("report", Document.class);
                    if (report == null) {
                        return;
                    }
                    String stringDate = report.getString("date");
                    if (StringUtils.isEmpty(stringDate)) {
                        return;
                    }
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    Date date = TimeUtils.toDate(stringDate);
                    updateDocument.getSet().put("report._date", date);
                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });

    }

}
