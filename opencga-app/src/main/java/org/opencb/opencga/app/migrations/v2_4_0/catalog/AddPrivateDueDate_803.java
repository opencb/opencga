package org.opencb.opencga.app.migrations.v2_4_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_private_dueDate_TASK-803",
        description = "Add private _dueDate to ClinicalAnalysis #TASK-803", version = "2.4.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220530)
public class AddPrivateDueDate_803 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        createIndex(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, new Document()
                .append("_dueDate", 1)
                .append("studyUid", 1)
        );

        migrateCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                Filters.exists("_dueDate", false), Projections.include("dueDate"), (doc, bulk) -> {
                    Document update = new Document();
                    String dueDate = doc.getString("dueDate");
                    if (StringUtils.isEmpty(dueDate)) {
                        dueDate = TimeUtils.getTime(TimeUtils.add1MonthtoDate(TimeUtils.getDate()));
                        update.put("dueDate", dueDate);
                    }
                    update.put("_dueDate", TimeUtils.toDate(dueDate));

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", doc.get("_id")),
                                    new Document("$set", update)
                            )
                    );
                });

        dropIndex(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, new Document()
                .append("dueDate", 1)
                .append("studyUid", 1));
    }

}
