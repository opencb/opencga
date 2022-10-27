package org.opencb.opencga.app.migrations.v2_4_10.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_missing_endDate_on_finished_jobs" ,
        description = "Add missing end date on finished jobs",
        version = "2.4.10",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20221026
)
public class AddMissingEndDateOnFinishedJobs extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(Arrays.asList(MongoDBAdaptorFactory.JOB_COLLECTION, MongoDBAdaptorFactory.DELETED_JOB_COLLECTION),
                Filters.and(Arrays.asList(
                        Filters.in("internal.status.id", Arrays.asList("DONE", "ERROR", "ABORTED", "UNKNOWN", "READY")),
                        Filters.or(Arrays.asList(
                                Filters.exists("execution.end", false),
                                Filters.eq("execution.end", null))
                        )
                )),
                Projections.include(Arrays.asList("execution", "_modificationDate")),
                (document, bulk) -> {
                    Document execution = document.get("execution", Document.class);
                    if (execution != null) {
                        // Add end date
                        Object modificationDate = document.get("_modificationDate");
                        if (modificationDate == null) {
                            modificationDate = TimeUtils.getDate();
                        }
                        execution.put("end", modificationDate);

                        // Add event
                        List<Document> events = execution.getList("events", Document.class);
                        if (events == null) {
                            events = new ArrayList<>();
                        } else {
                            events = new ArrayList<>(events);
                        }
                        events.add(new Document()
                                .append("type", "WARNING")
                                .append("id", "missing-execution-end-date")
                                .append("message", "Missing execution field 'end'. End date set as part of migration fix."));
                        execution.put("events", events);

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", document.get("_id")),
                                new Document("$set", new Document("execution", execution)))
                        );
                    }
                });
    }

}
