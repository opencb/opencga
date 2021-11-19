package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "rename_file_quality_control_fields",
        description = "Rename FileQualityControl fields #1844", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211119)
public class RenameFileQualityControlFields extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(MongoDBAdaptorFactory.FILE_COLLECTION,
                new Document(),
                Projections.include("_id", FileDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (doc, bulk) -> {
                    Document qc = doc.get(FileDBAdaptor.QueryParams.QUALITY_CONTROL.key(), Document.class);
                    if (qc != null) {
                        qc.put("files", Collections.emptyList());

                        Document alignment = qc.get("alignment", Document.class);
                        if (alignment != null) {
                            Document fastQcMetrics = alignment.get("fastQcMetrics", Document.class);
                            if (fastQcMetrics != null) {
                                fastQcMetrics.put("files", fastQcMetrics.get("images"));
                                fastQcMetrics.remove("images");
                            }

                            Document samtoolsStats = alignment.get("samtoolsStats", Document.class);
                            if (samtoolsStats != null) {
                                samtoolsStats.put("files", samtoolsStats.get("images"));
                                samtoolsStats.remove("images");
                            }
                        }

                        Document variant = qc.get("variant", Document.class);
                        if (variant != null) {
                            Document ascat = variant.get("ascatMetrics", Document.class);
                            if (ascat != null) {
                                ascat.put("files", ascat.get("images"));
                                ascat.remove("images");
                            }
                        }

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", doc.get("_id")),
                                new Document("$set", new Document(FileDBAdaptor.QueryParams.QUALITY_CONTROL.key(), qc)))
                        );
                    }
                }
        );
    }
}
