package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "rename_individual_quality_control_fields",
        description = "Rename IndividualQualityControl fields #1844", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211119)
public class RenameIndividualQualityControlFields extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                new Document(),
                Projections.include("_id", IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (doc, bulk) -> {
                    Document qc = doc.get(IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key(), Document.class);
                    if (qc != null) {
                        List<String> fileIds = qc.getList("fileIds", String.class);
                        qc.remove("fileIds");
                        qc.put("files", fileIds != null ? fileIds : Collections.emptyList());

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", doc.get("_id")),
                                new Document("$set", new Document(IndividualDBAdaptor.QueryParams.QUALITY_CONTROL.key(), qc)))
                        );
                    }
                }
        );
    }
}
