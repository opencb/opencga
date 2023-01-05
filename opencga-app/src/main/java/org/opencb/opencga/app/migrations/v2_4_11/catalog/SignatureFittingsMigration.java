package org.opencb.opencga.app.migrations.v2_4_11.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "signature_fittings" ,
        description = "Replace fitting for fittings in Signature",
        version = "2.4.11",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20221109
)
public class SignatureFittingsMigration extends MigrationTool {
    @Override
    protected void run() throws Exception {
        migrateCollection(Arrays.asList(MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                        MongoDBAdaptorFactory.DELETED_SAMPLE_COLLECTION),
                Filters.exists("qualityControl.variant.signatures.fitting"),
                Projections.include(Collections.singletonList("qualityControl.variant.signatures")),
                (document, bulk) -> {
                    Document qc = document.get("qualityControl", Document.class);
                    if (qc == null) {
                        return;
                    }
                    Document variant = qc.get("variant", Document.class);
                    if (variant == null) {
                        return;
                    }
                    List<Document> signatures = variant.getList("signatures", Document.class);
                    if (CollectionUtils.isNotEmpty(signatures)) {
                        for (Document signature : signatures) {
                            if (signature != null) {
                                Document fitting = signature.get("fitting", Document.class);
                                if (fitting != null) {
                                    fitting.put("id", "fitting-1");
                                    signature.put("fittings", Collections.singletonList(fitting));
                                    signature.remove("fitting");
                                }
                            }
                        }

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", document.get("_id")),
                                new Document("$set", new Document("qualityControl.variant", signatures)))
                        );
                    }
                });
    }
}
