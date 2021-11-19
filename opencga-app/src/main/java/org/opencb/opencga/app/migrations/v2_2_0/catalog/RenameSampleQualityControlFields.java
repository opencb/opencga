package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "rename_sample_quality_control_fields",
        description = "Rename SampleQualityControl fields #1844", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211119)
public class RenameSampleQualityControlFields extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION,
                new Document(),
                Projections.include("_id", SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (sampleDoc, bulk) -> {

                    Document qc = sampleDoc.get(SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key(), Document.class);
                    if (qc != null) {
                        List<String> files = qc.getList("files", String.class);
                        List<String> fileIds = qc.getList("fileIds", String.class);
                        qc.remove("fileIds");

                        if (CollectionUtils.isEmpty(files) && CollectionUtils.isNotEmpty(fileIds)) {
                            qc.put("files", fileIds);
                        } else {
                            qc.put("files", Collections.emptyList());
                        }

                        Document variant = qc.get("variant", Document.class);
                        if (variant != null) {
                            // Change List<GenomePlot> genomePlots for GenomePlot genomePlot;
                            List<Document> plots = variant.getList("genomePlots", Document.class);
                            variant.remove("genomePlots");
                            if (CollectionUtils.isNotEmpty(plots)) {
                                variant.put("genomePlot", plots.get(0));
                            }
                            
                            // Add files
                            variant.put("files", Collections.emptyList());

                            // Check signatures
                            List<Document> signatures = variant.getList("signatures", Document.class);
                            if (signatures != null) {
                                for (Document signature : signatures) {
                                    // Check SignatureFitting
                                    Document fitting = signature.get("fitting", Document.class);

                                    // Rename image for file
                                    String image = fitting.get("image", String.class);
                                    fitting.remove("image");
                                    fitting.put("file", image != null ? image : "");
                                }
                            }
                        }

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", sampleDoc.get("_id")),
                                new Document("$set", new Document(SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key(), qc)))
                        );
                    }
                }
        );
    }

}
