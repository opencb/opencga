package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "improve_sample_quality_control",
        description = "Quality control normalize comments and fields #1826", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        rank = 9)
public class ImproveSampleQualityControl extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // File
        improvementsSample();
    }

    private void improvementsSample() {

        SampleConverter sampleConverter = new SampleConverter();
        migrateCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION,
                new Document(SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key() + ".fileIds", new Document("$exists", false)),
                Projections.include("_id", SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (sampleDoc, bulk) -> {

                    Document qc = sampleDoc.get(SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key(), Document.class);
                    if (qc != null) {
                        List<String> files = qc.getList("files", String.class);
                        logger.info("Sample migration: Found files are {}", files);

                        Sample sample = sampleConverter.convertToDataModelType(sampleDoc);
                        SampleQualityControl fqc = sample.getQualityControl();

                        if (files != null && fqc != null) {
                            if (CollectionUtils.isEmpty(fqc.getFileIds())) {
                                fqc.setFileIds(files);
                            }

                            Document doc = sampleConverter.convertToStorageType(sample);
                            bulk.add(new UpdateOneModel<>(
                                            eq("_id", sampleDoc.get("_id")),
                                            new Document("$set", new Document(SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key(),
                                                    doc.get(SampleDBAdaptor.QueryParams.QUALITY_CONTROL.key())))
                                    )
                            );
                        }
                    }
                }
        );
    }
}
