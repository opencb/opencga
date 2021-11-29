package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.models.sample.SampleInternalVariant;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_sample_internal_variant_1851",
        description = "Add new SampleInternalVariant #1851", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211129)
public class AddSampleInternalVariant_1851 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION,
                new Document("internal.variant", new Document("$exists", false)),
                Projections.include("_id", "internal"),
                (doc, bulk) -> {
                    Document internal = doc.get("internal", Document.class);

                    if (internal != null) {
                        SampleInternalVariant sampleInternalVariant = SampleInternalVariant.init();
                        Document sampleInternalVariantDoc = convertToDocument(sampleInternalVariant);
                        internal.put("variant", sampleInternalVariantDoc);
                    } else {
                        SampleInternal sampleInternal = SampleInternal.init();
                        Document sampleInternalDoc = convertToDocument(sampleInternal);
                        doc.put("internal", sampleInternalDoc);
                    }

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", doc.get("_id")),
                                    new Document("$set", new Document("internal", doc.get("internal")))
                            )
                    );
                });
    }

}
