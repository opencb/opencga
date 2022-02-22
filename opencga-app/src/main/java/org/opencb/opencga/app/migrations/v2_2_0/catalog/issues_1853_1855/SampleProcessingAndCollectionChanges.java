package org.opencb.opencga.app.migrations.v2_2_0.catalog.issues_1853_1855;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.common.ExternalSource;
import org.opencb.opencga.core.models.sample.SampleCollection;
import org.opencb.opencga.core.models.sample.SampleProcessing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "sample_source_treatments_#1854",
        description = "Sample source, treatments, processing and collection changes #1854", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211201, patch = 2)
public class SampleProcessingAndCollectionChanges extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION,
                new Document("collection.from", new Document("$exists", false)),
                Projections.include("_id", "collection", "processing"),
                (doc, bulk) -> {
                    Document processing = doc.get("processing", Document.class);
                    if (processing != null) {
                        String product = processing.getString("product");
                        if (StringUtils.isNotEmpty(product)) {
                            OntologyTermAnnotation ontologyTermAnnotation =
                                    new OntologyTermAnnotation(product, "", "", "", "", Collections.emptyMap());
                            Document ontologyDoc = convertToDocument(ontologyTermAnnotation);
                            processing.put("product", Collections.singletonList(ontologyDoc));
                        } else {
                            processing.put("product", Collections.emptyList());
                        }
                    } else {
                        SampleProcessing sampleProcessing = SampleProcessing.init();
                        processing = convertToDocument(sampleProcessing);
                    }

                    Document collection = doc.get("collection", Document.class);
                    if (collection != null) {
                        List<OntologyTermAnnotation> ontologyTermAnnotationList = new ArrayList<>();
                        String tissue = collection.getString("tissue");
                        processOntologyTermAnnotationField(tissue, "tissue", ontologyTermAnnotationList);
                        String organ = collection.getString("organ");
                        processOntologyTermAnnotationField(organ, "organ", ontologyTermAnnotationList);

                        List<Document> ontologyDocList = convertToDocument(ontologyTermAnnotationList);
                        collection.put("from", ontologyDocList);
                        collection.put("type", "");
                        collection.remove("tissue");
                        collection.remove("organ");
                    } else {
                        SampleCollection sampleCollection = SampleCollection.init();
                        collection = convertToDocument(sampleCollection);
                    }

                    ExternalSource source = ExternalSource.init();
                    Document sourceDoc = convertToDocument(source);

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", doc.get("_id")),
                            new Document("$set", new Document()
                                    .append("source", sourceDoc)
                                    .append("processing", processing)
                                    .append("collection", collection)
                                    .append("treatments", Collections.emptyList())
                            ))
                    );
                }
        );

        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        collection.dropIndex(new Document().append("processing.product", 1).append("studyUid", 1));
        collection.dropIndex(new Document().append("collection.tissue", 1).append("studyUid", 1));
        collection.dropIndex(new Document().append("collection.organ", 1).append("studyUid", 1));
        collection.createIndex(new Document().append("processing.product.id", 1).append("studyUid", 1), new IndexOptions().background(true));
        collection.createIndex(new Document().append("collection.from.id", 1).append("studyUid", 1), new IndexOptions().background(true));
        collection.createIndex(new Document().append("collection.type", 1).append("studyUid", 1), new IndexOptions().background(true));

        // Patch 2 - Remove array from processing.product
        migrateCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION,
                new Document("processing.product", new Document("$exists", true)),
                Projections.include("_id", "processing"),
                (doc, bulk) -> {
                    Document processing = doc.get("processing", Document.class);
                    if (processing != null) {
                        List<Document> product = processing.getList("product", Document.class);
                        if (CollectionUtils.isEmpty(product)) {
                            processing.put("product", new Document());
                        } else {
                            processing.put("product", product.get(0));
                        }
                    } else {
                        SampleProcessing sampleProcessing = SampleProcessing.init();
                        processing = convertToDocument(sampleProcessing);
                    }

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", doc.get("_id")),
                                    new Document("$set", new Document("processing", processing))
                            )
                    );
                }
        );
    }

    private void processOntologyTermAnnotationField(String field, String source, List<OntologyTermAnnotation> ontologyTermAnnotationList) {
        if (StringUtils.isNotEmpty(field)) {
            ontologyTermAnnotationList.add(new OntologyTermAnnotation(field, "", "", source, "", Collections.emptyMap()));
        }
    }
}
