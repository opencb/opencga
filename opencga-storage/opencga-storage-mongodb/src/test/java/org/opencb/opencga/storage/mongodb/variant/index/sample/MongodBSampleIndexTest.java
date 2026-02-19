package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexTest;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleIndexQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@Category(LongTests.class)
public class MongodBSampleIndexTest extends SampleIndexTest implements MongoDBVariantStorageTest {

    @Override
    public void load() throws Exception {
        super.load();

        URI outdir = newOutputUri();
        for (String study : studies) {
            System.out.println("=== Study: " + study + " ===");
            printVariantsContent(study, outdir);
            printSampleIndexContents(study, outdir);
        }
    }


    public void printVariantsContent(String study, URI outdir) throws Exception {
        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) super.dbAdaptor;

        Path output = Paths.get(outdir.resolve(study + ".variants.json"));
        try (OutputStream os = Files.newOutputStream(output);
             PrintStream out = new PrintStream(os)) {

            try (VariantDBIterator it = dbAdaptor.iterator(new VariantQuery().study(study)
                    .includeSampleAll()
                    .includeSampleId(true)
                    .includeFileAll(), new QueryOptions())) {
                while (it.hasNext()) {
                    Variant variant = it.next();
                    out.println(variant.toJson());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
    public void printSampleIndexContents(String study, URI outdir) throws Exception {
        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) super.dbAdaptor;
        int studyId = dbAdaptor.getMetadataManager().getStudyId(study);

        for (Integer sampleId : metadataManager.getIndexedSamples(studyId)) {
            String sampleName = metadataManager.getSampleName(studyId, sampleId);

            SampleIndexQuery query = sampleIndexDBAdaptor.parseSampleIndexQuery(new VariantQuery().study(study).sample(sampleName));
            String collectionName = ((MongoDBSampleIndexDBAdaptor) sampleIndexDBAdaptor)
                    .getSampleIndexCollectionName(studyId, query.getSchema().getVersion());

            Path output = Paths.get(outdir.resolve(collectionName + "." + sampleName + ".detailed.txt"));
            System.out.println("output = " + output);
            try (OutputStream os = Files.newOutputStream(output);
                 PrintStream out = new PrintStream(os)) {

                out.println();
                out.println();
                out.println();
                out.println("SAMPLE: " + sampleName + " (id=" + sampleId + ")");

                int variantIds = 0;
                try (CloseableIterator<SampleIndexVariant> it = sampleIndexDBAdaptor.indexVariantIterator(query)) {
                    while (it.hasNext()) {
                        SampleIndexVariant entry = it.next();
                        variantIds++;
                        out.println("_______________________");
                        out.println("#" + variantIds);
                        out.println(entry.toString(query.getSchema()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Test
    public void regenerateSampleIndex() throws Exception {
        MongoDBSampleIndexDBAdaptor mongoAdaptor = (MongoDBSampleIndexDBAdaptor) sampleIndexDBAdaptor;
        ObjectMap options = new ObjectMap(ParamConstants.OVERWRITE, true);

        for (String study : studies) {
            int studyId = metadataManager.getStudyId(study);
            int version = getSampleIndexVersion(studyId);
            String collectionName = mongoAdaptor.getSampleIndexCollectionName(studyId, version);
            mongoAdaptor.createCollectionIfNeeded(studyId, version);

            // Snapshot current collection content
            Map<String, Document> expected = snapshotCollection(collectionName);
            System.out.println("Study " + study + ": snapshotted " + expected.size() + " documents from " + collectionName);

            // Drop the collection to force a clean rebuild
            dropCollection(collectionName);

            // Rebuild sample genotype index
            variantStorageEngine.getSampleIndexDBAdaptor().newSampleGenotypeIndexer(variantStorageEngine)
                    .buildSampleIndex(study, Collections.singletonList(VariantQueryUtils.ALL), options, true, sampleIndexDBAdaptor.getSchemaFactory().getSchemaForVersion(studyId, version));
//            variantStorageEngine.sampleIndex(study, Collections.singletonList(VariantQueryUtils.ALL), options);

            // Rebuild sample annotation index
//            variantStorageEngine.sampleIndexAnnotate(study, Collections.singletonList(VariantQueryUtils.ALL), options);
            variantStorageEngine.getSampleIndexDBAdaptor().newSampleAnnotationIndexer(variantStorageEngine)
                    .updateSampleAnnotation(studyId, metadataManager.getIndexedSamples(studyId), options, true, version);

            // Rebuild family index (only for studies that have trios defined)
            List<Trio> studyTrios = getTriosForStudy(study);
            if (!studyTrios.isEmpty()) {
                variantStorageEngine.familyIndex(study, studyTrios, options);
            }

            // Compare rebuilt collection with snapshot
            Map<String, Document> actual = snapshotCollection(collectionName);
            System.out.println("Study " + study + ": rebuilt " + actual.size() + " documents");

            assertEqualCollections(study, expected, actual);
        }
    }

    /**
     * Read all documents from a MongoDB collection into a map keyed by document {@code _id}.
     */
    private Map<String, Document> snapshotCollection(String collectionName) {
        MongoDataStoreManager mongoDataStoreManager = ((MongoDBVariantStorageEngine) variantStorageEngine).getDBAdaptor().getMongoDataStoreManager();
        MongoDBCollection collection = mongoDataStoreManager.get(variantStorageEngine.getDBName()).getCollection(collectionName);
        Map<String, Document> snapshot = new HashMap<>();
        try (MongoDBIterator<Document> it = collection.iterator(new Document(), new QueryOptions())) {
            while (it.hasNext()) {
                Document doc = it.next();
                snapshot.put(doc.getString("_id"), doc);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return snapshot;
    }

    /**
     * Drop the given MongoDB collection so the next rebuild starts from an empty state.
     */
    private void dropCollection(String collection) {
        ((MongoDBSampleIndexDBAdaptor) sampleIndexDBAdaptor).dropCollection(collection);
    }

    private void assertEqualCollections(String study, Map<String, Document> expected, Map<String, Document> actual) {
        assertEquals("Different number of documents for study " + study, expected.size(), actual.size());
        for (Map.Entry<String, Document> entry : expected.entrySet()) {
            String docId = entry.getKey();
            Document actualDoc = actual.get(docId);
            assertNotNull("Missing document '" + docId + "' for study " + study, actualDoc);
            assertEquals("Document mismatch for '" + docId + "' in study " + study, entry.getValue(), actualDoc);
        }
    }

}
