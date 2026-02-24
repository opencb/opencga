package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBOperations;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.validGenotype;

/**
 * A pass-through {@link Task}{@code <MongoDBOperations, MongoDBOperations>} that builds sample-index
 * entries from the merged file documents piggybacked on each {@link MongoDBOperations} object.
 *
 * <p>This task is inserted between {@link org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMerger}
 * and the variant loader in both the direct-load and stage+merge pipelines. By running after the merger,
 * it operates on the final merged file documents (including any updated {@code alts} and {@code mgt} fields),
 * so the sample index is always consistent with what is stored in the variants collection.
 */
public class MongoDBSampleIndexFromMergeTask implements Task<MongoDBOperations, MongoDBOperations> {

    private final int studyId;
    private final SampleIndexSchema schema;
    private final MongoDBSampleGenotypeIndexerTask indexerTask;
    private final SampleIndexEntryWriter entryWriter;
    private final Set<String> loadedGenotypes = new HashSet<>();

    public MongoDBSampleIndexFromMergeTask(SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                           int studyId, List<Integer> sampleIds,
                                           ObjectMap options, SampleIndexSchema schema) throws StorageEngineException {
        this.studyId = studyId;
        this.schema = schema;
        this.indexerTask = new MongoDBSampleGenotypeIndexerTask(sampleIndexDBAdaptor, studyId, sampleIds, options, schema, true);
        this.entryWriter = sampleIndexDBAdaptor.newSampleIndexEntryWriter(studyId, -1, schema, options);
    }

    @Override
    public void pre() {
        entryWriter.open();
    }

    @Override
    public List<MongoDBOperations> apply(List<MongoDBOperations> batch) throws Exception {
        List<Document> syntheticDocs = new ArrayList<>();
        for (MongoDBOperations ops : batch) {
            for (Pair<Variant, List<Document>> pair : ops.getPendingFileDocs()) {
                Variant variant = pair.getKey();
                List<Document> fileDocs = pair.getValue();
                syntheticDocs.add(buildSyntheticDoc(variant, fileDocs));
                trackGenotypes(fileDocs);
            }
        }
        if (!syntheticDocs.isEmpty()) {
            List<SampleIndexEntry> entries = indexerTask.apply(syntheticDocs);
            if (!entries.isEmpty()) {
                entryWriter.write(entries);
            }
        }
        return batch;
    }

    @Override
    public List<MongoDBOperations> drain() throws Exception {
        List<SampleIndexEntry> entries = indexerTask.drain();
        if (!entries.isEmpty()) {
            entryWriter.write(entries);
        }
        return Collections.emptyList();
    }

    @Override
    public void post() {
        entryWriter.post();
    }

    public Set<String> getLoadedGenotypes() {
        return loadedGenotypes;
    }

    public int getSampleIndexVersion() {
        return schema.getVersion();
    }

    private void trackGenotypes(List<Document> fileDocs) {
        for (Document fileDoc : fileDocs) {
            Document mgt = fileDoc.get(DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD, Document.class);
            if (mgt != null) {
                for (String gtKey : mgt.keySet()) {
                    String gt = DocumentToSamplesConverter.genotypeToDataModelType(gtKey);
                    if (validGenotype(gt)) {
                        loadedGenotypes.add(gt);
                    }
                }
            }
        }
    }

    private Document buildSyntheticDoc(Variant variant, List<Document> fileDocs) {
        Document doc = new Document();
        doc.put(DocumentToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome());
        doc.put(DocumentToVariantConverter.START_FIELD, variant.getStart());
        doc.put(DocumentToVariantConverter.END_FIELD, variant.getEnd());
        doc.put(DocumentToVariantConverter.REFERENCE_FIELD, variant.getReference());
        doc.put(DocumentToVariantConverter.ALTERNATE_FIELD, variant.getAlternate());
        if (variant.getType() != null) {
            doc.put(DocumentToVariantConverter.TYPE_FIELD, variant.getType().name());
        }
        doc.put(DocumentToVariantConverter.FILES_FIELD, fileDocs);
        return doc;
    }
}
