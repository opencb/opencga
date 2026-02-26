package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
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
 * A {@link DataWriter}{@code <MongoDBOperations>} that builds sample-index entries from the merged
 * file documents piggybacked on each {@link MongoDBOperations} object.
 *
 * <p>This writer is used as the second branch of a {@link DataWriter#tee(DataWriter, DataWriter, boolean)}
 * composite writer alongside the variant loader. By receiving batches from the PTR writer thread (which
 * delivers them in reader order after the PTR's internal reordering), the writer's sorted accumulation
 * buffer always sees variants in chromosomal order, which is required for correct bucket flushing.
 *
 * <p>Previously this was a pass-through {@link org.opencb.commons.run.Task} inserted between
 * {@link org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMerger} and the variant
 * loader. Converting to a {@link DataWriter} allows it to be composed via
 * {@link DataWriter#tee(DataWriter, DataWriter, boolean)}, letting the variant loader and sample index
 * build proceed in parallel background threads.
 */
public class MongoDBSampleIndexFromMergeTask implements DataWriter<MongoDBOperations> {

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
    public boolean open() {
        entryWriter.open();
        return true;
    }

    @Override
    public boolean write(List<MongoDBOperations> batch) {
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
            try {
                List<SampleIndexEntry> entries = indexerTask.apply(syntheticDocs);
                if (!entries.isEmpty()) {
                    entryWriter.write(entries);
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @Override
    public boolean post() {
        try {
            List<SampleIndexEntry> entries = indexerTask.drain();
            if (!entries.isEmpty()) {
                entryWriter.write(entries);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        entryWriter.post();
        return true;
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
