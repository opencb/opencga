package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexerTask;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * MongoDB implementation of SampleGenotypeIndexer that builds the sample genotype index
 * by reading variants from the variant storage and writing index entries to MongoDB.
 */
public class MongoDBSampleGenotypeIndexer extends SampleGenotypeIndexer {

    private final VariantStorageEngine engine;
    private final MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor;

    public MongoDBSampleGenotypeIndexer(MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor, VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected void indexBatch(int studyId, SampleIndexSchema schema, List<Integer> sampleIds, ObjectMap options)
            throws StorageEngineException {
        String studyName = metadataManager.getStudyName(studyId);
        List<String> sampleNames = samplesAsNames(studyId, sampleIds);

        ProgressLogger progressLogger = new ProgressLogger("Building sample genotype index");

        QueryOptions variantOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(
                        VariantField.ID,
                        VariantField.CHROMOSOME,
                        VariantField.START,
                        VariantField.END,
                        VariantField.REFERENCE,
                        VariantField.ALTERNATE,
                        VariantField.TYPE,
                        VariantField.STUDIES_SAMPLES,
                        VariantField.STUDIES_FILES))
                .append(QueryOptions.SORT, true);

        VariantQuery variantQuery = new VariantQuery()
                .study(studyName);
        boolean hasMultiFileSample = false;
        Set<String> allFileNames = new LinkedHashSet<>();
        for (Integer sampleId : sampleIds) {
            SampleMetadata sm = metadataManager.getSampleMetadata(studyId, sampleId);
            if (sm.isMultiFileSample()) {
                hasMultiFileSample = true;
            }
            for (Integer fileId : sm.getFiles()) {
                allFileNames.add(metadataManager.getFileName(studyId, fileId));
            }
        }
        if (hasMultiFileSample) {
            // FIXME: sample filter doesn't work with multi-file samples. We need to include all samples and filter them in the reader
            variantQuery.includeSample(sampleNames);
        } else if (sampleIds.size() < 50) {
            variantQuery.sample(sampleNames);
        } else {
            variantQuery.includeSample(sampleNames);
        }
        VariantDBReader reader = new VariantDBReader(engine.getDBAdaptor(), variantQuery, variantOptions);
        Task<Variant, SampleIndexEntry> task = new SampleGenotypeIndexerTask(sampleIndexDBAdaptor, studyId, sampleIds, options, schema);
        MongoDBSampleIndexEntryWriter writer = sampleIndexDBAdaptor.newSampleIndexEntryWriter(studyId, schema, options);

        // Note: Using a single task
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1)
                .setSorted(false)
                .setBatchSize(10).build();
        ParallelTaskRunner<Variant, SampleIndexEntry> ptr = new ParallelTaskRunner<>(reader,
                task.then(progressLogger.asTask()),
                writer, config);

        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error building sample genotype index", e);
        }

    }

    private List<String> samplesAsNames(int studyId, List<Integer> sampleIds) {
        List<String> names = new ArrayList<>(sampleIds.size());
        for (Integer sampleId : sampleIds) {
            names.add(metadataManager.getSampleName(studyId, sampleId));
        }
        return names;
    }
}
