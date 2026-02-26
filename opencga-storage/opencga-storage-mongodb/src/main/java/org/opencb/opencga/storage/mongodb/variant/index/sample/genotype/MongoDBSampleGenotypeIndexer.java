package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.bson.Document;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * MongoDB implementation of SampleGenotypeIndexer that builds the sample genotype index
 * by reading raw Documents from the nativeIterator and writing index entries to MongoDB.
 *
 * <p>Uses the nativeIterator to read unmerged Documents so that per-file genotypes are
 * preserved exactly as stored, producing the same index as the initial load.
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
        if (sampleIds.size() < 50) {
            variantQuery.sample(sampleNames);
        } else {
            variantQuery.includeSample(sampleNames);
        }

        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) engine.getDBAdaptor();
        ParsedVariantQuery parsedVariantQuery = engine.parseQuery(variantQuery, variantOptions);
        DataReader<Document> reader = BatchUtils.toDataReader(dbAdaptor.nativeIterator(parsedVariantQuery, variantOptions, true));
        Task<Document, SampleIndexEntry> task = new MongoDBSampleGenotypeIndexerTask(
                sampleIndexDBAdaptor, studyId, sampleIds, options, schema);
        MongoDBSampleIndexEntryWriter writer = sampleIndexDBAdaptor.newSampleIndexEntryWriter(studyId, schema, options);

        // Note: Using a single task
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1)
                .setSorted(false)
                .setBatchSize(10).build();
        ParallelTaskRunner<Document, SampleIndexEntry> ptr = new ParallelTaskRunner<>(
                reader, task.then(progressLogger.asTask()), writer, config);

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
