package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryConverter;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexVariantConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.*;

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
        MongoDBCollection collection = sampleIndexDBAdaptor.createCollectionIfNeeded(studyId, schema.getVersion());
        SampleIndexVariantConverter variantConverter = new SampleIndexVariantConverter(schema);

        // Build map of sampleId -> SampleMetadata for file position lookup
        Map<Integer, SampleMetadata> sampleMetadataMap = new HashMap<>();
        for (Integer sampleId : sampleIds) {
            sampleMetadataMap.put(sampleId, metadataManager.getSampleMetadata(studyId, sampleId));
        }


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

        // Process chromosome in batches
        Map<Integer, Map<Integer, SampleIndexEntryBuilder>> sampleBuilders = new HashMap<>();

        VariantQuery variantQuery = new VariantQuery()
                .study(studyName);

        if (sampleIds.size() < 50) {
            variantQuery.put(VariantQueryParam.SAMPLE.key(), sampleNames);
        } else {
            variantQuery.includeSample(sampleNames);
        }
        VariantDBReader reader = new VariantDBReader(engine.getDBAdaptor(), variantQuery, variantOptions);
        Task<Variant, SampleIndexEntry> task = new SampleIndexEntryConverter(sampleIndexDBAdaptor, studyId, sampleIds, options, schema);
        MongoDBSampleIndexEntryWriter writer = sampleIndexDBAdaptor.newSampleIndexEntryWriter(studyId, schema, options);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(4).setBatchSize(1000).build();
        new ParallelTaskRunner<>(reader,
                task,
                writer, config);

    }

    private List<String> samplesAsNames(int studyId, List<Integer> sampleIds) {
        List<String> names = new ArrayList<>(sampleIds.size());
        for (Integer sampleId : sampleIds) {
            names.add(metadataManager.getSampleName(studyId, sampleId));
        }
        return names;
    }
}
