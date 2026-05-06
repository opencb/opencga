package org.opencb.opencga.storage.mongodb.variant.index.sample.annotation;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexerTask;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.mongodb.variant.index.sample.DocumentToSampleIndexEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.index.sample.genotype.MongoDBSampleIndexEntryWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoDBSampleAnnotationIndexer extends SampleAnnotationIndexer {

    private final VariantStorageEngine engine;
    private final MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final DocumentToSampleIndexEntryConverter converter = new DocumentToSampleIndexEntryConverter();

    public MongoDBSampleAnnotationIndexer(MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor,
            VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected void indexBatch(int studyId, List<Integer> sampleIds, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {

        SampleIndexSchema schema = sampleIndexDBAdaptor.getSchemaFactory().getSchemaForVersion(studyId, sampleIndexVersion);

        String studyName = metadataManager.getStudyName(studyId);
        List<String> sampleNames = samplesAsNames(studyId, sampleIds);

        ProgressLogger progressLogger = new ProgressLogger("Building secondary sample annotation index");

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
                        VariantField.STUDIES_FILES,
                        VariantField.ANNOTATION))
                .append(QueryOptions.SORT, true);

        VariantQuery variantQuery = new VariantQuery()
                .study(studyName);

        if (sampleIds.size() < 50) {
            variantQuery.put(VariantQueryParam.SAMPLE.key(), sampleNames);
        } else {
            variantQuery.includeSample(sampleNames);
        }

        Query variantQueryProcessed = engine.preProcessQuery(new VariantQuery(variantQuery), variantOptions);

        VariantDBReader reader = new VariantDBReader(engine.getDBAdaptor(), variantQueryProcessed, variantOptions);
        SampleAnnotationIndexerTask task = new SampleAnnotationIndexerTask(sampleIds, schema);
        DataWriter<SampleIndexEntry> writer = new MongoDBSampleIndexEntryWriter(sampleIndexDBAdaptor, studyId, schema);

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
        } catch (Exception e) {
            throw new StorageEngineException("Error indexing sample annotation index for study " + studyName, e);
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
