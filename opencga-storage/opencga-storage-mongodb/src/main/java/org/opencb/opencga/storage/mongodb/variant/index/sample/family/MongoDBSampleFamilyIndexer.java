package org.opencb.opencga.storage.mongodb.variant.index.sample.family;

import org.bson.Document;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.index.sample.genotype.MongoDBSampleIndexEntryWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class MongoDBSampleFamilyIndexer extends SampleFamilyIndexer {

    private final MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final VariantStorageEngine engine;

    public MongoDBSampleFamilyIndexer(MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor, VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.engine = engine;
    }

    @Override
    protected void indexBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException {
        SampleIndexSchema schema = sampleIndexDBAdaptor.getSchemaFactory().getSchemaForVersion(studyId, version);
        List<String> childNames = trios.stream().map(Trio::getChild).collect(Collectors.toList());
        ProgressLogger progressLogger = new ProgressLogger("Building sample family index");

        // Collect all trio members so parent files are included in the projection
        Set<String> allMemberNamesSet = new LinkedHashSet<>(childNames);
        for (Trio trio : trios) {
            if (trio.getFather() != null) {
                allMemberNamesSet.add(trio.getFather());
            }
            if (trio.getMother() != null) {
                allMemberNamesSet.add(trio.getMother());
            }
        }
        List<String> allMemberNames = new ArrayList<>(allMemberNamesSet);

        QueryOptions queryOptions = new QueryOptions()
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

        VariantQuery variantQuery = new VariantQuery().study(study);
        if (childNames.size() < 50) {
            // Use all members (children + parents) as the sample filter so that variants where only
            // parents have non-ref GTs (but the child has 0/0) are also processed.  Those variants
            // can be non-de-novo Mendelian Errors (child should have inherited a parental allele).
            variantQuery.sample(allMemberNames);
        }
        // Always include all trio members so parent file data (mgt) is projected
        variantQuery.includeSample(allMemberNames);

        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) engine.getDBAdaptor();
        ParsedVariantQuery parsedQuery = engine.parseQuery(variantQuery, queryOptions);
        DataReader<Document> reader = BatchUtils.toDataReader(dbAdaptor.nativeIterator(parsedQuery, queryOptions, true));
        Task<Document, SampleIndexEntry> task = new MongoDBSampleFamilyIndexerTask(sampleIndexDBAdaptor, studyId, trios, options);
        MongoDBSampleIndexEntryWriter writer = sampleIndexDBAdaptor.newSampleIndexEntryWriter(studyId, schema, options);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1)
                .setSorted(false)
                .setBatchSize(10).build();
        ParallelTaskRunner<Document, SampleIndexEntry> ptr = new ParallelTaskRunner<>(
                reader, task.then(progressLogger.asTask()), writer, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error building sample family index", e);
        }
    }
}
