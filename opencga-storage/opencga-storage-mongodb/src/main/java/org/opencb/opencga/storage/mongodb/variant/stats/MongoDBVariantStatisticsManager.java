package org.opencb.opencga.storage.mongodb.variant.stats;

import org.bson.Document;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.json.JsonSerializerTask;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created on 18/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStatisticsManager extends DefaultVariantStatisticsManager {

    private static Logger logger = LoggerFactory.getLogger(MongoDBVariantStatisticsManager.class);

    public MongoDBVariantStatisticsManager(VariantMongoDBAdaptor dbAdaptor, IOConnectorProvider ioConnectorProvider) {
        super(dbAdaptor, ioConnectorProvider);
    }

    @Override
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts,
                           Map<String, Integer> cohortIdsMap, StudyMetadata studyMetadata, QueryOptions options)
            throws IOException, StorageEngineException {
        if (options == null) {
            options = new QueryOptions();
        }
        Aggregation aggregation = getAggregation(studyMetadata, options);

        // This direct stats calculator does not work for aggregated studies.
        if (AggregationUtils.isAggregated(aggregation)) {
            return super.createStats(variantDBAdaptor, output, cohorts, cohortIdsMap, studyMetadata, options);
        }

        //Parse query options
        int batchSize = options.getInt(
                VariantStorageOptions.STATS_CALCULATE_BATCH_SIZE.key(),
                VariantStorageOptions.STATS_CALCULATE_BATCH_SIZE.defaultValue());
        int numTasks = options.getInt(
                VariantStorageOptions.STATS_CALCULATE_THREADS.key(),
                VariantStorageOptions.STATS_CALCULATE_THREADS.defaultValue());
        boolean statsMultiAllelic = options.getBoolean(
                VariantStorageOptions.STATS_MULTI_ALLELIC.key(),
                VariantStorageOptions.STATS_MULTI_ALLELIC.defaultValue());
        boolean overwrite = options.getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(), false);

        if (cohorts == null) {
            cohorts = new LinkedHashMap<>();
        }

        preCalculateStats(variantDBAdaptor.getMetadataManager(), studyMetadata, cohorts, overwrite, options);
        overwrite = checkOverwrite(variantDBAdaptor.getMetadataManager(), studyMetadata, cohorts, overwrite);

//        VariantSourceStats variantSourceStats = new VariantSourceStats(/*FILE_ID*/, Integer.toString(studyMetadata.getStudyId()));

        // reader, tasks and writer
        Query readerQuery = VariantStatisticsManager.buildInputQuery(variantDBAdaptor.getMetadataManager(),
                studyMetadata, cohorts.keySet(), options);
        logger.info("ReaderQuery: " + readerQuery.toJson());
        QueryOptions readerOptions = VariantStatisticsManager.buildIncludeExclude().append(QueryOptions.SORT, true);
        logger.info("ReaderQueryOptions: " + readerOptions.toJson());

        try (MongoDBIterator<Document> it = ((VariantMongoDBAdaptor) variantDBAdaptor).nativeIterator(readerQuery, readerOptions, true)) {
            // reader
            DataReader<Document> reader = i -> {
                List<Document> documents = new ArrayList<>(i);
                while (it.hasNext() && i-- > 0) {
                    documents.add(it.next());
                }
                return documents;
            };

            // tasks
            List<Integer> cohortIds = variantDBAdaptor.getMetadataManager().getCohortIds(studyMetadata.getId(), cohorts.keySet());

            List<Task<Document, String>> tasks = new ArrayList<>(numTasks);
            ProgressLogger progressLogger = buildCreateStatsProgressLogger(variantDBAdaptor, readerQuery, options);
            MongoDBVariantStatsCalculator statsCalculatorTask = new MongoDBVariantStatsCalculator(
                    variantDBAdaptor.getMetadataManager(),
                    studyMetadata,
                    cohortIds,
                    getUnknownGenotype(options), statsMultiAllelic);
            for (int i = 0; i < numTasks; i++) {
                tasks.add(statsCalculatorTask
                        .then((Task<VariantStatsWrapper, VariantStatsWrapper>) batch -> {
                            progressLogger.increment(batch.size(), () -> ", up to position "
                                    + batch.get(batch.size() - 1).getChromosome()
                                    + ':'
                                    + batch.get(batch.size() - 1).getStart());
                            return batch;
                        })
                        .then(new JsonSerializerTask<>(VariantStatsWrapper.class)));
            }

            // writer
            StringDataWriter writer = buildVariantStatsStringDataWriter(output);

            // runner
            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(numTasks).setBatchSize(batchSize).build();
            ParallelTaskRunner<Document, String> runner = new ParallelTaskRunner<>(reader, tasks, writer, config);
            try {
                logger.info("Starting stats creation for cohorts {}", cohorts.keySet());
                long start = System.currentTimeMillis();
                runner.run();
                logger.info("Finishing stats creation, time: {}ms", System.currentTimeMillis() - start);
            } catch (ExecutionException e) {
                throw new StorageEngineException("Unable to calculate statistics.", e);
            }

            // source stats
//            Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
//            try (OutputStream outputSourceStream = getOutputStream(fileSourcePath, options)) {
//                ObjectWriter sourceWriter = jsonObjectMapper.writerFor(VariantSourceStats.class);
//                outputSourceStream.write(sourceWriter.writeValueAsBytes(variantSourceStats));
//            }

//            variantDBAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata, options);

            return output;
        }

    }

}
