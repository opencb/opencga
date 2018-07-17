package org.opencb.opencga.storage.mongodb.variant.stats;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.json.JsonSerializerTask;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.metadata.StudyConfigurationManager.checkStudyConfiguration;

/**
 * Created on 18/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStatisticsManager extends DefaultVariantStatisticsManager {

    public MongoDBVariantStatisticsManager(VariantMongoDBAdaptor dbAdaptor) {
        super(dbAdaptor);
    }

    @Override
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts,
                           Map<String, Integer> cohortIdsMap, StudyConfiguration studyConfiguration, QueryOptions options)
            throws IOException, StorageEngineException {

        // This direct stats calculator does not work for aggregated studies.
        if (studyConfiguration.isAggregated()) {
            return super.createStats(variantDBAdaptor, output, cohorts, cohortIdsMap, studyConfiguration, options);
        }

        if (options == null) {
            options = new QueryOptions();
        }

        //Parse query options
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int numTasks = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        boolean overwrite = options.getBoolean(Options.OVERWRITE_STATS.key(), false);
        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);

        if (cohorts == null) {
            cohorts = new LinkedHashMap<>();
        }
        checkAndUpdateStudyConfigurationCohorts(studyConfiguration, cohorts, overwrite, updateStats);
        List<Integer> cohortIds = cohorts.keySet().stream().map(studyConfiguration.getCohortIds()::get).collect(Collectors.toList());

        if (!overwrite) {
            for (String cohortName : cohorts.keySet()) {
                Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
                if (studyConfiguration.getInvalidStats().contains(cohortId)) {
                    logger.debug("Cohort \"{}\":{} is invalid. Need to overwrite stats. Using overwrite = true", cohortName, cohortId);
                    overwrite = true;
                }
            }
        }
        checkStudyConfiguration(studyConfiguration);

//        VariantSourceStats variantSourceStats = new VariantSourceStats(/*FILE_ID*/, Integer.toString(studyConfiguration.getStudyId()));

        // reader, tasks and writer
        Query readerQuery = VariantStatisticsManager.buildInputQuery(studyConfiguration, cohorts.keySet(), overwrite, updateStats, options);
        logger.info("ReaderQuery: " + readerQuery.toJson());
        QueryOptions readerOptions = VariantStatisticsManager.buildIncludeExclude().append(QueryOptions.SORT, true);
        logger.info("ReaderQueryOptions: " + readerOptions.toJson());

        try (MongoCursor<Document> cursor = ((VariantMongoDBAdaptor) variantDBAdaptor).nativeIterator(readerQuery, readerOptions, true)) {
            // reader
            DataReader<Document> reader = i -> {
                List<Document> documents = new ArrayList<>(i);
                while (cursor.hasNext() && i-- > 0) {
                    documents.add(cursor.next());
                }
                return documents;
            };

            // tasks
            List<Task<Document, String>> tasks = new ArrayList<>(numTasks);
            ProgressLogger progressLogger = buildCreateStatsProgressLogger(variantDBAdaptor, readerQuery, options);
            for (int i = 0; i < numTasks; i++) {
                tasks.add(new MongoDBVariantStatsCalculator(studyConfiguration, cohortIds, "./.")
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

            variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

            return output;
        }

    }

}
