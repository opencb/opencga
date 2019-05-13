/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.json.JsonDataReader;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.db.VariantStatsDBWriter;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;

/**
 * Created by jmmut on 12/02/15.
 */
public class DefaultVariantStatisticsManager extends VariantStatisticsManager {

    public static final String OUTPUT_FILE_NAME = "output.file.name";
    public static final String OUTPUT = "output";
    public static final String STATS_LOAD_PARALLEL = "stats.load.parallel";
    public static final boolean DEFAULT_STATS_LOAD_PARALLEL = true;

    protected static final String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";
    protected static final String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";

    private final JsonFactory jsonFactory;
    protected final ObjectMapper jsonObjectMapper;
    private final VariantDBAdaptor dbAdaptor;
    protected long numStatsToLoad = 0;
    private static Logger logger = LoggerFactory.getLogger(DefaultVariantStatisticsManager.class);
    private final IOManagerProvider ioManagerProvider;

    public DefaultVariantStatisticsManager(VariantDBAdaptor dbAdaptor, IOManagerProvider ioManagerProvider) {
        this.dbAdaptor = dbAdaptor;
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        this.ioManagerProvider = ioManagerProvider;
    }

    @Override
    public void calculateStatistics(String study, List<String> cohorts, QueryOptions options) throws IOException, StorageEngineException {

        URI output;
        try {
            output = UriUtils.createUri(options.getString(OUTPUT));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        URI stats = createStats(dbAdaptor, output, study, cohorts, options);

        loadStats(dbAdaptor, stats, study, options);
    }

    /**
     * Gets iterator from OpenCGA Variant database.
     **/
    private Iterator<Variant> obtainIterator(VariantDBAdaptor variantDBAdaptor, Query query, QueryOptions options) {

        Query iteratorQuery = query != null ? query : new Query();
        //Parse query options
        QueryOptions iteratorQueryOptions = options != null ? options : new QueryOptions();

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternate", "reference", "sourceEntries");
        iteratorQueryOptions.add("include", include);

        return variantDBAdaptor.iterator(iteratorQuery, iteratorQueryOptions);
    }

    public final URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, String study, List<String> cohorts,
                           QueryOptions options) throws IOException, StorageEngineException {

        VariantStorageMetadataManager metadataManager = variantDBAdaptor.getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        Map<String, Set<String>> cohortsMap = new LinkedHashMap<>(cohorts.size());
        for (String cohort : cohorts) {
            if (isAggregated(studyMetadata, options)) {
                cohortsMap.put(cohort, Collections.emptySet());
            } else {
                Integer cohortId = metadataManager.getCohortId(studyMetadata.getId(), cohort);
                if (cohortId == null) {
                    throw new StorageEngineException("Unknown cohort " + cohort);
                }
                Set<String> samples = new LinkedHashSet<>();
                for (Integer sampleId : metadataManager.getCohortMetadata(studyMetadata.getId(), cohort).getSamples()) {
                    samples.add(metadataManager.getSampleName(studyMetadata.getId(), sampleId));
                }
                cohortsMap.put(cohort, samples);
            }
        }
        return createStats(variantDBAdaptor, output, cohortsMap, null, studyMetadata, options);
    }
    /**
     * retrieves batches of Variants, delegates to obtain VariantStatsWrappers from those Variants, and writes them to the output URI.
     * <p>
     * steps:
     * <p>
     * * gets options like batchsize, overwrite, tagMap...  if(options == null) throws
     * * if no cohorts provided and the study is aggregated, uses the cohorts in the tagMap if any
     * * add the cohorts to the studyConfiuration
     * * checks invalidated stats, and set overwrite=true if needed
     * * sets up a ParallelTaskRunner: a reader, a writer and tasks
     * * writes the source stats
     *
     * @param variantDBAdaptor to obtain the Variants
     * @param output           where to write the VariantStats
     * @param cohorts          cohorts (subsets) of the samples. key: cohort name, defaultValue: list of sample names.
     * @param cohortIds        Cohort ID
     * @param studyMetadata Study configuration object
     * @param options          (mandatory) fileId, (optional) filters to the query, batch size, number of threads to use...
     * @return outputUri prefix for the file names (without the "._type_.stats.json.gz")
     * @throws IOException If any error occurs
     * @throws StorageEngineException If any error occurs
     */
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts,
                           Map<String, Integer> cohortIds, StudyMetadata studyMetadata, QueryOptions options)
            throws IOException, StorageEngineException {
//        String fileId;
        if (options == null) {
            options = new QueryOptions();
        }

        //Parse query options
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue());
        int numTasks = options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue());
        boolean overwrite = options.getBoolean(Options.OVERWRITE_STATS.key(), false);
        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
        Properties tagmap = VariantStatisticsManager.getAggregationMappingProperties(options);
//            fileId = options.getString(VariantStorageEngine.Options.FILE_ID.key());
        Aggregation aggregation = getAggregation(studyMetadata, options);

        // if no cohorts provided and the study is aggregated: try to get the cohorts from the tagMap
        if (cohorts == null || AggregationUtils.isAggregated(aggregation) && tagmap != null) {
            if (AggregationUtils.isAggregated(aggregation) && tagmap != null) {
                cohorts = new LinkedHashMap<>();
                for (String c : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
                    cohorts.put(c, Collections.emptySet());
                }
            } else {
                cohorts = new LinkedHashMap<>();
            }
        }

        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        preCalculateStats(metadataManager, studyMetadata, cohorts, overwrite, updateStats, options);
        overwrite = checkOverwrite(metadataManager, studyMetadata, cohorts, overwrite);

        VariantSourceStats variantSourceStats = new VariantSourceStats(null/*FILE_ID*/, Integer.toString(studyMetadata.getId()));


        // reader, tasks and writer
        Query readerQuery = VariantStatisticsManager.buildInputQuery(variantDBAdaptor.getMetadataManager(),
                studyMetadata, cohorts.keySet(), overwrite, updateStats, options);
        logger.info("ReaderQuery: " + readerQuery.toJson());
        QueryOptions readerOptions = new QueryOptions(QueryOptions.SORT, true)
                .append(QueryOptions.EXCLUDE, VariantField.ANNOTATION);
        logger.info("ReaderQueryOptions: " + readerOptions.toJson());
        VariantDBReader reader = new VariantDBReader(variantDBAdaptor, readerQuery, readerOptions);
        List<Task<Variant, String>> tasks = new ArrayList<>(numTasks);
        ProgressLogger progressLogger = buildCreateStatsProgressLogger(dbAdaptor, readerQuery, readerOptions);
        for (int i = 0; i < numTasks; i++) {
            tasks.add(new VariantStatsWrapperTask(overwrite, cohorts, studyMetadata, variantSourceStats, tagmap, progressLogger,
                    aggregation));
        }
        StringDataWriter writer = buildVariantStatsStringDataWriter(output);

        // runner
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(numTasks).setBatchSize(batchSize).build();
        ParallelTaskRunner runner = new ParallelTaskRunner<>(reader, tasks, writer, config);
        try {
            logger.info("starting stats creation for cohorts {}", cohorts.keySet());
            long start = System.currentTimeMillis();
            runner.run();
            logger.info("finishing stats creation, time: {}ms", System.currentTimeMillis() - start);
        } catch (ExecutionException e) {
            throw new StorageEngineException("Unable to calculate statistics.", e);
        }
        // source stats
        URI fileSourcePath = UriUtils.replacePath(output, output.getPath() + SOURCE_STATS_SUFFIX);
        try (OutputStream outputSourceStream = ioManagerProvider.newOutputStream(fileSourcePath)) {
            ObjectWriter sourceWriter = jsonObjectMapper.writerFor(VariantSourceStats.class);
            outputSourceStream.write(sourceWriter.writeValueAsBytes(variantSourceStats));
        }

//        variantDBAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata, options);

        return output;
    }

    protected ProgressLogger buildCreateStatsProgressLogger(VariantDBAdaptor variantDBAdaptor, Query readerQuery, QueryOptions options) {
        boolean skipCount = options.getBoolean(QueryOptions.SKIP_COUNT, false);
        return new ProgressLogger("Calculated stats:",
                () -> {
                    if (skipCount) {
                        return 0L;
                    } else {
                        numStatsToLoad = variantDBAdaptor.count(readerQuery).first();
                        return numStatsToLoad;
                    }
                }, 200).setBatchSize(5000);
    }

    protected StringDataWriter buildVariantStatsStringDataWriter(URI output) throws IOException {
        URI variantStats = UriUtils.replacePath(output, output.getPath() + VARIANT_STATS_SUFFIX);
        logger.info("will write stats to {}", variantStats);
        return new StringDataWriter(ioManagerProvider.newOutputStream(variantStats), true, true);
    }

    class VariantStatsWrapperTask implements Task<Variant, String> {

        private boolean overwrite;
        private Map<String, Set<String>> cohorts;
        private StudyMetadata studyMetadata;
        private final ProgressLogger progressLogger;
        //        private String fileId;
        private ObjectMapper jsonObjectMapper;
        private ObjectWriter variantsWriter;
        private VariantSourceStats variantSourceStats;
        private Properties tagmap;
        private VariantStatisticsCalculator variantStatisticsCalculator;

        VariantStatsWrapperTask(boolean overwrite, Map<String, Set<String>> cohorts,
                                StudyMetadata studyMetadata, VariantSourceStats variantSourceStats, Properties tagmap,
                                ProgressLogger progressLogger, Aggregation aggregation) {
            this.overwrite = overwrite;
            this.cohorts = cohorts;
            this.studyMetadata = studyMetadata;
            this.progressLogger = progressLogger;
            jsonObjectMapper = new ObjectMapper(new JsonFactory());
            jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
            jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
            variantsWriter = jsonObjectMapper.writerFor(VariantStatsWrapper.class);
            this.variantSourceStats = variantSourceStats;
            this.tagmap = tagmap;
            variantStatisticsCalculator = new VariantStatisticsCalculator(overwrite);
            variantStatisticsCalculator.setAggregationType(aggregation, tagmap);
        }

        @Override
        public List<String> apply(List<Variant> variants) {

            List<String> strings = new ArrayList<>(variants.size());
            boolean defaultCohortAbsent = false;

            List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variants,
                    studyMetadata.getName(), cohorts);

            long start = System.currentTimeMillis();
            for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                try {
                    strings.add(variantsWriter.writeValueAsString(variantStatsWrapper));
                    if (variantStatsWrapper.getCohortStats().get(StudyEntry.DEFAULT_COHORT) == null) {
                        defaultCohortAbsent = true;
                    }
                } catch (JsonProcessingException e) {
                    throw Throwables.propagate(e);
                }
            }

            // we don't want to overwrite file stats regarding all samples with stats about a subset of samples. Maybe if we change
            // VariantSource.stats to a map with every subset...
            if (!defaultCohortAbsent) {
                synchronized (variantSourceStats) {
                    variantSourceStats.updateFileStats(variants);
                    variantSourceStats.updateSampleStats(variants, null);  // TODO test
                }
            }
            logger.debug("another batch of {} elements calculated. time: {}ms", strings.size(), System.currentTimeMillis() - start);
            if (!variants.isEmpty()) {
                progressLogger.increment(variants.size(), () -> ", up to position "
                        + variants.get(variants.size() - 1).getChromosome()
                        + ":"
                        + variants.get(variants.size() - 1).getStart());
//                logger.info("stats created up to position {}:{}", variants.get(variants.size() - 1).getChromosome(),
//                        variants.get(variants.size() - 1).getStart());
            } else {
                logger.info("task with empty batch");
            }
            return strings;
        }

        @Override
        public void post() {
            if (variantStatisticsCalculator.getSkippedFiles() > 0) {
                logger.warn("Non calculated variant stats: " + variantStatisticsCalculator.getSkippedFiles());
            }
        }
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, String study, QueryOptions options) throws
            IOException, StorageEngineException {
        VariantStorageMetadataManager variantStorageMetadataManager = variantDBAdaptor.getMetadataManager();
        StudyMetadata studyMetadata = variantStorageMetadataManager.getStudyMetadata(study);
        loadStats(uri, studyMetadata, options);
    }

    public void loadStats(URI uri, StudyMetadata studyMetadata, QueryOptions options) throws
            IOException, StorageEngineException {

        URI variantStatsUri = UriUtils.replacePath(uri, uri.getPath() + VARIANT_STATS_SUFFIX);
        URI sourceStatsUri = UriUtils.replacePath(uri, uri.getPath() + SOURCE_STATS_SUFFIX);

        Set<String> cohorts = readCohortsFromStatsFile(variantStatsUri);
//        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
//        checkAndUpdateCalculatedCohorts(studyMetadata, variantStatsUri, updateStats);

        boolean error = false;
        try {
            logger.info("starting stats loading from {} and {}", variantStatsUri, sourceStatsUri);
            long start = System.currentTimeMillis();

            loadVariantStats(variantStatsUri, studyMetadata, options);
//        loadSourceStats(variantDBAdaptor, sourceStatsUri, studyMetadata, options);

            logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            error = true;
            throw e;
        } finally {
            postCalculateStats(dbAdaptor.getMetadataManager(), studyMetadata, cohorts, error);
        }
//        variantDBAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata, options);
    }

    public void loadVariantStats(URI uri, StudyMetadata studyMetadata, QueryOptions options)
            throws IOException, StorageEngineException {

        /* Open input streams */

        InputStream variantInputStream = ioManagerProvider.newInputStream(uri);


        ProgressLogger progressLogger = new ProgressLogger("Loaded stats:", numStatsToLoad);
        ParallelTaskRunner<VariantStatsWrapper, ?> ptr;
        DataReader<VariantStatsWrapper> dataReader = newVariantStatsWrapperDataReader(variantInputStream);
        List<VariantStatsDBWriter> writers = new ArrayList<>();
        if (options.getBoolean(STATS_LOAD_PARALLEL, DEFAULT_STATS_LOAD_PARALLEL)) {
            ptr = new ParallelTaskRunner<>(
                    dataReader,
                    () -> {
                        VariantStatsDBWriter dbWriter = newVariantStatisticsDBWriter(dbAdaptor, studyMetadata, options);
                        dbWriter.pre();
                        dbWriter.setProgressLogger(progressLogger);
                        writers.add(dbWriter);
                        return (batch -> {
                            dbWriter.write(batch);
                            return Collections.emptyList();
                        });
                    },
                    null,
                    ParallelTaskRunner.Config.builder().setAbortOnFail(true)
                            .setBatchSize(options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue()))
                            .setNumTasks(options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue())).build()

            );
        } else {
            VariantStatsDBWriter dbWriter = newVariantStatisticsDBWriter(dbAdaptor, studyMetadata, options);
            dbWriter.setProgressLogger(progressLogger);
            writers.add(dbWriter);
            ptr = new ParallelTaskRunner<>(
                    dataReader,
                    batch -> batch,
                    dbWriter,
                    ParallelTaskRunner.Config.builder().setAbortOnFail(true)
                            .setBatchSize(options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue()))
                            .setNumTasks(options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue())).build()

            );
        }

        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading stats", e);
        }

        Long writes = writers.stream().map(VariantStatsDBWriter::getNumWrites).reduce((a, b) -> a + b).orElse(0L);
        Long variantStats = writers.stream().map(VariantStatsDBWriter::getVariantStats).reduce((a, b) -> a + b).orElse(0L);
        if (writes < variantStats) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantStats, writes);
            logger.info("note: maybe those variants didn't had the proper study? maybe the new and the old stats were the same?");
        }

    }

    protected DataReader<VariantStatsWrapper> newVariantStatsWrapperDataReader(InputStream inputStream) {
        JsonDataReader<VariantStatsWrapper> reader = new JsonDataReader<>(VariantStatsWrapper.class, inputStream);
        reader.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        reader.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        return reader;
    }

    protected VariantStatsDBWriter newVariantStatisticsDBWriter(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata,
                                                             QueryOptions options) {
        return new VariantStatsDBWriter(dbAdaptor, studyMetadata, options);
    }

    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, String study, QueryOptions options)
            throws IOException {
        VariantStorageMetadataManager variantStorageMetadataManager = variantDBAdaptor.getMetadataManager();
        StudyMetadata studyMetadata = variantStorageMetadataManager.getStudyMetadata(study);
        loadSourceStats(variantDBAdaptor, uri, studyMetadata, options);
    }

    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyMetadata studyMetadata, QueryOptions options)
            throws IOException {
        /* Open input stream and initialize Json parse */
        VariantSourceStats variantSourceStats;
        try (InputStream sourceInputStream = ioManagerProvider.newInputStream(uri);
             JsonParser sourceParser = jsonFactory.createParser(sourceInputStream)) {
            variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);
        }

        // TODO if variantSourceStats doesn't have studyId and fileId, create another with variantSource.getStudyId() and variantSource
        // .getFileId()
//        variantDBAdaptor.getVariantFileMetadataDBAdaptor().updateStats(variantSourceStats, studyMetadata, options);

    }

    void checkAndUpdateCalculatedCohorts(StudyMetadata studyMetadata, URI uri, boolean updateStats)
            throws IOException, StorageEngineException {
        Set<String> cohortNames = readCohortsFromStatsFile(uri);
        VariantStatisticsManager
                .checkAndUpdateCalculatedCohorts(dbAdaptor.getMetadataManager(), studyMetadata, cohortNames, updateStats);
    }

    private Set<String> readCohortsFromStatsFile(URI uri) throws IOException {
        Set<String> cohortNames;
        /** Open input streams and Initialize Json parse **/
        try (InputStream variantInputStream = ioManagerProvider.newInputStream(uri);
             JsonParser parser = jsonFactory.createParser(variantInputStream)) {
            if (parser.nextToken() != null) {
                VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);
                cohortNames = variantStatsWrapper.getCohortStats().keySet();
            } else {
                throw new IOException("File " + uri + " is empty");
            }
        }
        return cohortNames;
    }

    protected static boolean checkOverwrite(VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata,
                                            Map<String, Set<String>> cohorts, boolean overwrite) {
        if (!overwrite) {
            for (String cohortName : cohorts.keySet()) {
                CohortMetadata cohort = metadataManager.getCohortMetadata(studyMetadata.getId(), cohortName);
                if (cohort.isInvalid()) {
                    logger.debug("Cohort \"{}\":{} is invalid. Need to overwrite stats. Using overwrite = true",
                            cohortName, cohort.getId());
                    overwrite = true;
                    break;
                }
            }
        }
        return overwrite;
    }

}
