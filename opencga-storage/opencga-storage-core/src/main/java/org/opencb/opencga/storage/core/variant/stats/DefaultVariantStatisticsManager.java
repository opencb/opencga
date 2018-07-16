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
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
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
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.db.VariantStatsDBWriter;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;

/**
 * Created by jmmut on 12/02/15.
 */
public class DefaultVariantStatisticsManager implements VariantStatisticsManager{

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
    protected static Logger logger = LoggerFactory.getLogger(DefaultVariantStatisticsManager.class);

    public DefaultVariantStatisticsManager(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
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


    protected OutputStream getOutputStream(Path filePath, QueryOptions options) throws IOException {
        OutputStream outputStream = new FileOutputStream(filePath.toFile());
        logger.info("will write stats to {}", filePath);
        if (filePath.toString().endsWith(".gz")) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
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

        StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, options).first();
        Map<String, Set<String>> cohortsMap = new HashMap<>(cohorts.size());
        for (String cohort : cohorts) {
            if (studyConfiguration.isAggregated()) {
                cohortsMap.put(cohort, Collections.emptySet());
            } else {
                Integer cohortId = studyConfiguration.getCohortIds().get(cohort);
                if (cohortId == null) {
                    throw new StorageEngineException("Unknown cohort " + cohort);
                }
                Set<String> samples = studyConfiguration.getCohorts().get(cohortId).stream()
                        .map(studyConfiguration.getSampleIds().inverse()::get)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                cohortsMap.put(cohort, samples);
            }
        }
        return createStats(variantDBAdaptor, output, cohortsMap, null, studyConfiguration, options);
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
     * @param studyConfiguration Study configuration object
     * @param options          (mandatory) fileId, (optional) filters to the query, batch size, number of threads to use...
     * @return outputUri prefix for the file names (without the "._type_.stats.json.gz")
     * @throws IOException If any error occurs
     * @throws StorageEngineException If any error occurs
     */
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts,
                           Map<String, Integer> cohortIds, StudyConfiguration studyConfiguration, QueryOptions options)
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

        // if no cohorts provided and the study is aggregated: try to get the cohorts from the tagMap
        if (cohorts == null || studyConfiguration.isAggregated() && tagmap != null) {
            if (studyConfiguration.isAggregated() && tagmap != null) {
                cohorts = new LinkedHashMap<>();
                for (String c : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
                    cohorts.put(c, Collections.emptySet());
                }
            } else {
                cohorts = new LinkedHashMap<>();
            }
        }

        studyConfiguration = preCalculateStats(cohorts, studyConfiguration, overwrite, updateStats);

        if (!overwrite) {
            for (String cohortName : cohorts.keySet()) {
                Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
                if (studyConfiguration.getInvalidStats().contains(cohortId)) {
                    logger.debug("Cohort \"{}\":{} is invalid. Need to overwrite stats. Using overwrite = true", cohortName, cohortId);
                    overwrite = true;
                }
            }
        }

        VariantSourceStats variantSourceStats = new VariantSourceStats(null/*FILE_ID*/, Integer.toString(studyConfiguration.getStudyId()));


        // reader, tasks and writer
        Query readerQuery = VariantStatisticsManager.buildInputQuery(studyConfiguration, cohorts.keySet(), overwrite, updateStats, options);
        logger.info("ReaderQuery: " + readerQuery.toJson());
        QueryOptions readerOptions = new QueryOptions(QueryOptions.SORT, true)
                .append(QueryOptions.EXCLUDE, VariantField.ANNOTATION);
        logger.info("ReaderQueryOptions: " + readerOptions.toJson());
        VariantDBReader reader = new VariantDBReader(studyConfiguration, variantDBAdaptor, readerQuery, readerOptions);
        List<Task<Variant, String>> tasks = new ArrayList<>(numTasks);
        ProgressLogger progressLogger = buildCreateStatsProgressLogger(dbAdaptor, readerQuery, readerOptions);
        for (int i = 0; i < numTasks; i++) {
            tasks.add(new VariantStatsWrapperTask(overwrite, cohorts, studyConfiguration, variantSourceStats, tagmap, progressLogger));
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
        Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
        try (OutputStream outputSourceStream = getOutputStream(fileSourcePath, options)) {
            ObjectWriter sourceWriter = jsonObjectMapper.writerFor(VariantSourceStats.class);
            outputSourceStream.write(sourceWriter.writeValueAsBytes(variantSourceStats));
        }

        variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

        return output;
    }

    protected StudyConfiguration preCalculateStats(Map<String, Set<String>> cohorts, StudyConfiguration studyConfiguration,
                                                   boolean overwrite, boolean updateStats)
            throws StorageEngineException {
        return dbAdaptor.getStudyConfigurationManager().lockAndUpdate(studyConfiguration.getStudyName(), sc -> {
            dbAdaptor.getStudyConfigurationManager().registerCohorts(sc, cohorts);
            checkAndUpdateStudyConfigurationCohorts(sc, cohorts, overwrite, updateStats);
            return sc;
        });
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

    protected StringDataWriter buildVariantStatsStringDataWriter(URI output) {
        Path variantStatsPath = Paths.get(output.getPath() + VARIANT_STATS_SUFFIX);
        logger.info("will write stats to {}", variantStatsPath);
        return new StringDataWriter(variantStatsPath, true);
    }

    class VariantStatsWrapperTask implements ParallelTaskRunner.Task<Variant, String> {

        private boolean overwrite;
        private Map<String, Set<String>> cohorts;
        private StudyConfiguration studyConfiguration;
        private final ProgressLogger progressLogger;
        //        private String fileId;
        private ObjectMapper jsonObjectMapper;
        private ObjectWriter variantsWriter;
        private VariantSourceStats variantSourceStats;
        private Properties tagmap;
        private VariantStatisticsCalculator variantStatisticsCalculator;

        VariantStatsWrapperTask(boolean overwrite, Map<String, Set<String>> cohorts,
                                StudyConfiguration studyConfiguration,
                                VariantSourceStats variantSourceStats, Properties tagmap, ProgressLogger progressLogger) {
            this.overwrite = overwrite;
            this.cohorts = cohorts;
            this.studyConfiguration = studyConfiguration;
            this.progressLogger = progressLogger;
            jsonObjectMapper = new ObjectMapper(new JsonFactory());
            jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
            jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
            variantsWriter = jsonObjectMapper.writerFor(VariantStatsWrapper.class);
            this.variantSourceStats = variantSourceStats;
            this.tagmap = tagmap;
            variantStatisticsCalculator = new VariantStatisticsCalculator(overwrite);
            variantStatisticsCalculator.setAggregationType(studyConfiguration.getAggregation(), tagmap);
        }

        @Override
        public List<String> apply(List<Variant> variants) {

            List<String> strings = new ArrayList<>(variants.size());
            boolean defaultCohortAbsent = false;

            List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variants,
                    studyConfiguration.getStudyName(), cohorts);

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
        StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, options).first();
        loadStats(variantDBAdaptor, uri, studyConfiguration, options);
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options) throws
            IOException, StorageEngineException {

        URI variantStatsUri = Paths.get(uri.getPath() + VARIANT_STATS_SUFFIX).toUri();
        URI sourceStatsUri = Paths.get(uri.getPath() + SOURCE_STATS_SUFFIX).toUri();

        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
        checkAndUpdateCalculatedCohorts(studyConfiguration, variantStatsUri, updateStats);

        logger.info("starting stats loading from {} and {}", variantStatsUri, sourceStatsUri);
        long start = System.currentTimeMillis();

        loadVariantStats(variantStatsUri, studyConfiguration, options);
//        loadSourceStats(variantDBAdaptor, sourceStatsUri, studyConfiguration, options);

        logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);

        variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

    }

    public void loadVariantStats(URI uri, StudyConfiguration studyConfiguration, QueryOptions options)
            throws IOException, StorageEngineException {

        /* Open input streams */
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);


        ProgressLogger progressLogger = new ProgressLogger("Loaded stats:", numStatsToLoad);
        ParallelTaskRunner<VariantStatsWrapper, ?> ptr;
        DataReader<VariantStatsWrapper> dataReader = newVariantStatsWrapperDataReader(variantInputStream);
        List<VariantStatsDBWriter> writers = new ArrayList<>();
        if (options.getBoolean(STATS_LOAD_PARALLEL, DEFAULT_STATS_LOAD_PARALLEL)) {
            ptr = new ParallelTaskRunner<>(
                    dataReader,
                    () -> {
                        VariantStatsDBWriter dbWriter = newVariantStatisticsDBWriter(dbAdaptor, studyConfiguration, options);
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
            VariantStatsDBWriter dbWriter = newVariantStatisticsDBWriter(dbAdaptor, studyConfiguration, options);
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

    protected VariantStatsDBWriter newVariantStatisticsDBWriter(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration,
                                                             QueryOptions options) {
        return new VariantStatsDBWriter(dbAdaptor, studyConfiguration, options);
    }

    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, String study, QueryOptions options)
            throws IOException {
        StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, options).first();
        loadSourceStats(variantDBAdaptor, uri, studyConfiguration, options);
    }

    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options)
            throws IOException {

        /* select input path */
        Path sourceInput = Paths.get(uri.getPath());

        /* Open input stream and initialize Json parse */
        VariantSourceStats variantSourceStats;
        try (InputStream sourceInputStream = new GZIPInputStream(new FileInputStream(sourceInput.toFile()));
             JsonParser sourceParser = jsonFactory.createParser(sourceInputStream)) {
            variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);
        }

        // TODO if variantSourceStats doesn't have studyId and fileId, create another with variantSource.getStudyId() and variantSource
        // .getFileId()
//        variantDBAdaptor.getVariantFileMetadataDBAdaptor().updateStats(variantSourceStats, studyConfiguration, options);

    }

    /*
     * Check that all SampleIds are in the StudyConfiguration.
     * <p>
     * If some cohort does not have samples, reads the content from StudyConfiguration.
     * If there is no cohortId for come cohort, reads the content from StudyConfiguration or auto-generate a cohortId
     * If some cohort has a different number of samples, check if this cohort is invalid.
     * <p>
     * Do not update the "calculatedStats" array. Just check that the provided cohorts are not calculated or invalid.
     * <p>
     * new requirements:
     * * an empty cohort is not an error if the study is aggregated
     * * there may be several empty cohorts, not just the ALL, because there may be several aggregated files with different sets of
     * hidden samples.
     * * if a cohort is already calculated, it is not an error if overwrite was provided
     *
     */
    protected static List<Integer> checkAndUpdateStudyConfigurationCohorts(StudyConfiguration studyConfiguration,
                                                          Map<String, Set<String>> cohorts,
                                                          boolean overwrite, boolean updateStats)
            throws StorageEngineException {
        List<Integer> cohortIdList = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Set<String> samples = entry.getValue();
            final int cohortId = studyConfiguration.getCohortIds().get(cohortName);

            final Set<Integer> sampleIds;
            if (samples == null || samples.isEmpty()) {
                //There are not provided samples for this cohort. Take samples from StudyConfiguration
                if (studyConfiguration.isAggregated()) {
                    samples = Collections.emptySet();
                    sampleIds = Collections.emptySet();
                } else {
                    sampleIds = studyConfiguration.getCohorts().get(cohortId);
                    if (sampleIds == null || sampleIds.isEmpty()) {
//                if (sampleIds == null || (sampleIds.isEmpty()
//                        && Aggregation.NONE.equals(studyConfiguration.getAggregation()))) {
                        //ERROR: StudyConfiguration does not have samples for this cohort, and it is not an aggregated study
                        throw new StorageEngineException("Cohort \"" + cohortName + "\" is empty");
                    }
                    samples = new HashSet<>();
                    Map<Integer, String> idSamples = StudyConfiguration.inverseMap(studyConfiguration.getSampleIds());
                    for (Integer sampleId : sampleIds) {
                        samples.add(idSamples.get(sampleId));
                    }
                }
                cohorts.put(cohortName, samples);
            } else {
                sampleIds = new HashSet<>(samples.size());
                for (String sample : samples) {
                    if (!studyConfiguration.getSampleIds().containsKey(sample)) {
                        //ERROR Sample not found
                        throw new StorageEngineException("Sample " + sample + " not found in the StudyConfiguration");
                    } else {
                        sampleIds.add(studyConfiguration.getSampleIds().get(sample));
                    }
                }
                if (sampleIds.size() != samples.size()) {
                    throw new StorageEngineException("Duplicated samples in cohort " + cohortName + ":" + cohortId);
                }
                if (studyConfiguration.getCohorts().get(cohortId) != null
                        && !sampleIds.equals(studyConfiguration.getCohorts().get(cohortId))) {
                    if (!studyConfiguration.getInvalidStats().contains(cohortId)
                            && studyConfiguration.getCalculatedStats().contains(cohortId)) {
                        //If provided samples are different than the stored in the StudyConfiguration, and the cohort was not invalid.
                        throw new StorageEngineException("Different samples in cohort " + cohortName + ":" + cohortId + ". "
                                + "Samples in the StudyConfiguration: " + studyConfiguration.getCohorts().get(cohortId).size() + ". "
                                + "Samples provided " + samples.size() + ". Invalidate stats to continue.");
                    }
                }
            }

//            if (studyConfiguration.getInvalidStats().contains(cohortId)) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
//            }
            if (studyConfiguration.getCalculatedStats().contains(cohortId)) {
                if (overwrite) {
                    studyConfiguration.getCalculatedStats().remove(cohortId);
                    studyConfiguration.getInvalidStats().add(cohortId);
                } else if (updateStats) {
                    logger.debug("Cohort \"" + cohortName + "\" stats already calculated. Calculate only for missing positions");
                } else {
                    throw new StorageEngineException("Cohort \"" + cohortName + "\" stats already calculated");
                }
            }

            cohortIdList.add(cohortId);
            studyConfiguration.getCohortIds().put(cohortName, cohortId);
            studyConfiguration.getCohorts().put(cohortId, sampleIds);
        }
        return cohortIdList;
    }

    void checkAndUpdateCalculatedCohorts(StudyConfiguration studyConfiguration, URI uri, boolean updateStats)
            throws IOException, StorageEngineException {
        /** Select input path **/
        Path variantInput = Paths.get(uri.getPath());

        /** Open input streams and Initialize Json parse **/
        try (InputStream variantInputStream = new GZIPInputStream(new FileInputStream(variantInput.toFile()));
             JsonParser parser = jsonFactory.createParser(variantInputStream)) {
            if (parser.nextToken() != null) {
                VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);
                Set<String> cohortNames = variantStatsWrapper.getCohortStats().keySet();
                VariantStatisticsManager.checkAndUpdateCalculatedCohorts(studyConfiguration, cohortNames, updateStats);
            } else {
                throw new IOException("File " + uri + " is empty");
            }
        }
    }

}
