/*
 * Copyright 2015 OpenCB
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
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.opencb.biodata.models.variant.VariantSource.Aggregation.isAggregated;
import static org.opencb.opencga.storage.core.variant.VariantStorageETL.checkStudyConfiguration;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;

/**
 * Created by jmmut on 12/02/15.
 */
public class VariantStatisticsManager {

    public static final String OUTPUT_FILE_NAME = "output.file.name";

    private String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";
    private String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";
    private final JsonFactory jsonFactory;
    private ObjectMapper jsonObjectMapper;
    protected static Logger logger = LoggerFactory.getLogger(VariantStatisticsManager.class);

    public VariantStatisticsManager() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    private OutputStream getOutputStream(Path filePath, QueryOptions options) throws IOException {
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
     * @throws StorageManagerException If any error occurs
     */
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts,
                           Map<String, Integer> cohortIds, StudyConfiguration studyConfiguration, QueryOptions options)
            throws IOException, StorageManagerException {
//        String fileId;
        if (options == null) {
            options = new QueryOptions();
        }

        //Parse query options
        int batchSize = options.getInt(Options.LOAD_BATCH_SIZE.key(), 100); // future optimization, threads, etc
        int numTasks = options.getInt(Options.LOAD_THREADS.key(), 6);
        boolean overwrite = options.getBoolean(Options.OVERWRITE_STATS.key(), false);
        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
        Properties tagmap = options.get(Options.AGGREGATION_MAPPING_PROPERTIES.key(), Properties.class, null);
//            fileId = options.getString(VariantStorageManager.Options.FILE_ID.key());

        // if no cohorts provided and the study is aggregated: try to get the cohorts from the tagMap
        if (cohorts == null || isAggregated(studyConfiguration.getAggregation()) && tagmap != null) {
            if (isAggregated(studyConfiguration.getAggregation()) && tagmap != null) {
                cohorts = new LinkedHashMap<>();
                for (String c : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
                    cohorts.put(c, Collections.emptySet());
                }
            } else {
                cohorts = new LinkedHashMap<>();
            }
        }

        checkAndUpdateStudyConfigurationCohorts(studyConfiguration, cohorts, cohortIds, overwrite, updateStats);
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


        VariantSourceStats variantSourceStats = new VariantSourceStats(null/*FILE_ID*/, Integer.toString(studyConfiguration.getStudyId()));


        // reader, tasks and writer
        Query readerQuery = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId())
                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), studyConfiguration.getStudyId());
        if (options.containsKey(Options.FILE_ID.key())) {
            readerQuery.append(VariantDBAdaptor.VariantQueryParams.FILES.key(), options.get(Options.FILE_ID.key()));
        }
        if (options.containsKey(VariantDBAdaptor.VariantQueryParams.REGION.key())) {
            Object region = options.get(VariantDBAdaptor.VariantQueryParams.REGION.key());
            readerQuery.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), region);
        }
        if (updateStats) {
            //Get all variants that not contain any of the required cohorts
            readerQuery.append(VariantDBAdaptor.VariantQueryParams.COHORTS.key(),
                    cohorts.keySet().stream().map((cohort) -> "!" + studyConfiguration.getStudyName() + ":" + cohort).collect(Collectors
                            .joining(";")));
        }
        logger.info("ReaderQuery: " + readerQuery.toJson());
        QueryOptions readerOptions = new QueryOptions(QueryOptions.SORT, true);
        logger.info("ReaderQueryOptions: " + readerOptions.toJson());
        VariantDBReader reader = new VariantDBReader(studyConfiguration, variantDBAdaptor, readerQuery, readerOptions);
        List<ParallelTaskRunner.Task<Variant, String>> tasks = new ArrayList<>(numTasks);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Long> future = executor.submit(() -> variantDBAdaptor.count(readerQuery).first());
        executor.shutdown();
        ProgressLogger progressLogger = new ProgressLogger("Calculated stats:", future, 200).setBatchSize(5000);
        for (int i = 0; i < numTasks; i++) {
            tasks.add(new VariantStatsWrapperTask(overwrite, cohorts, studyConfiguration, variantSourceStats, tagmap, progressLogger));
        }
        Path variantStatsPath = Paths.get(output.getPath() + VARIANT_STATS_SUFFIX);
        logger.info("will write stats to {}", variantStatsPath);
        StringDataWriter writer = new StringDataWriter(variantStatsPath, true);

        // runner
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, numTasks * 2, false);
        ParallelTaskRunner runner = new ParallelTaskRunner<>(reader, tasks, writer, config);
        try {
            logger.info("starting stats creation for cohorts {}", cohorts.keySet());
            long start = System.currentTimeMillis();
            runner.run();
            logger.info("finishing stats creation, time: {}ms", System.currentTimeMillis() - start);
        } catch (ExecutionException e) {
            throw new StorageManagerException("Unable to calculate statistics.", e);
        }
        // source stats
        Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
        OutputStream outputSourceStream = getOutputStream(fileSourcePath, options);
        ObjectWriter sourceWriter = jsonObjectMapper.writerFor(VariantSourceStats.class);
        outputSourceStream.write(sourceWriter.writeValueAsBytes(variantSourceStats));
        outputSourceStream.close();

        variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

        return output;
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
                    studyConfiguration.getStudyName(), null/*fileId*/, cohorts);

            long start = System.currentTimeMillis();
            for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                try {
                    strings.add(variantsWriter.writeValueAsString(variantStatsWrapper));
                    if (variantStatsWrapper.getCohortStats().get(StudyEntry.DEFAULT_COHORT) == null) {
                        defaultCohortAbsent = true;
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
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

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options) throws
            IOException, StorageManagerException {

        URI variantStatsUri = Paths.get(uri.getPath() + VARIANT_STATS_SUFFIX).toUri();
        URI sourceStatsUri = Paths.get(uri.getPath() + SOURCE_STATS_SUFFIX).toUri();

        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
        checkAndUpdateCalculatedCohorts(studyConfiguration, variantStatsUri, updateStats);

        logger.info("starting stats loading from {} and {}", variantStatsUri, sourceStatsUri);
        long start = System.currentTimeMillis();

        loadVariantStats(variantDBAdaptor, variantStatsUri, studyConfiguration, options);
        loadSourceStats(variantDBAdaptor, sourceStatsUri, studyConfiguration, options);

        logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);

        variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

    }

    public void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options)
            throws IOException, StorageManagerException {

        variantDBAdaptor.preUpdateStats(studyConfiguration);

        /* Open input streams */
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        final int[] writes = {0};
        final int[] variantsNumber = {0};

        /* Initialize Json parse */
        try (JsonParser parser = jsonFactory.createParser(variantInputStream)) {
            ProgressLogger progressLogger = new ProgressLogger("Loaded stats:");
            ParallelTaskRunner<VariantStatsWrapper, Integer> ptr = new ParallelTaskRunner<>(
                    size -> {
                        ArrayList<VariantStatsWrapper> statsBatch = new ArrayList<>(size);
                        try {
                            while (parser.nextToken() != null && statsBatch.size() < size) {
                                variantsNumber[0]++;
                                statsBatch.add(parser.readValueAs(VariantStatsWrapper.class));
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return statsBatch;
                    },
                    statsBatch -> {
                        QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, studyConfiguration, options);
                        VariantStatsWrapper last = statsBatch.get(statsBatch.size() - 1);
                        progressLogger.increment(statsBatch.size(), ", up to position " + last.getChromosome() + ":" + last.getPosition());
                        return Collections.singletonList(writeResult.getNumResults());
                    },
                    writesList -> {
                        writes[0] += writesList.get(0);
                        return true;
                    },
                    ParallelTaskRunner.Config.builder().setAbortOnFail(true)
                            .setBatchSize(options.getInt(Options.LOAD_BATCH_SIZE.key(), Options.LOAD_BATCH_SIZE.defaultValue()))
                            .setNumTasks(options.getInt(Options.LOAD_THREADS.key(), Options.LOAD_THREADS.defaultValue())).build()

            );
            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageManagerException("Error loading stats", e);
            }
        }

        if (writes[0] < variantsNumber[0]) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantsNumber[0], writes[0]);
            logger.info("note: maybe those variants didn't had the proper study? maybe the new and the old stats were the same?");
        }

    }

    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options)
            throws IOException {

        /* Open input streams */
        Path sourceInput = Paths.get(uri.getPath());
        InputStream sourceInputStream;
        sourceInputStream = new FileInputStream(sourceInput.toFile());
        sourceInputStream = new GZIPInputStream(sourceInputStream);

        /* Initialize Json parse */
        JsonParser sourceParser = jsonFactory.createParser(sourceInputStream);

        VariantSourceStats variantSourceStats;
//        if (sourceParser.nextToken() != null) {
        variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);
//        }

        // TODO if variantSourceStats doesn't have studyId and fileId, create another with variantSource.getStudyId() and variantSource
        // .getFileId()
        variantDBAdaptor.getVariantSourceDBAdaptor().updateSourceStats(variantSourceStats, studyConfiguration, options);

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
    List<Integer> checkAndUpdateStudyConfigurationCohorts(StudyConfiguration studyConfiguration,
                                                          Map<String, Set<String>> cohorts,
                                                          Map<String, Integer> cohortIds,
                                                          boolean overwrite, boolean updateStats)
            throws StorageManagerException {
        List<Integer> cohortIdList = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Set<String> samples = entry.getValue();
            final int cohortId;


            // get a valid cohortId
            if (cohortIds == null || cohortIds.isEmpty()) {
                if (studyConfiguration.getCohortIds().containsKey(cohortName)) {
                    cohortId = studyConfiguration.getCohortIds().get(cohortName);
                } else {
                    //Auto-generate cohortId. Max CohortId + 1
                    // if there are no cohorts and we are creating the first as 0
                    cohortId = studyConfiguration.getCohortIds().isEmpty()
                            ? 0
                            : Collections.max(studyConfiguration.getCohortIds().values()) + 1;
                }
            } else {
                if (!cohortIds.containsKey(cohortName)) {
                    //ERROR Missing cohortId
                    throw new StorageManagerException("Missing cohortId for the cohort : " + cohortName);
                }
                cohortId = cohortIds.get(entry.getKey());
            }

            // check that the cohortId-cohortName is consistent with StudyConfiguration
            if (studyConfiguration.getCohortIds().containsKey(cohortName)) {
                if (!studyConfiguration.getCohortIds().get(cohortName).equals(cohortId)) {
                    //ERROR Duplicated cohortName
                    throw new StorageManagerException("Duplicated cohortName " + cohortName + ":" + cohortId
                            + ". Appears in the StudyConfiguration as "
                            + cohortName + ":" + studyConfiguration.getCohortIds().get(cohortName));
                }
            } else if (studyConfiguration.getCohortIds().containsValue(cohortId)) {
                //ERROR Duplicated cohortId
                throw new StorageManagerException("Duplicated cohortId " + cohortName + ":" + cohortId
                        + ". Appears in the StudyConfiguration as "
                        + StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).get(cohortId) + ":" + cohortId);
            }

            final Set<Integer> sampleIds;
            if (samples == null || samples.isEmpty()) {
                //There are not provided samples for this cohort. Take samples from StudyConfiguration
                if (isAggregated(studyConfiguration.getAggregation())) {
                    samples = Collections.emptySet();
                    sampleIds = Collections.emptySet();
                } else {
                    sampleIds = studyConfiguration.getCohorts().get(cohortId);
                    if (sampleIds == null || sampleIds.isEmpty()) {
//                if (sampleIds == null || (sampleIds.isEmpty()
//                        && VariantSource.Aggregation.NONE.equals(studyConfiguration.getAggregation()))) {
                        //ERROR: StudyConfiguration does not have samples for this cohort, and it is not an aggregated study
                        throw new StorageManagerException("Cohort \"" + cohortName + "\" is empty");
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
                        throw new StorageManagerException("Sample " + sample + " not found in the StudyConfiguration");
                    } else {
                        sampleIds.add(studyConfiguration.getSampleIds().get(sample));
                    }
                }
                if (sampleIds.size() != samples.size()) {
                    throw new StorageManagerException("Duplicated samples in cohort " + cohortName + ":" + cohortId);
                }
                if (studyConfiguration.getCohorts().get(cohortId) != null
                        && !sampleIds.equals(studyConfiguration.getCohorts().get(cohortId))) {
                    if (!studyConfiguration.getInvalidStats().contains(cohortId)) {
                        //If provided samples are different than the stored in the StudyConfiguration, and the cohort was not invalid.
                        throw new StorageManagerException("Different samples in cohort " + cohortName + ":" + cohortId + ". "
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
                    throw new StorageManagerException("Cohort \"" + cohortName + "\" stats already calculated");
                }
            }

            cohortIdList.add(cohortId);
            studyConfiguration.getCohortIds().put(cohortName, cohortId);
            studyConfiguration.getCohorts().put(cohortId, sampleIds);
        }
        return cohortIdList;
    }

    void checkAndUpdateCalculatedCohorts(StudyConfiguration studyConfiguration, URI uri, boolean updateStats) throws IOException {

        /** Open input streams **/
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        /** Initialize Json parse **/
        try (JsonParser parser = jsonFactory.createParser(variantInputStream)) {
            if (parser.nextToken() != null) {
                VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);
                Set<String> cohortNames = variantStatsWrapper.getCohortStats().keySet();
                checkAndUpdateCalculatedCohorts(studyConfiguration, cohortNames, updateStats);
            } else {
                throw new IOException("File " + uri + " is empty");
            }
        }
    }

    void checkAndUpdateCalculatedCohorts(StudyConfiguration studyConfiguration, Collection<String> cohorts, boolean updateStats)
            throws IOException {
        for (String cohortName : cohorts) {
//            if (cohortName.equals(VariantSourceEntry.DEFAULT_COHORT)) {
//                continue;
//            }
            Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
            if (studyConfiguration.getInvalidStats().contains(cohortId)) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
                logger.debug("Cohort \"" + cohortName + "\" stats calculated and INVALID. Set as calculated");
                studyConfiguration.getInvalidStats().remove(cohortId);
            }
            if (studyConfiguration.getCalculatedStats().contains(cohortId)) {
                if (!updateStats) {
                    throw new IOException("Cohort \"" + cohortName + "\" stats already calculated");
                }
            } else {
                studyConfiguration.getCalculatedStats().add(cohortId);
            }
        }
    }

}
