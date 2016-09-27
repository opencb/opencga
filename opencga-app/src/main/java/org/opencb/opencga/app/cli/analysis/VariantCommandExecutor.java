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

package org.opencb.opencga.app.cli.analysis;


import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsPopulationFrequencyExporter;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsTsvExporter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.variant.VariantFetcher;
import org.opencb.opencga.analysis.storage.variant.VariantStorage;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.analysis.variant.VariantFileIndexer;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.monitor.daemons.IndexDaemon;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporter;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroWriter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.opencb.opencga.analysis.variant.VariantFileIndexer.LOAD;
import static org.opencb.opencga.analysis.variant.VariantFileIndexer.TRANSFORM;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends AnalysisStorageCommandExecutor {

    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantStorageManager variantStorageManager;

    public VariantCommandExecutor(AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = variantCommandOptions.getParsedSubCommand();
        configure();

        sessionId = getSessionId(variantCommandOptions.commonOptions);

        switch (subCommandString) {
            case "ibs":
                ibs();
                break;
            case "delete":
                delete();
                break;
            case "query":
                query();
                break;
            case "export-frequencies":
                exportFrequencies();
                break;
            case "index":
                index();
                break;
            case "stats":
                stats();
                break;
            case "annotate":
                annotate();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }


    private VariantStorageManager initVariantStorageManager(DataStore dataStore)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        String storageEngine = dataStore.getStorageEngine();
        if (isEmpty(storageEngine)) {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
        return variantStorageManager;
    }


    private void ibs() {
        throw new UnsupportedOperationException();
    }


    private void exportFrequencies() throws Exception {

        AnalysisCliOptionsParser.ExportVariantStatsCommandOptions exportCliOptions = variantCommandOptions.exportVariantStatsCommandOptions;
        AnalysisCliOptionsParser.QueryVariantCommandOptions queryCliOptions = variantCommandOptions.queryVariantCommandOptions;

        queryCliOptions.outputFormat = exportCliOptions.outputFormat.toLowerCase().replace("tsv", "stats");
        queryCliOptions.study = exportCliOptions.studies;
        queryCliOptions.returnStudy = exportCliOptions.studies;
        queryCliOptions.limit = exportCliOptions.queryOptions.limit;
        queryCliOptions.sort = true;
        queryCliOptions.skip = exportCliOptions.queryOptions.skip;
        queryCliOptions.region = exportCliOptions.queryOptions.region;
        queryCliOptions.regionFile = exportCliOptions.queryOptions.regionFile;
        queryCliOptions.output = exportCliOptions.queryOptions.output;
        queryCliOptions.gene = exportCliOptions.queryOptions.gene;
        queryCliOptions.count = exportCliOptions.queryOptions.count;
        queryCliOptions.returnSample = "";

        query();
    }

    private void query() throws Exception {

        AnalysisCliOptionsParser.QueryVariantCommandOptions cliOptions = variantCommandOptions.queryVariantCommandOptions;

        Map<Long, String> studyIds = getStudyIds(sessionId);
        Query query = VariantQueryCommandUtils.parseQuery(cliOptions, studyIds);
        QueryOptions queryOptions = VariantQueryCommandUtils.parseQueryOptions(cliOptions);

        VariantFetcher variantFetcher = new VariantFetcher(catalogManager, storageManagerFactory);

        if (cliOptions.count) {
            QueryResult<Long> result = variantFetcher.count(query, sessionId);
            System.out.println("Num. results\t" + result.getResult().get(0));
        } else if (StringUtils.isNotEmpty(cliOptions.groupBy)) {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResult groupBy = variantFetcher.groupBy(query, queryOptions, cliOptions.groupBy, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
        } else if (StringUtils.isNotEmpty(cliOptions.rank)) {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResult rank = variantFetcher.rank(query, queryOptions, cliOptions.rank, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
        } else {
            final String outputFormat;
            if (StringUtils.isNotEmpty(cliOptions.outputFormat)) {
                outputFormat = cliOptions.outputFormat.toLowerCase();
            } else {
                outputFormat = "vcf";
            }

            try (OutputStream outputStream = VariantQueryCommandUtils.getOutputStream(cliOptions);
                 VariantDBIterator iterator = variantFetcher.iterator(query, queryOptions, sessionId)) {

                StudyConfiguration studyConfiguration;
                final DataWriter<Variant> exporter;
                switch (VariantQueryCommandUtils.VariantOutputFormat.safeValueOf(outputFormat)) {
                    case VCF:
//                StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
//                Map<Long, List<Sample>> samplesMetadata = variantFetcher.getSamplesMetadata(studyId, query, queryOptions, sessionId);
//                QueryResult<StudyConfiguration> studyConfigurationResult = studyConfigurationManager.getStudyConfiguration(
//                        query.getAsStringList(RETURNED_STUDIES.key()).get(0), null);
                        studyConfiguration = variantFetcher
                                .getStudyConfiguration(query.getAsIntegerList(RETURNED_STUDIES.key()).get(0), null, sessionId);
                        if (studyConfiguration != null) {
                            // Samples to be returned
                            if (query.containsKey(RETURNED_SAMPLES.key())) {
                                queryOptions.put(RETURNED_SAMPLES.key(), query.get(RETURNED_SAMPLES.key()));
                            }

//                        options.add("includeAnnotations", queryVariantsCommandOptions.includeAnnotations);
                            if (cliOptions.annotations != null) {
                                queryOptions.add("annotations", cliOptions.annotations);
                            }
//                            VariantVcfExporter.htsExport(iterator, studyConfiguration, outputStream, queryOptions);
                            long studyId = variantFetcher.getMainStudyId(query);
                            VariantSourceDBAdaptor sourceDBAdaptor = variantFetcher.getSourceDBAdaptor((int) studyId, sessionId);
                            exporter = new VariantVcfExporter(studyConfiguration, sourceDBAdaptor, outputStream, queryOptions);
                        } else {
                            throw new IllegalArgumentException("No study found named " + query.getAsStringList(RETURNED_STUDIES.key()).get(0));
                        }
                        break;
                    case JSON:
                        // we know that it is JSON, otherwise we have not reached this point
                        exporter = batch -> {
                            batch.forEach(variant -> {
                                try {
                                    outputStream.write(variant.toJson().getBytes());
                                    outputStream.write('\n');
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                            return true;
                        };

                        break;
                    case AVRO:
                        String codecName = "";
                        if (VariantQueryCommandUtils.VariantOutputFormat.isGzip(outputFormat)) {
                            codecName = "gzip";
                        }
                        if (outputFormat.endsWith("snappy")) {
                            codecName = "snappy";
                        }
                        exporter = new VariantAvroWriter(VariantAvro.getClassSchema(), codecName, outputStream);

                        break;
                    case STATS:
                        studyConfiguration = variantFetcher
                                .getStudyConfiguration(query.getAsIntegerList(RETURNED_STUDIES.key()).get(0), null, sessionId);
                        List<String> cohorts = new ArrayList<>(studyConfiguration.getCohortIds().keySet());
                        cohorts.sort(String::compareTo);

                        exporter = new VariantStatsTsvExporter(outputStream, studyConfiguration.getStudyName(), cohorts);

                        break;
                    case CELLBASE:
                        exporter = new VariantStatsPopulationFrequencyExporter(outputStream);
                        break;
                    default:
                        throw new ParameterException("Unknown output format " + outputFormat);
                }

                ParallelTaskRunner.Task<Variant, Variant> progressTask;
                ExecutorService executor;
                if (VariantQueryCommandUtils.isStandardOutput(cliOptions)) {
                    progressTask = batch -> batch;
                    executor = null;
                } else {
                    executor = Executors.newSingleThreadExecutor();
                    Future<Long> future = executor.submit(() -> {
                        Long count = variantFetcher.count(query, sessionId).first();
                        count = Math.min(queryOptions.getLong(QueryOptions.LIMIT, Long.MAX_VALUE), count - queryOptions.getLong(QueryOptions.SKIP, 0));
                        return count;
                    });
                    executor.shutdown();
                    ProgressLogger progressLogger = new ProgressLogger("Export variants", future, 200);
                    progressTask = batch -> {
                        progressLogger.increment(batch.size());
                        return batch;
                    };
                }
                ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                        .setNumTasks(1)
                        .setBatchSize(10)
                        .setAbortOnFail(true)
                        .build();
                ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>(batchSize -> {
                    List<Variant> variants = new ArrayList<>(batchSize);
                    while (iterator.hasNext() && variants.size() < batchSize) {
                        variants.add(iterator.next());
                    }
                    return variants;
                }, progressTask, exporter, config);

                ptr.run();
                if (executor != null) {
                    executor.shutdownNow();
                }
                logger.info("Time fetching data: " + iterator.getTimeFetching(TimeUnit.MILLISECONDS) / 1000.0 + "s");
                logger.info("Time converting data: " + iterator.getTimeConverting(TimeUnit.MILLISECONDS) / 1000.0 + "s");

            }
        }
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageManagerException,
            InstantiationException, IllegalAccessException, URISyntaxException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LOAD, variantCommandOptions.indexVariantCommandOptions.load);
        queryOptions.put(TRANSFORM, variantCommandOptions.indexVariantCommandOptions.transform);

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        queryOptions.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
        queryOptions.put(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
        queryOptions.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);

        queryOptions.putIfNotNull(VariantFileIndexer.CATALOG_PATH, cliOptions.catalogPath);
        queryOptions.putIfNotNull(VariantFileIndexer.TRANSFORMED_FILES, cliOptions.transformedPaths);

        queryOptions.put(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate);
        if (cliOptions.annotator != null) {
            queryOptions.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        queryOptions.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);
        /*
        *         // 2) Read and validate cli args. Configure options
        ObjectMap options = storageConfiguration.getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyId);
        // Use the INDEXED_FILE_ID instead of the given fileID. It may be the transformed file.
        options.put(VariantStorageManager.Options.FILE_ID.key(), job.getAttributes().get(Job.INDEXED_FILE_ID));
        options.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        options.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
        options.put(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
        options.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);

        options.put(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate);
        if (cliOptions.annotator != null) {
            options.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        options.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);
        * */

        VariantFileIndexer variantFileIndexer = new VariantFileIndexer(catalogConfiguration, storageConfiguration);
        variantFileIndexer.index(cliOptions.fileId, cliOptions.outdir, sessionId, queryOptions);

//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);
//
//        // 1) Create, if not provided, an indexation job
//        if (isEmpty(cliOptions.job.jobId)) {
//            Job job;
//            long outDirId;
//            if (cliOptions.outdir == null) {
//                outDirId = catalogManager.getFileParent(inputFileId, null, sessionId).first().getId();
//            } else  {
//                outDirId = catalogManager.getFileId(cliOptions.outdir);
//            }
//
//            AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
//
//            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
//                    .stream()
//                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
//                    .collect(Collectors.toList());
//
//            QueryOptions options = new QueryOptions()
//                    .append(ExecutorManager.EXECUTE, !cliOptions.job.queue)
//                    .append(ExecutorManager.SIMULATE, false)
//                    .append(AnalysisFileIndexer.TRANSFORM, cliOptions.transform)
//                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
//                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
//                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats)
//                    .append(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate)
//                    .append(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated)
//                    .append(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields)
//                    .append(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype)
//                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel);
//
//            QueryResult<Job> result = analysisFileIndexer.index(inputFileId, outDirId, sessionId, options);
//            if (cliOptions.job.queue) {
//                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
//            }
//
//        } else {
//            long studyId = catalogManager.getStudyIdByFileId(inputFileId);
//            index(getJob(studyId, cliOptions.job.jobId, sessionId));
//        }
    }

//    /**
//     * Index a variant file.
//     *
//     * steps:
//     * 1) Initialize VariantStorageManager
//     * 2) Read and validate cli args. Configure options
//     * 3) Execute indexation
//     * 4) Save indexation result
//     *
//     * @throws CatalogException
//     * @throws IllegalAccessException
//     * @throws ClassNotFoundException
//     * @throws InstantiationException
//     * @throws StorageManagerException
//     */
//    private void index(Job job) throws CatalogException, IllegalAccessException, ClassNotFoundException, InstantiationException,
//            StorageManagerException {
//        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;
//
//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);
//
//        // 1) Initialize VariantStorageManager
//        long studyId = catalogManager.getStudyIdByFileId(inputFileId);
//        Study study = catalogManager.getStudy(studyId, sessionId).first();
//
//        /*
//         * Getting VariantStorageManager
//         * We need to find out the Storage Engine Id to be used from Catalog
//         */
//        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
//        initVariantStorageManager(dataStore);
//
//        // 2) Read and validate cli args. Configure options
//        ObjectMap options = storageConfiguration.getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
//        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
//        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyId);
//        // Use the INDEXED_FILE_ID instead of the given fileID. It may be the transformed file.
//        options.put(VariantStorageManager.Options.FILE_ID.key(), job.getAttributes().get(Job.INDEXED_FILE_ID));
//        options.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
//        options.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
//        options.put(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
//        options.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);
//
//        options.put(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate);
//        if (cliOptions.annotator != null) {
//            options.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
//        }
//        options.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);
//
////        if (cliOptions.aggregationMappingFile != null) {
////            // TODO move this options to new configuration.yml
////            Properties aggregationMappingProperties = new Properties();
////            try {
////                aggregationMappingProperties.load(new FileInputStream(cliOptions.aggregationMappingFile));
////                options.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
////            } catch (FileNotFoundException e) {
////                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", cliOptions
////                        .aggregationMappingFile);
////            }
////        }
//
//        if (cliOptions.commonOptions.params != null) {
//            options.putAll(cliOptions.commonOptions.params);
//        }
//
//        final boolean doExtract;
//        final boolean doTransform;
//        final boolean doLoad;
//        StorageETLResult storageETLResult = null;
//        Exception exception = null;
//
//        if (!cliOptions.load && !cliOptions.transform) {
//            doExtract = true;
//            doTransform = true;
//            doLoad = true;
//        } else {
//            doExtract = cliOptions.transform;
//            doTransform = cliOptions.transform;
//            doLoad = cliOptions.load;
//        }
//
//        // 3) Execute indexation
//        try {
//            File file = catalogManager.getFile(inputFileId, sessionId).first();
//            URI fileUri = catalogManager.getFileUri(file);
//            storageETLResult = variantStorageManager.index(Collections.singletonList(fileUri), job.getTmpOutDirUri(),
//                    doExtract, doTransform, doLoad).get(0);
//        } catch (StorageETLException e) {
//            storageETLResult = e.getResults().get(0);
//            exception = e;
//            e.printStackTrace();
//            throw e;
//        } catch (Exception e) {
//            exception = e;
//            e.printStackTrace();
//            throw e;
//        } finally {
//            // 4) Save indexation result.
//            // TODO: Uncomment this line
////            new ExecutionOutputRecorder(catalogManager, sessionId).saveStorageResult(job, storageETLResult);
//        }
//    }

    private void stats() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageManagerException, InstantiationException, IllegalAccessException, URISyntaxException {
        AnalysisCliOptionsParser.StatsVariantCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        long studyId = catalogManager.getStudyId(cliOptions.studyId, sessionId);
        VariantStorage variantStorage = new VariantStorage(catalogManager);

        QueryOptions options = new QueryOptions()
                .append(VariantStatisticsManager.OUTPUT_FILE_NAME, cliOptions.fileName)
//                .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                .append(AnalysisFileIndexer.LOAD, cliOptions.load)
//                .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel) // unused
                .append(VariantStorageManager.Options.UPDATE_STATS.key(), cliOptions.updateStats)
                .append(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated)
                .append(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.aggregationMappingFile);
        options.putIfNotEmpty(VariantStorageManager.Options.FILE_ID.key(), cliOptions.fileId);

        options.putAll(cliOptions.commonOptions.params);

        List<Long> cohortIds = new LinkedList<>();
        if (StringUtils.isNotBlank(cliOptions.cohortIds)) {
            for (String cohort : cliOptions.cohortIds.split(",")) {
                if (StringUtils.isNumeric(cohort)) {
                    cohortIds.add(Long.parseLong(cohort));
                } else {
                    QueryResult<Cohort> result = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), cohort),
                            new QueryOptions("include", "projects.studies.cohorts.id"), sessionId);
                    if (result.getResult().isEmpty()) {
                        throw new CatalogException("Cohort \"" + cohort + "\" not found!");
                    } else {
                        cohortIds.add(result.first().getId());
                    }
                }
            }
        }
        variantStorage.calculateStats(studyId, cohortIds, cliOptions.catalogPath, cliOptions.outdir, sessionId, options);
//        QueryResult<Job> result = variantStorage.calculateStats(outDirId, cohortIds, sessionId, options);
//        if (cliOptions.job.queue) {
//            System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
//        }

    }

    @Deprecated
    private void stats(Job job)
            throws ClassNotFoundException, InstantiationException, CatalogException, IllegalAccessException, IOException, StorageManagerException {
        AnalysisCliOptionsParser.StatsVariantCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyId(cliOptions.studyId, sessionId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        initVariantStorageManager(dataStore);


        /*
         * Create DBAdaptor
         */
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(dataStore.getDbName());
        StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration((int) studyId, null).first();

        /*
         * Parse Options
         */
        ObjectMap options = storageConfiguration.getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());

        options.put(VariantStorageManager.Options.OVERWRITE_STATS.key(), cliOptions.overwriteStats);
        options.put(VariantStorageManager.Options.UPDATE_STATS.key(), cliOptions.updateStats);
        if (cliOptions.region != null) {
            options.putIfNotNull(VariantDBAdaptor.VariantQueryParams.REGION.key(), cliOptions.region);
        }
        long fileId = catalogManager.getFileId(cliOptions.fileId, sessionId);
        if (fileId != 0) {
            options.put(VariantStorageManager.Options.FILE_ID.key(), fileId);
        }
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyId);

        if (cliOptions.commonOptions.params != null) {
            options.putAll(cliOptions.commonOptions.params);
        }

        Map<String, Integer> cohortIds = new HashMap<>();
        Map<String, Set<String>> cohorts = new HashMap<>();

        Properties aggregationMappingProperties = null;
        if (isNotEmpty(cliOptions.aggregationMappingFile)) {
            aggregationMappingProperties = new Properties();
            try (InputStream is = new FileInputStream(cliOptions.aggregationMappingFile)){
                aggregationMappingProperties.load(is);
                options.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", cliOptions
                        .aggregationMappingFile);
            }
        }

        List<String> cohortNames;
        if (isEmpty(cliOptions.cohortIds)) {
            if (aggregationMappingProperties == null) {
                throw new IllegalArgumentException("Missing cohorts");
            } else {
                cohortNames = new LinkedList<>(VariantAggregatedStatsCalculator.getCohorts(aggregationMappingProperties));
            }
        } else {
            cohortNames = Arrays.asList(cliOptions.cohortIds.split(","));
        }

        for (String cohort : cohortNames) {
            int cohortId;
            if (StringUtils.isNumeric(cohort)) {
                cohortId = Integer.parseInt(cohort);
            } else {
                if (studyConfiguration.getCohortIds().containsKey(cohort)) {
                    cohortId = studyConfiguration.getCohortIds().get(cohort);
                } else {
                    throw new IllegalArgumentException("Unknown cohort name " + cohort);
                }
            }
            Set<String> samples = studyConfiguration.getCohorts().get(cohortId)
                    .stream()
                    .map(sampleId -> studyConfiguration.getSampleIds().inverse().get(sampleId))
                    .collect(Collectors.toSet());
            cohorts.put(studyConfiguration.getCohortIds().inverse().get(cohortId), samples);
        }

        options.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);


        /*
         * Create and load stats
         */
//        URI outputUri = UriUtils.createUri(cliOptions.fileName == null ? "" : cliOptions.fileName);
//        URI outputUri = job.getTmpOutDirUri();
        URI outputUri = IndexDaemon.getJobTemporaryFolder(job.getId(), catalogConfiguration.getTempJobsDir()).toUri();
        String filename;
        if (isEmpty(cliOptions.fileName)) {
            filename = VariantStorageManager.buildFilename(studyConfiguration.getStudyName(), (int) fileId);
        } else {
            filename = cliOptions.fileName;
        }



//        assertDirectoryExists(directoryUri);
        VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();

        boolean doCreate = true;
        boolean doLoad = true;
//        doCreate = statsVariantsCommandOptions.create;
//        doLoad = statsVariantsCommandOptions.load != null;
//        if (!statsVariantsCommandOptions.create && statsVariantsCommandOptions.load == null) {
//            doCreate = doLoad = true;
//        } else if (statsVariantsCommandOptions.load != null) {
//            filename = statsVariantsCommandOptions.load;
//        }


        QueryOptions queryOptions = new QueryOptions(options);
        if (doCreate) {
            filename += "." + TimeUtils.getTime();
            outputUri = outputUri.resolve(filename);
            outputUri = variantStatisticsManager.createStats(dbAdaptor, outputUri, cohorts, cohortIds,
                    studyConfiguration, queryOptions);
        }

        if (doLoad) {
            outputUri = outputUri.resolve(filename);
            variantStatisticsManager.loadStats(dbAdaptor, outputUri, studyConfiguration, queryOptions);
        }
    }

    @Deprecated
    private void annotate() throws StorageManagerException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        AnalysisCliOptionsParser.AnnotateVariantCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;

        // 1) Create, if not provided, an indexation job
        if (isEmpty(cliOptions.job.jobId)) {
            Job job;
            long studyId = catalogManager.getStudyId(cliOptions.studyId, sessionId);
            long outDirId;
            if (isEmpty(cliOptions.outdirId)) {
                outDirId = catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(), ""), null, sessionId)
                        .first().getId();
            } else {
                outDirId = catalogManager.getFileId(cliOptions.outdirId, sessionId);
            }

            VariantStorage variantStorage = new VariantStorage(catalogManager);

            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.toList());

            QueryOptions options = new QueryOptions()
                    .append(ExecutorManager.EXECUTE, !cliOptions.job.queue)
                    .append(ExecutorManager.SIMULATE, false)
                    .append(AnalysisFileIndexer.CREATE, cliOptions.create)
                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel)
                    .append(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations)
                    .append(VariantAnnotationManager.FILE_NAME, cliOptions.fileName)
                    .append(VariantAnnotationManager.SPECIES, cliOptions.species)
                    .append(VariantAnnotationManager.ASSEMBLY, cliOptions.assembly)
                    .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, cliOptions.customAnnotationKey)
                    .append(VariantDBAdaptor.VariantQueryParams.REGION.key(), cliOptions.filterRegion)
                    .append(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), cliOptions.filterChromosome)
                    .append(VariantDBAdaptor.VariantQueryParams.GENE.key(), cliOptions.filterGene)
                    .append(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), cliOptions.filterAnnotConsequenceType);

            QueryResult<Job> result = variantStorage.annotateVariants(studyId, outDirId, sessionId, options);
            if (cliOptions.job.queue) {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
            }

        } else {
            long studyId = catalogManager.getStudyId(cliOptions.studyId, sessionId);
            String jobId = cliOptions.job.jobId;
            annotate(getJob(studyId, jobId, sessionId));
        }
    }

    @Deprecated
    private void annotate(Job job)
            throws StorageManagerException, IOException, VariantAnnotatorException, CatalogException, IllegalAccessException,
            ClassNotFoundException, InstantiationException {
        AnalysisCliOptionsParser.AnnotateVariantCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;

//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyId(cliOptions.studyId, sessionId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        initVariantStorageManager(dataStore);

        /*
         * Create DBAdaptor
         */
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(dataStore.getDbName());
        StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration((int) studyId, null).first();

        /*
         * Create Annotator
         */
        ObjectMap options = storageConfiguration.getStorageEngine(dataStore.getStorageEngine()).getVariant().getOptions();
        if (cliOptions.annotator != null) {
            options.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        if (cliOptions.species != null) {
            options.put(VariantAnnotationManager.SPECIES, cliOptions.species);
        }
        if (cliOptions.assembly != null) {
            options.put(VariantAnnotationManager.ASSEMBLY, cliOptions.assembly);
        }
        if (cliOptions.customAnnotationKey != null) {
            options.put(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, cliOptions.customAnnotationKey);
        }

        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(storageConfiguration, dataStore.getStorageEngine(), options);
//            VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(annotatorSource, annotatorProperties,
// cliOptions.species, cliOptions.assembly);
        VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(annotator, dbAdaptor);

        /*
         * Annotation options
         */
        Query query = new Query();
        if (cliOptions.filterRegion != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), cliOptions.filterRegion);
        }
        if (cliOptions.filterChromosome != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), cliOptions.filterChromosome);
        }
        if (cliOptions.filterGene != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), cliOptions.filterGene);
        }
        if (cliOptions.filterAnnotConsequenceType != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(),
                    cliOptions.filterAnnotConsequenceType);
        }
        if (!cliOptions.overwriteAnnotations) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
        }
//        URI outputUri = job.getTmpOutDirUri();
        URI outputUri = IndexDaemon.getJobTemporaryFolder(job.getId(), catalogConfiguration.getTempJobsDir()).toUri();
        Path outDir = Paths.get(outputUri.resolve(".").getPath());

        /*
         * Create and load annotations
         */
        boolean doCreate = cliOptions.create, doLoad = cliOptions.load != null;
        if (!cliOptions.create && cliOptions.load == null) {
            doCreate = true;
            doLoad = true;
        }

        URI annotationFile = null;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation ");
            annotationFile = variantAnnotationManager.createAnnotation(outDir, cliOptions.fileName == null
                    ? dataStore.getDbName()
                    : cliOptions.fileName, query, new QueryOptions(options));
            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            if (annotationFile == null) {
                long fileId = catalogManager.getFileId(cliOptions.load, sessionId);
                annotationFile = catalogManager.getFileUri(catalogManager.getFile(fileId, sessionId).first());
            }
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions(options));
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }
    }

}
