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


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.variant.VariantFetcher;
import org.opencb.opencga.analysis.storage.variant.VariantStorage;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
        if (StringUtils.isEmpty(storageEngine)) {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
        return variantStorageManager;
    }


    private void ibs() {
        throw new UnsupportedOperationException();
    }

    private void query() throws Exception {

        AnalysisCliOptionsParser.QueryVariantCommandOptions cliOptions = variantCommandOptions.queryVariantCommandOptions;

        String sessionId = variantCommandOptions.commonOptions.sessionId;

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
            String outputFormat = "vcf";
            if (StringUtils.isNotEmpty(cliOptions.outputFormat)) {
                if (cliOptions.outputFormat.equals("json") || cliOptions.outputFormat.equals("json.gz")) {
                    outputFormat = "json";
                }
            }
            OutputStream outputStream = VariantQueryCommandUtils.getOutputStream(cliOptions);
            VariantDBIterator iterator = variantFetcher.iterator(query, queryOptions, sessionId);
            if (outputFormat.equalsIgnoreCase("vcf")) {
//                StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
//                Map<Long, List<Sample>> samplesMetadata = variantFetcher.getSamplesMetadata(studyId, query, queryOptions, sessionId);
//                QueryResult<StudyConfiguration> studyConfigurationResult = studyConfigurationManager.getStudyConfiguration(
//                        query.getAsStringList(RETURNED_STUDIES.key()).get(0), null);
                StudyConfiguration studyConfiguration = variantFetcher
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
                    VariantVcfExporter variantVcfExporter = new VariantVcfExporter();
                    variantVcfExporter.export(iterator, studyConfiguration, outputStream, queryOptions);
                } else {
                    logger.warn("no study found named " + query.getAsStringList(RETURNED_STUDIES.key()).get(0));
                }
            } else {
                // we know that it is JSON, otherwise we have not reached this point
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    outputStream.write(variant.toJson().getBytes());
                    outputStream.write('\n');
                }
            }
            iterator.close();

        }

    }


    private void delete() {
        throw new UnsupportedOperationException();
    }

    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageManagerException, InstantiationException, IllegalAccessException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        String sessionId = variantCommandOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Create, if not provided, an indexation job
        if (StringUtils.isEmpty(cliOptions.job.jobId)) {
            Job job;
            long outDirId;
            if (cliOptions.outdirId == null) {
                outDirId = catalogManager.getFileParent(inputFileId, null, sessionId).first().getId();
            } else  {
                outDirId = catalogManager.getFileId(cliOptions.outdirId);
            }

            AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.toList());

            QueryOptions options = new QueryOptions()
                    .append(AnalysisJobExecutor.EXECUTE, !cliOptions.job.queue)
                    .append(AnalysisJobExecutor.SIMULATE, false)
                    .append(AnalysisFileIndexer.TRANSFORM, cliOptions.transform)
                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate)
                    .append(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated)
                    .append(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields)
                    .append(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype)
                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel);

            QueryResult<Job> result = analysisFileIndexer.index(inputFileId, outDirId, sessionId, options);
            if (cliOptions.job.queue) {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
            }

        } else {
            long studyId = catalogManager.getStudyIdByFileId(inputFileId);
            index(getJob(studyId, cliOptions.job.jobId, sessionId));
        }
    }

    /**
     * Index a variant file.
     *
     * steps:
     * 1) Initialize VariantStorageManager
     * 2) Read and validate cli args. Configure options
     * 3) Execute indexation
     * 4) Save indexation result
     *
     * @throws CatalogException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws StorageManagerException
     */
    private void index(Job job)
            throws CatalogException, IllegalAccessException, ClassNotFoundException,
            InstantiationException, StorageManagerException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);


        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyIdByFileId(inputFileId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        initVariantStorageManager(dataStore);

        // 2) Read and validate cli args. Configure options
        ObjectMap options = storageConfiguration.getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyId);
        // Use the INDEXED_FILE_ID instead of the given fileID. It may be the transformed file.
        options.put(VariantStorageManager.Options.FILE_ID.key(), job.getAttributes().containsKey(Job.INDEXED_FILE_ID));
        options.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        options.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
        options.put(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
        options.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);

        options.put(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate);
        if (cliOptions.annotator != null) {
            options.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        options.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);

//        if (cliOptions.aggregationMappingFile != null) {
//            // TODO move this options to new configuration.yml
//            Properties aggregationMappingProperties = new Properties();
//            try {
//                aggregationMappingProperties.load(new FileInputStream(cliOptions.aggregationMappingFile));
//                options.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
//            } catch (FileNotFoundException e) {
//                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", cliOptions
//                        .aggregationMappingFile);
//            }
//        }

        if (cliOptions.commonOptions.params != null) {
            options.putAll(cliOptions.commonOptions.params);
        }

        final boolean doExtract;
        final boolean doTransform;
        final boolean doLoad;
        StorageETLResult storageETLResult = null;
        Exception exception = null;

        if (!cliOptions.load && !cliOptions.transform) {
            doExtract = true;
            doTransform = true;
            doLoad = true;
        } else {
            doExtract = cliOptions.transform;
            doTransform = cliOptions.transform;
            doLoad = cliOptions.load;
        }

        // 3) Execute indexation
        try {
            File file = catalogManager.getFile(inputFileId, sessionId).first();
            URI fileUri = catalogManager.getFileUri(file);
            storageETLResult = variantStorageManager.index(Collections.singletonList(fileUri), job.getTmpOutDirUri(),
                    doExtract, doTransform, doLoad).get(0);
        } catch (StorageETLException e) {
            storageETLResult = e.getResults().get(0);
            exception = e;
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            exception = e;
            e.printStackTrace();
            throw e;
        } finally {
            // 4) Save indexation result.
            new AnalysisOutputRecorder(catalogManager, sessionId).saveStorageResult(job, storageETLResult);
        }
    }

    private void stats() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageManagerException, InstantiationException, IllegalAccessException {
        AnalysisCliOptionsParser.StatsVariantCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;

        // 1) Create, if not provided, an indexation job
        if (StringUtils.isEmpty(cliOptions.job.jobId)) {
            Job job;
            long studyId = catalogManager.getStudyId(cliOptions.studyId);
            long outDirId;
            if (StringUtils.isEmpty(cliOptions.outdirId)) {
                outDirId = catalogManager.getAllFiles(studyId, new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), ""), null, sessionId).first().getId();
            } else {
                outDirId = catalogManager.getFileId(cliOptions.outdirId);
            }
            VariantStorage variantStorage = new VariantStorage(catalogManager);

            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.toList());

            QueryOptions options = new QueryOptions()
                    .append(AnalysisJobExecutor.EXECUTE, !cliOptions.job.queue)
                    .append(AnalysisJobExecutor.SIMULATE, false)
//                    .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel)
                    .append(VariantStorageManager.Options.UPDATE_STATS.key(), cliOptions.updateStats)
                    .append(VariantStorageManager.Options.FILE_ID.key(), cliOptions.fileId)
                    .append(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.aggregationMappingFile);


            List<Long> cohortIds = new LinkedList<>();
            for (String cohort : cliOptions.cohortIds.split(",")) {
                if (StringUtils.isNumeric(cohort)) {
                    cohortIds.add(Long.parseLong(cohort));
                } else {
                    QueryResult<Cohort> result = catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), cohort),
                            new QueryOptions("include", "projects.studies.cohorts.id"), sessionId);
                    if (result.getResult().isEmpty()) {
                        throw new CatalogException("Cohort \"" + cohort + "\" not found!");
                    } else {
                        cohortIds.add(result.first().getId());
                    }
                }
            }
            QueryResult<Job> result = variantStorage.calculateStats(outDirId, cohortIds, sessionId, options);
            if (cliOptions.job.queue) {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
            }
        } else {
            long studyId = catalogManager.getStudyId(cliOptions.studyId);
            stats(getJob(studyId, cliOptions.job.jobId, sessionId));
        }
    }

    private void stats(Job job)
            throws ClassNotFoundException, InstantiationException, CatalogException, IllegalAccessException, IOException, StorageManagerException {
        AnalysisCliOptionsParser.StatsVariantCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        String sessionId = variantCommandOptions.commonOptions.sessionId;
//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);



        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyId(cliOptions.studyId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
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
        if (cliOptions.fileId != 0) {
            options.put(VariantStorageManager.Options.FILE_ID.key(), cliOptions.fileId);
        }
        options.put(VariantStorageManager.Options.STUDY_ID.key(), cliOptions.studyId);

        if (cliOptions.commonOptions.params != null) {
            options.putAll(cliOptions.commonOptions.params);
        }

        Map<String, Integer> cohortIds = new HashMap<>();
        Map<String, Set<String>> cohorts = new HashMap<>();
        for (String cohort : cliOptions.cohortIds.split(",")) {
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

        if (cliOptions.aggregationMappingFile != null) {
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(cliOptions.aggregationMappingFile));
                options.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", cliOptions
                        .aggregationMappingFile);
            }
        }

        /*
         * Create and load stats
         */
//        URI outputUri = UriUtils.createUri(cliOptions.fileName == null ? "" : cliOptions.fileName);
        URI outputUri = job.getTmpOutDirUri();
        String filename;
        if (StringUtils.isEmpty(cliOptions.fileName)) {
            filename = VariantStorageManager.buildFilename(studyConfiguration.getStudyName(), cliOptions.fileId);
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


    private void annotate() throws StorageManagerException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException, AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        AnalysisCliOptionsParser.AnnotateVariantCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;

        String sessionId = variantCommandOptions.commonOptions.sessionId;

        // 1) Create, if not provided, an indexation job
        if (StringUtils.isEmpty(cliOptions.job.jobId)) {
            Job job;
            long studyId = catalogManager.getStudyId(cliOptions.studyId);
            long outDirId;
            if (StringUtils.isEmpty(cliOptions.outdirId)) {
                outDirId = catalogManager.getAllFiles(studyId, new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), ""), null, sessionId).first().getId();
            } else {
                outDirId = catalogManager.getFileId(cliOptions.outdirId);
            }

            VariantStorage variantStorage = new VariantStorage(catalogManager);

            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.toList());

            QueryOptions options = new QueryOptions()
                    .append(AnalysisJobExecutor.EXECUTE, !cliOptions.job.queue)
                    .append(AnalysisJobExecutor.SIMULATE, false)
//                    .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel)
                    .append(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations)
                    .append(VariantAnnotationManager.FILE_NAME, cliOptions.fileName)
                    .append(VariantAnnotationManager.SPECIES, cliOptions.species)
                    .append(VariantAnnotationManager.ASSEMBLY, cliOptions.assembly)
                    .append(VariantDBAdaptor.VariantQueryParams.REGION.key(), cliOptions.filterRegion)
                    .append(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), cliOptions.filterChromosome)
                    .append(VariantDBAdaptor.VariantQueryParams.GENE.key(), cliOptions.filterGene)
                    .append(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), cliOptions.filterAnnotConsequenceType);

            QueryResult<Job> result = variantStorage.annotateVariants(studyId, outDirId, sessionId, options);
            if (cliOptions.job.queue) {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
            }

        } else {
            long studyId = catalogManager.getStudyId(cliOptions.studyId);
            String jobId = cliOptions.job.jobId;
            annotate(getJob(studyId, jobId, sessionId));
        }
    }

    private void annotate(Job job)
            throws StorageManagerException, IOException, VariantAnnotatorException, CatalogException, IllegalAccessException,
            ClassNotFoundException, InstantiationException {
        AnalysisCliOptionsParser.AnnotateVariantCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;
//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);



        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyId(cliOptions.studyId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
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

        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(storageConfiguration, dataStore.getStorageEngine());
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
        URI outputUri = job.getTmpOutDirUri();
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
                    : cliOptions.fileName, query, new QueryOptions());
            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            if (annotationFile == null) {
//                annotationFile = new URI(null, c.load, null);
                annotationFile = Paths.get(cliOptions.load).toUri();
            }
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions());
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }
    }

}
