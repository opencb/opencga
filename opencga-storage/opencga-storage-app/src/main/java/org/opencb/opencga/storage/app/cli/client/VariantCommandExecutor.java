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

package org.opencb.opencga.storage.app.cli.client;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.OptionsParser;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.benchmark.BenchmarkManager;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.server.grpc.GenericServiceModel;
import org.opencb.opencga.storage.server.grpc.VariantProto;
import org.opencb.opencga.storage.server.grpc.VariantServiceGrpc;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private StorageEngineConfiguration storageConfiguration;
    private VariantStorageManager variantStorageManager;

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    private void configure(OptionsParser.CommonOptions commonOptions) throws Exception {

        this.logFile = commonOptions.logFile;

        /**
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used
         * If not storage engine is passed then the default is taken from storage-configuration.yml file
         **/
        this.storageEngine = (storageEngine != null && !storageEngine.isEmpty())
                ? storageEngine
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", this.storageEngine);

        this.storageConfiguration = configuration.getStorageEngine(storageEngine);

        StorageManagerFactory storageManagerFactory = StorageManagerFactory.get(configuration);
        if (storageEngine == null || storageEngine.isEmpty()) {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = variantCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "index":
                configure(variantCommandOptions.indexVariantsCommandOptions.commonOptions);
                index();
                break;
            case "query":
                configure(variantCommandOptions.queryVariantsCommandOptions.commonOptions);
                query();
                break;
            case "query-grpc":
                configure(variantCommandOptions.queryVariantsCommandOptions.commonOptions);
                queryGrpc();
                break;
            case "annotation":
                configure(variantCommandOptions.annotateVariantsCommandOptions.commonOptions);
                annotation();
                break;
            case "stats":
                configure(variantCommandOptions.statsVariantsCommandOptions.commonOptions);
                stats();
                break;
            case "benchmark":
                configure(variantCommandOptions.statsVariantsCommandOptions.commonOptions);
                benchmark();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void index() throws URISyntaxException, IOException, StorageManagerException, FileFormatException {
        CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions = variantCommandOptions.indexVariantsCommandOptions;
        List<URI> inputUris = new LinkedList<>();
        for (String uri : indexVariantsCommandOptions.input) {
            URI variantsUri = UriUtils.createUri(uri);
            if (variantsUri.getScheme().startsWith("file") || variantsUri.getScheme().isEmpty()) {
                FileUtils.checkFile(Paths.get(variantsUri));
            }
            inputUris.add(variantsUri);
        }

        URI pedigreeUri = (indexVariantsCommandOptions.pedigree != null && !indexVariantsCommandOptions.pedigree.isEmpty())
                ? UriUtils.createUri(indexVariantsCommandOptions.pedigree)
                : null;
        if (pedigreeUri != null) {
            FileUtils.checkFile(Paths.get(pedigreeUri));
        }

        URI outdirUri = (indexVariantsCommandOptions.outdir != null && !indexVariantsCommandOptions.outdir.isEmpty())
                ? UriUtils.createDirectoryUri(indexVariantsCommandOptions.outdir)
                // Get parent folder from input file
                : inputUris.get(0).resolve(".");
        if (outdirUri.getScheme().startsWith("file") || outdirUri.getScheme().isEmpty()) {
            FileUtils.checkDirectory(Paths.get(outdirUri), true);
        }
        logger.debug("All files and directories exist");

//            VariantSource source = new VariantSource(fileName, indexVariantsCommandOptions.fileId,
//                    indexVariantsCommandOptions.studyId, indexVariantsCommandOptions.study, indexVariantsCommandOptions.studyType,
// indexVariantsCommandOptions.aggregated);

        /** Add CLi options to the variant options **/
        ObjectMap variantOptions = storageConfiguration.getVariant().getOptions();
        variantOptions.put(VariantStorageManager.Options.STUDY_NAME.key(), indexVariantsCommandOptions.study);
        variantOptions.put(VariantStorageManager.Options.STUDY_ID.key(), indexVariantsCommandOptions.studyId);
        variantOptions.put(VariantStorageManager.Options.FILE_ID.key(), indexVariantsCommandOptions.fileId);
        variantOptions.put(VariantStorageManager.Options.SAMPLE_IDS.key(), indexVariantsCommandOptions.sampleIds);
        variantOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), indexVariantsCommandOptions.calculateStats);
        variantOptions.put(VariantStorageManager.Options.INCLUDE_STATS.key(), indexVariantsCommandOptions.includeStats);
//        variantOptions.put(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), indexVariantsCommandOptions.includeGenotype);
        variantOptions.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), indexVariantsCommandOptions.extraFields);
//        variantOptions.put(VariantStorageManager.Options.INCLUDE_SRC.key(), indexVariantsCommandOptions.includeSrc);
//        variantOptions.put(VariantStorageManager.Options.COMPRESS_GENOTYPES.key(), indexVariantsCommandOptions.compressGenotypes);
        variantOptions.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), indexVariantsCommandOptions.aggregated);
        if (indexVariantsCommandOptions.dbName != null) {
            variantOptions.put(VariantStorageManager.Options.DB_NAME.key(), indexVariantsCommandOptions.dbName);
        }
        variantOptions.put(VariantStorageManager.Options.ANNOTATE.key(), indexVariantsCommandOptions.annotate);
        if (indexVariantsCommandOptions.annotator != null) {
            variantOptions.put(VariantAnnotationManager.ANNOTATION_SOURCE, indexVariantsCommandOptions.annotator);
        }
        variantOptions.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, indexVariantsCommandOptions.overwriteAnnotations);
        if (indexVariantsCommandOptions.studyConfigurationFile != null && !indexVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
            variantOptions.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, indexVariantsCommandOptions.studyConfigurationFile);
        }

        if (indexVariantsCommandOptions.aggregationMappingFile != null) {
            // TODO move this options to new configuration.yml
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(indexVariantsCommandOptions.aggregationMappingFile));
                variantOptions.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", indexVariantsCommandOptions
                        .aggregationMappingFile);
            }
        }

        if (indexVariantsCommandOptions.commonOptions.params != null) {
            variantOptions.putAll(indexVariantsCommandOptions.commonOptions.params);
        }
        logger.debug("Configuration options: {}", variantOptions.toJson());


        /** Execute ETL steps **/
        boolean doExtract, doTransform, doLoad;

        if (!indexVariantsCommandOptions.load && !indexVariantsCommandOptions.transform) {
            doExtract = true;
            doTransform = true;
            doLoad = true;
        } else {
            doExtract = indexVariantsCommandOptions.transform;
            doTransform = indexVariantsCommandOptions.transform;
            doLoad = indexVariantsCommandOptions.load;
        }

        variantStorageManager.index(inputUris, outdirUri, doExtract, doTransform, doLoad);

    }

    private void query() throws Exception {
        CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions = variantCommandOptions.queryVariantsCommandOptions;

        storageConfiguration.getVariant().getOptions().putAll(queryVariantsCommandOptions.commonOptions.params);

        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(queryVariantsCommandOptions.dbName);
        List<String> studyNames = variantDBAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions());

        Query query = VariantQueryCommandUtils.parseQuery(queryVariantsCommandOptions, studyNames);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(queryVariantsCommandOptions);
        OutputStream outputStream = VariantQueryCommandUtils.getOutputStream(queryVariantsCommandOptions);

        if (queryVariantsCommandOptions.count) {
            QueryResult<Long> result = variantDBAdaptor.count(query);
            System.out.println("Num. results\t" + result.getResult().get(0));
            return;
        }


        String outputFormat = "vcf";
        if (StringUtils.isNotEmpty(queryVariantsCommandOptions.outputFormat)) {
            if (queryVariantsCommandOptions.outputFormat.equals("json") || queryVariantsCommandOptions.outputFormat.equals("json.gz")) {
                outputFormat = "json";
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        if (StringUtils.isNotEmpty(queryVariantsCommandOptions.rank)) {
            executeRank(query, variantDBAdaptor, queryVariantsCommandOptions);
        } else {
            if (StringUtils.isNotEmpty(queryVariantsCommandOptions.groupBy)) {
                QueryResult groupBy = variantDBAdaptor.groupBy(query, queryVariantsCommandOptions.groupBy, options);
                System.out.println("groupBy = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
            } else {
                VariantDBIterator iterator = variantDBAdaptor.iterator(query, options);
                if (outputFormat.equalsIgnoreCase("vcf")) {
                    StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
                    QueryResult<StudyConfiguration> studyConfigurationResult = studyConfigurationManager.getStudyConfiguration(
                            query.getAsStringList(RETURNED_STUDIES.key()).get(0), null);
                    if (studyConfigurationResult.getResult().size() >= 1) {
                        // Samples to be returned
                        if (query.containsKey(RETURNED_SAMPLES.key())) {
                            options.put(RETURNED_SAMPLES.key(), query.get(RETURNED_SAMPLES.key()));
                        }

//                        options.add("includeAnnotations", queryVariantsCommandOptions.includeAnnotations);
                        if (queryVariantsCommandOptions.annotations != null) {
                            options.add("annotations", queryVariantsCommandOptions.annotations);
                        }
                        VariantVcfExporter.htsExport(iterator, studyConfigurationResult.first(),
                                variantDBAdaptor.getVariantSourceDBAdaptor(), outputStream, options);
                    } else {
                        logger.warn("no study found named " + query.getAsStringList(RETURNED_STUDIES.key()).get(0));
                    }
//                    printVcfResult(iterator, studyConfigurationManager, printWriter);
                } else {
                    // we know that it is JSON, otherwise we have not reached this point
                    printJsonResult(iterator, outputStream);
                }
                iterator.close();
            }
        }
        outputStream.close();
    }

    private void queryGrpc() throws Exception {
        CliOptionsParser.QueryGrpCVariantsCommandOptions queryGrpcCommandOptions = variantCommandOptions.queryGrpCVariantsCommandOptions;

        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(queryGrpcCommandOptions.dbName);
        List<String> studyNames = variantDBAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions());

        // We prepare and build the needed objects: query, queryOptions and outputStream to print results
        Query query = VariantQueryCommandUtils.parseQuery(queryGrpcCommandOptions, studyNames);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(queryGrpcCommandOptions);
        OutputStream outputStream = VariantQueryCommandUtils.getOutputStream(queryGrpcCommandOptions);
        PrintStream printStream = new PrintStream(outputStream);

        // Query object implements a Map<String, Object> while gRPC request object is a Map<String, String>,
        // we need to convert all parsed query fields into String, Lists are taken care of to avoid square brackets
        Map<String, String> queryString = new HashMap<>();
        query.keySet().stream().forEach(s ->  {
            if (query.get(s) instanceof ArrayList || query.get(s) instanceof LinkedList) {
                String replace = query.getString(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key())
                        .replace(" ", "").replace("[", "").replace("]", "");
                queryString.put(s, replace);
            } else {
                queryString.put(s, String.valueOf(query.get(s)));
            }
        });
        logger.debug("Query object: {}", queryString);

        Map<String, String> queryOptionsString = new HashMap<>();
        options.keySet().stream().forEach(s -> queryOptionsString.put(s, String.valueOf(options.get(s))));
        logger.debug("QueryOption object: {}", queryOptionsString);

        // Setting the storageEngine and database to execute the query, this are passed to the gRPC in the request object
        String storageEngine = configuration.getDefaultStorageEngineId();
        if (StringUtils.isNotEmpty(queryGrpcCommandOptions.commonOptions.storageEngine)) {
            storageEngine = queryGrpcCommandOptions.commonOptions.storageEngine;
        }

        String database = configuration.getStorageEngine(storageEngine).getVariant().getOptions().getString("database.name");
        if (StringUtils.isNotEmpty(queryGrpcCommandOptions.dbName)) {
            database = queryGrpcCommandOptions.dbName;
        }

        // We create the OpenCGA gRPC request object with the query, queryOptions, storageEngine and database
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .setStorageEngine(storageEngine)
                .setDatabase(database)
                .putAllQuery(queryString)
                .putAllOptions(queryOptionsString)
                .build();


        // Connecting to the server host and port
        String grpcServerHost = "localhost";
        if (StringUtils.isNotEmpty(queryGrpcCommandOptions.host)) {
            grpcServerHost = queryGrpcCommandOptions.host;
        }

        int grpcServerPort = configuration.getServer().getGrpc();
        if (queryGrpcCommandOptions.port > 0) {
            grpcServerPort = queryGrpcCommandOptions.port;
        }
        logger.debug("Connecting to gRPC server at '{}:{}' to database '{}:{}'", grpcServerHost, grpcServerPort, storageEngine, database);

        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcServerHost, grpcServerPort)
                .usePlaintext(true)
                .build();


        // We use a blocking stub to execute the query to gRPC
        VariantServiceGrpc.VariantServiceBlockingStub geneServiceBlockingStub = VariantServiceGrpc.newBlockingStub(channel);
        Iterator<VariantProto.Variant> variantIterator = geneServiceBlockingStub.get(request);
        while (variantIterator.hasNext()) {
            VariantProto.Variant next = variantIterator.next();
            printStream.println(next.toString());
        }

        // Close open resources
        printStream.close();
        channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    private void annotation() throws StorageManagerException, IOException, URISyntaxException, VariantAnnotatorException {
        CliOptionsParser.AnnotateVariantsCommandOptions annotateVariantsCommandOptions
                = variantCommandOptions.annotateVariantsCommandOptions;

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(annotateVariantsCommandOptions.dbName);

        /*
         * Create Annotator
         */
        ObjectMap options = configuration.getStorageEngine(storageEngine).getVariant().getOptions();
        if (annotateVariantsCommandOptions.annotator != null) {
            options.put(VariantAnnotationManager.ANNOTATION_SOURCE, annotateVariantsCommandOptions.annotator);
        }
        if (annotateVariantsCommandOptions.customAnnotationKey != null) {
            options.put(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, annotateVariantsCommandOptions.customAnnotationKey);
        }
        if (annotateVariantsCommandOptions.species != null) {
            options.put(VariantAnnotationManager.SPECIES, annotateVariantsCommandOptions.species);
        }
        if (annotateVariantsCommandOptions.assembly != null) {
            options.put(VariantAnnotationManager.ASSEMBLY, annotateVariantsCommandOptions.assembly);
        }
        options.putAll(annotateVariantsCommandOptions.commonOptions.params);

        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(configuration, storageEngine);
//            VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(annotatorSource, annotatorProperties,
// annotateVariantsCommandOptions.species, annotateVariantsCommandOptions.assembly);
        VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(annotator, dbAdaptor);

        /*
         * Annotation options
         */
        Query query = new Query();
        if (annotateVariantsCommandOptions.filterRegion != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), annotateVariantsCommandOptions.filterRegion);
        }
        if (annotateVariantsCommandOptions.filterChromosome != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), annotateVariantsCommandOptions.filterChromosome);
        }
        if (annotateVariantsCommandOptions.filterGene != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), annotateVariantsCommandOptions.filterGene);
        }
        if (annotateVariantsCommandOptions.filterAnnotConsequenceType != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(),
                    annotateVariantsCommandOptions.filterAnnotConsequenceType);
        }
        if (!annotateVariantsCommandOptions.overwriteAnnotations) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
        }
        URI outputUri = UriUtils.createUri(annotateVariantsCommandOptions.outdir == null ? "." : annotateVariantsCommandOptions.outdir);
        Path outDir = Paths.get(outputUri.resolve(".").getPath());

        /*
         * Create and load annotations
         */
        boolean doCreate = annotateVariantsCommandOptions.create, doLoad = annotateVariantsCommandOptions.load != null;
        if (!annotateVariantsCommandOptions.create && annotateVariantsCommandOptions.load == null) {
            doCreate = true;
            doLoad = true;
        }

        URI annotationFile = null;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation ");
            annotationFile = variantAnnotationManager.createAnnotation(outDir, annotateVariantsCommandOptions.fileName == null
                    ? annotateVariantsCommandOptions.dbName
                    : annotateVariantsCommandOptions.fileName, query, new QueryOptions());
            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            if (annotationFile == null) {
//                annotationFile = new URI(null, c.load, null);
                annotationFile = Paths.get(annotateVariantsCommandOptions.load).toUri();
            }
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions(options));

            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }
    }

    private void stats() throws IOException, URISyntaxException, StorageManagerException, IllegalAccessException, InstantiationException,
            ClassNotFoundException {
        CliOptionsParser.StatsVariantsCommandOptions statsVariantsCommandOptions = variantCommandOptions.statsVariantsCommandOptions;

        ObjectMap options = storageConfiguration.getVariant().getOptions();
        if (statsVariantsCommandOptions.dbName != null && !statsVariantsCommandOptions.dbName.isEmpty()) {
            options.put(VariantStorageManager.Options.DB_NAME.key(), statsVariantsCommandOptions.dbName);
        }
        options.put(VariantStorageManager.Options.OVERWRITE_STATS.key(), statsVariantsCommandOptions.overwriteStats);
        options.put(VariantStorageManager.Options.UPDATE_STATS.key(), statsVariantsCommandOptions.updateStats);
        if (statsVariantsCommandOptions.fileId != 0) {
            options.put(VariantStorageManager.Options.FILE_ID.key(), statsVariantsCommandOptions.fileId);
        }
        options.put(VariantStorageManager.Options.STUDY_ID.key(), statsVariantsCommandOptions.studyId);
        if (statsVariantsCommandOptions.studyConfigurationFile != null && !statsVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
            options.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, statsVariantsCommandOptions.studyConfigurationFile);
        }

        if (statsVariantsCommandOptions.commonOptions.params != null) {
            options.putAll(statsVariantsCommandOptions.commonOptions.params);
        }

        Map<String, Set<String>> cohorts = null;
        if (statsVariantsCommandOptions.cohort != null && !statsVariantsCommandOptions.cohort.isEmpty()) {
            cohorts = new LinkedHashMap<>(statsVariantsCommandOptions.cohort.size());
            for (Map.Entry<String, String> entry : statsVariantsCommandOptions.cohort.entrySet()) {
                List<String> samples = Arrays.asList(entry.getValue().split(","));
                if (samples.size() == 1 && samples.get(0).isEmpty()) {
                    samples = new ArrayList<>();
                }
                cohorts.put(entry.getKey(), new HashSet<>(samples));
            }
        }

        options.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), statsVariantsCommandOptions.aggregated);

        if (statsVariantsCommandOptions.aggregationMappingFile != null) {
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(statsVariantsCommandOptions.aggregationMappingFile));
                options.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", statsVariantsCommandOptions
                        .aggregationMappingFile);
            }
        }

        /**
         * Create DBAdaptor
         */
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(options.getString(VariantStorageManager.Options.DB_NAME.key()));
//        dbAdaptor.setConstantSamples(Integer.toString(statsVariantsCommandOptions.fileId));    // TODO jmmut: change to studyId when we
// remove fileId
        StudyConfiguration studyConfiguration = dbAdaptor.getStudyConfigurationManager()
                .getStudyConfiguration(statsVariantsCommandOptions.studyId, new QueryOptions(options)).first();
        if (studyConfiguration == null) {
            studyConfiguration = new StudyConfiguration(statsVariantsCommandOptions.studyId, statsVariantsCommandOptions.dbName);
        }
        /**
         * Create and load stats
         */
        URI outputUri = UriUtils.createUri(statsVariantsCommandOptions.fileName == null ? "" : statsVariantsCommandOptions.fileName);
        URI directoryUri = outputUri.resolve(".");
        String filename = outputUri.equals(directoryUri) ? VariantStorageManager.buildFilename(studyConfiguration.getStudyName(),
                statsVariantsCommandOptions.fileId)
                : Paths.get(outputUri.getPath()).getFileName().toString();
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

        try {

            Map<String, Integer> cohortIds = statsVariantsCommandOptions.cohortIds.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.parseInt(e.getValue())));

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
        } catch (Exception e) {   // file not found? wrong file id or study id? bad parameters to ParallelTaskRunner?
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }



    private void executeRank(Query query, VariantDBAdaptor variantDBAdaptor,
                             CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String field = queryVariantsCommandOptions.rank;
        boolean asc = false;
        if (queryVariantsCommandOptions.rank.contains(":")) {  //  eg. gene:-1
            String[] arr = queryVariantsCommandOptions.rank.split(":");
            field = arr[0];
            if (arr[1].endsWith("-1")) {
                asc = true;
            }
        }
        int limit = 10;
        if (queryVariantsCommandOptions.limit > 0) {
            limit = queryVariantsCommandOptions.limit;
        }
        QueryResult rank = variantDBAdaptor.rank(query, field, limit, asc);
        System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
    }

    private void printJsonResult(VariantDBIterator variantDBIterator, OutputStream outputStream) throws IOException {
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            outputStream.write(variant.toJson().getBytes());
            outputStream.write('\n');
        }
    }

    private void benchmark() throws StorageManagerException, InterruptedException, ExecutionException, InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        CliOptionsParser.BenchmarkCommandOptions benchmarkCommandOptions = variantCommandOptions.benchmarkCommandOptions;

// Overwrite default options from configuration.yaml with CLI parameters
        if (benchmarkCommandOptions.commonOptions.storageEngine != null && !benchmarkCommandOptions.commonOptions.storageEngine.isEmpty()) {
            configuration.getBenchmark().setStorageEngine(benchmarkCommandOptions.commonOptions.storageEngine);
        } else {
            configuration.getBenchmark().setStorageEngine(configuration.getDefaultStorageEngineId());
            logger.debug("Storage Engine for benchmarking set to '{}'", configuration.getDefaultStorageEngineId());
        }

        if (benchmarkCommandOptions.repetition > 0) {
            configuration.getBenchmark().setNumRepetitions(benchmarkCommandOptions.repetition);
        }

        if (benchmarkCommandOptions.database != null && !benchmarkCommandOptions.database.isEmpty()) {
            configuration.getBenchmark().setDatabaseName(benchmarkCommandOptions.database);
        }

        if (benchmarkCommandOptions.table != null && !benchmarkCommandOptions.table.isEmpty()) {
            configuration.getBenchmark().setTable(benchmarkCommandOptions.table);
        }

        if (benchmarkCommandOptions.queries != null) {
            configuration.getBenchmark().setQueries(Arrays.asList(benchmarkCommandOptions.queries.split(",")));
        }

        DatabaseCredentials databaseCredentials = configuration.getBenchmark().getDatabase();
        if (benchmarkCommandOptions.host != null && !benchmarkCommandOptions.host.isEmpty()) {
            databaseCredentials.setHosts(Arrays.asList(benchmarkCommandOptions.host.split(",")));
        }

        if (benchmarkCommandOptions.concurrency > 0) {
            configuration.getBenchmark().setConcurrency(benchmarkCommandOptions.concurrency);
        }

        logger.debug("Benchmark configuration: {}", configuration.getBenchmark());

        // validate
        checkParams();

//        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(benchmarkCommandOptions.storageEngine);
        BenchmarkManager benchmarkManager = new BenchmarkManager(configuration);
        benchmarkManager.variantBenchmark();
    }

    private void checkParams() {
        if (configuration.getBenchmark().getDatabaseName() == null || configuration.getBenchmark().getDatabaseName().isEmpty()) {
            throw new ParameterException("Database name is null or empty. Please provide database name.");
        }

        if (configuration.getBenchmark().getTable() == null || configuration.getBenchmark().getTable().isEmpty()) {
            throw new ParameterException("Table name is null or empty. Please provide table name.");
        }

        if (configuration.getBenchmark().getDatabase().getHosts() == null
                || configuration.getBenchmark().getDatabase().getHosts().isEmpty()) {
            throw new ParameterException("Database name is null or empty. Please provide Database name.");
        }
    }

}
