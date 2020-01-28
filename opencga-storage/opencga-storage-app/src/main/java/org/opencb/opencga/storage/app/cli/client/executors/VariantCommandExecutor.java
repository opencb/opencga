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

package org.opencb.opencga.storage.app.cli.client.executors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.variant.converters.avro.VariantAvroToVariantContextConverter;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.search.solr.SolrVariantDBIterator;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateFamilyCommandOptions.AGGREGATE_FAMILY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateCommandOptions.AGGREGATE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private StorageEngineConfiguration storageConfiguration;
    private VariantStorageEngine variantStorageEngine;

    private StorageVariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(StorageVariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    private void configure(GeneralCliOptions.CommonOptions commonOptions, String dbName) throws Exception {

        this.logFile = commonOptions.logFile;

        /**
         * Getting VariantStorageEngine
         * We need to find out the Storage Engine Id to be used
         * If not storage engine is passed then the default is taken from storage-configuration.yml file
         **/
        this.storageEngine = (storageEngine != null && !storageEngine.isEmpty())
                ? storageEngine
                : configuration.getVariant().getDefaultEngine();
        logger.debug("Storage Engine set to '{}'", this.storageEngine);

        this.storageConfiguration = configuration.getVariantEngine(storageEngine);

        // TODO: Start passing catalogManager
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(configuration);
        if (storageEngine == null || storageEngine.isEmpty()) {
            this.variantStorageEngine = storageEngineFactory.getVariantStorageEngine(null, dbName);
        } else {
            this.variantStorageEngine = storageEngineFactory.getVariantStorageEngine(storageEngine, dbName);
        }
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

//        String subCommandString = variantCommandOptions.getParsedSubCommand();
        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        switch (subCommandString) {
            case "index":
                configure(variantCommandOptions.indexVariantsCommandOptions.commonOptions,
                        variantCommandOptions.indexVariantsCommandOptions.commonIndexOptions.dbName);
                index();
                break;
            case VARIANT_DELETE_COMMAND:
                configure(variantCommandOptions.variantDeleteCommandOptions.commonOptions,
                        variantCommandOptions.variantDeleteCommandOptions.dbName);
                remove();
                break;
            case "query":
                configure(variantCommandOptions.variantQueryCommandOptions.commonOptions,
                        variantCommandOptions.variantQueryCommandOptions.commonQueryOptions.dbName);
                query();
                break;
//            case "query-grpc":
//                configure(variantCommandOptions.queryVariantsCommandOptions.commonOptions);
//                queryGrpc();
//                break;
            case "import":
                configure(variantCommandOptions.importVariantsCommandOptions.commonOptions,
                        variantCommandOptions.importVariantsCommandOptions.dbName);
                importData();
                break;
            case "annotate":
                configure(variantCommandOptions.annotateVariantsCommandOptions.commonOptions,
                        variantCommandOptions.annotateVariantsCommandOptions.dbName);
                annotation();
                break;
            case ANNOTATION_SAVE_COMMAND:
                configure(variantCommandOptions.annotationSaveCommandOptions.commonOptions,
                        variantCommandOptions.annotationSaveCommandOptions.dbName);
                annotationSave();
                break;
            case ANNOTATION_DELETE_COMMAND:
                configure(variantCommandOptions.annotationDeleteCommandOptions.commonOptions,
                        variantCommandOptions.annotationDeleteCommandOptions.dbName);
                annotationDelete();
                break;
            case ANNOTATION_QUERY_COMMAND:
                configure(variantCommandOptions.annotationQueryCommandOptions.commonOptions,
                        variantCommandOptions.annotationQueryCommandOptions.dbName);
                annotationQuery();
                break;
            case ANNOTATION_METADATA_COMMAND:
                configure(variantCommandOptions.annotationMetadataCommandOptions.commonOptions,
                        variantCommandOptions.annotationMetadataCommandOptions.dbName);
                annotationMetadata();
                break;
            case "stats":
                configure(variantCommandOptions.statsVariantsCommandOptions.commonOptions,
                        variantCommandOptions.statsVariantsCommandOptions.dbName);
                stats();
                break;
            case AGGREGATE_FAMILY_COMMAND:
                configure(variantCommandOptions.fillGapsCommandOptions.commonOptions,
                        variantCommandOptions.fillGapsCommandOptions.dbName);
                fillGaps();
                break;
            case AGGREGATE_COMMAND:
                configure(variantCommandOptions.fillMissingCommandOptions.commonOptions,
                        variantCommandOptions.fillMissingCommandOptions.dbName);
                fillMissing();
                break;
            case "export":
                configure(variantCommandOptions.exportVariantsCommandOptions.queryOptions.commonOptions,
                        variantCommandOptions.exportVariantsCommandOptions.queryOptions.commonQueryOptions.dbName);
                export();
                break;
            case "search":
                configure(variantCommandOptions.searchVariantsCommandOptions.commonOptions,
                        variantCommandOptions.searchVariantsCommandOptions.dbName);
                search();
                break;
//            case "benchmark":
//                configure(variantCommandOptions.statsVariantsCommandOptions.commonOptions);
//                benchmark();
//                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
        variantStorageEngine.close();
    }

    private void index() throws URISyntaxException, IOException, StorageEngineException, FileFormatException {
        StorageVariantCommandOptions.VariantIndexCommandOptions indexVariantsCommandOptions = variantCommandOptions.indexVariantsCommandOptions;
        List<URI> inputUris = new LinkedList<>();
        String inputs[] = indexVariantsCommandOptions.commonIndexOptions.input.split(",");
        for (String uri : inputs) {
            URI variantsUri = UriUtils.createUri(uri);
            if (!indexVariantsCommandOptions.stdin) {
                if (variantsUri.getScheme().startsWith("file") || variantsUri.getScheme().isEmpty()) {
                    FileUtils.checkFile(Paths.get(variantsUri));
                }
            }
            inputUris.add(variantsUri);
        }

//        URI pedigreeUri = (indexVariantsCommandOptions.pedigree != null && !indexVariantsCommandOptions.pedigree.isEmpty())
//                ? UriUtils.createUri(indexVariantsCommandOptions.pedigree)
//                : null;
//        if (pedigreeUri != null) {
//            FileUtils.checkFile(Paths.get(pedigreeUri));
//        }

        URI outdirUri = (indexVariantsCommandOptions.commonIndexOptions.outdir != null && !indexVariantsCommandOptions.commonIndexOptions.outdir.isEmpty())
                ? UriUtils.createDirectoryUri(indexVariantsCommandOptions.commonIndexOptions.outdir)
                // Get parent folder from input file
                : inputUris.get(0).resolve(".");
        if (outdirUri.getScheme().startsWith("file") || outdirUri.getScheme().isEmpty()) {
            FileUtils.checkDirectory(Paths.get(outdirUri), true);
        }
        logger.debug("All files and directories exist");

//            VariantSource source = new VariantSource(fileName, indexVariantsCommandOptions.fileId,
//                    indexVariantsCommandOptions.studyId, indexVariantsCommandOptions.study, indexVariantsCommandOptions.studyType,
// indexVariantsCommandOptions.aggregated);

        /* Add CLi options to the variant options */
        ObjectMap params = storageConfiguration.getOptions();
        params.put(VariantStorageOptions.MERGE_MODE.key(), indexVariantsCommandOptions.merge);
        params.put(VariantStorageOptions.STUDY.key(), indexVariantsCommandOptions.study);
        params.put(VariantStorageOptions.STATS_CALCULATE.key(), indexVariantsCommandOptions.calculateStats);
        params.put(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), indexVariantsCommandOptions.excludeGenotype);
        params.put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), indexVariantsCommandOptions.includeExtraFields);
//        variantOptions.put(VariantStorageEngine.Options.INCLUDE_SRC.key(), indexVariantsCommandOptions.includeSrc);
//        variantOptions.put(VariantStorageEngine.Options.COMPRESS_GENOTYPES.key(), indexVariantsCommandOptions.compressGenotypes);
        params.put(VariantStorageOptions.STATS_AGGREGATION.key(), indexVariantsCommandOptions.aggregated);
        params.put(VariantStorageOptions.STDIN.key(), indexVariantsCommandOptions.stdin);
        params.put(VariantStorageOptions.STDOUT.key(), indexVariantsCommandOptions.stdout);

        params.put(VariantStorageOptions.ANNOTATE.key(), indexVariantsCommandOptions.annotate);
        if (indexVariantsCommandOptions.annotator != null) {
            params.put(VariantStorageOptions.ANNOTATOR.key(), indexVariantsCommandOptions.annotator);
        }
        params.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), indexVariantsCommandOptions.overwriteAnnotations);
//        if (indexVariantsCommandOptions.studyConfigurationFile != null && !indexVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
//            params.put(FileStudyConfigurationAdaptor.STUDY_CONFIGURATION_PATH, indexVariantsCommandOptions.studyConfigurationFile);
//        }
        params.put(VariantStorageOptions.RESUME.key(), indexVariantsCommandOptions.resume);
        params.put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), indexVariantsCommandOptions.loadSplitData);
        params.put(VariantStorageOptions.POST_LOAD_CHECK_SKIP.key(), indexVariantsCommandOptions.skipPostLoadCheck);
        params.put(VariantStorageOptions.INDEX_SEARCH.key(), indexVariantsCommandOptions.indexSearch);
        params.put(VariantStorageOptions.SPECIES.key(), indexVariantsCommandOptions.species);
        params.put(VariantStorageOptions.ASSEMBLY.key(), indexVariantsCommandOptions.assembly);

        if (indexVariantsCommandOptions.aggregationMappingFile != null) {
            // TODO move this options to new configuration.yml
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(indexVariantsCommandOptions.aggregationMappingFile));
                params.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", indexVariantsCommandOptions
                        .aggregationMappingFile);
            }
        }

        if (indexVariantsCommandOptions.commonOptions.params != null) {
            params.putAll(indexVariantsCommandOptions.commonOptions.params);
        }
        logger.debug("Configuration options: {}", params.toJson());


        /* Execute ETL steps */
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
        variantStorageEngine.index(inputUris, outdirUri, doExtract, doTransform, doLoad);
    }

    private void remove() throws Exception {
        StorageVariantCommandOptions.VariantDeleteCommandOptions cliOptions = variantCommandOptions.variantDeleteCommandOptions;

        variantStorageEngine.getOptions().put(VariantStorageOptions.RESUME.key(), cliOptions.resume);
        variantStorageEngine.getOptions().putAll(cliOptions.commonOptions.params);

        if (cliOptions.file.size() == 1 && cliOptions.file.get(0).equalsIgnoreCase(VariantQueryUtils.ALL)) {
            variantStorageEngine.removeStudy(cliOptions.study);
        } else {
            variantStorageEngine.removeFiles(cliOptions.study, cliOptions.file);
        }
    }

    private void query() throws Exception {
        StorageVariantCommandOptions.VariantQueryCommandOptions variantQueryCommandOptions = variantCommandOptions.variantQueryCommandOptions;

//        if (true) {
////            System.out.println(variantCommandOptions.queryVariantsCommandOptions.toString());
//            System.out.println(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(variantCommandOptions
//                    .variantQueryCommandOptions));
//            return;
//        }

        storageConfiguration.getOptions().putAll(variantQueryCommandOptions.commonOptions.params);

        List<String> studyNames = variantStorageEngine.getMetadataManager().getStudyNames();

        Query query = VariantQueryCommandUtils.parseQuery(variantQueryCommandOptions, studyNames);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(variantQueryCommandOptions);

        if (variantQueryCommandOptions.commonQueryOptions.count) {
            DataResult<Long> result = variantStorageEngine.count(query);
            System.out.println("Num. results\t" + result.getResults().get(0));
        } else if (StringUtils.isNotEmpty(variantQueryCommandOptions.rank)) {
            executeRank(query, variantStorageEngine, variantQueryCommandOptions);
        } else if (StringUtils.isNotEmpty(variantQueryCommandOptions.groupBy)) {
            ObjectMapper objectMapper = new ObjectMapper();
            DataResult groupBy = variantStorageEngine.groupBy(query, variantQueryCommandOptions.groupBy, options);
            System.out.println("groupBy = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
        } else {
            URI uri = StringUtils.isEmpty(variantQueryCommandOptions.commonQueryOptions.output)
                    ? null
                    : UriUtils.createUri(variantQueryCommandOptions.commonQueryOptions.output);
            URI variantsFile = StringUtils.isEmpty(variantQueryCommandOptions.variantsFile)
                    ? null
                    : UriUtils.createUri(variantQueryCommandOptions.variantsFile);

            if (variantQueryCommandOptions.annotations != null) {
                options.add("annotations", variantQueryCommandOptions.annotations);
            }
            VariantWriterFactory.VariantOutputFormat of = VariantWriterFactory
                    .toOutputFormat(variantQueryCommandOptions.outputFormat, variantQueryCommandOptions.commonQueryOptions.output);
            variantStorageEngine.exportData(uri, of, variantsFile, query, options);
        }
    }

    private void importData() throws URISyntaxException, StorageEngineException, IOException {
        StorageVariantCommandOptions.ImportVariantsCommandOptions importVariantsOptions = variantCommandOptions.importVariantsCommandOptions;

        URI uri = UriUtils.createUri(importVariantsOptions.input);
        ObjectMap options = new ObjectMap();
        options.putAll(importVariantsOptions.commonOptions.params);
        variantStorageEngine.importData(uri, options);

    }

    private void annotation() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException {
        StorageVariantCommandOptions.VariantAnnotateCommandOptions annotateVariantsCommandOptions
                = variantCommandOptions.annotateVariantsCommandOptions;

        /*
         * Create Annotator
         */
        ObjectMap options = configuration.getVariantEngine(storageEngine).getOptions();
        if (annotateVariantsCommandOptions.annotator != null) {
            options.put(VariantStorageOptions.ANNOTATOR.key(), annotateVariantsCommandOptions.annotator);
        }
        if (annotateVariantsCommandOptions.customName != null) {
            options.put(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, annotateVariantsCommandOptions.customName);
        }
        if (annotateVariantsCommandOptions.species != null) {
            options.put(VariantStorageOptions.SPECIES.key(), annotateVariantsCommandOptions.species);
        }
        if (annotateVariantsCommandOptions.assembly != null) {
            options.put(VariantStorageOptions.ASSEMBLY.key(), annotateVariantsCommandOptions.assembly);
        }

        String fileName = annotateVariantsCommandOptions.outputFileName == null
                ? annotateVariantsCommandOptions.dbName
                : annotateVariantsCommandOptions.outputFileName;
        options.put(DefaultVariantAnnotationManager.FILE_NAME, fileName);

        URI outputUri = UriUtils.createUri(annotateVariantsCommandOptions.outdir == null ? "." : annotateVariantsCommandOptions.outdir);
        Path outDir = Paths.get(outputUri.resolve(".").getPath());

        options.put(DefaultVariantAnnotationManager.OUT_DIR, outDir.toString());

        options.putAll(annotateVariantsCommandOptions.commonOptions.params);

        /*
         * Annotation options
         */
        Query query = new Query();
        if (annotateVariantsCommandOptions.region != null) {
            query.put(VariantQueryParam.REGION.key(), annotateVariantsCommandOptions.region);
        }
        if (!annotateVariantsCommandOptions.overwriteAnnotations) {
            query.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        }

        /*
         * Create and load annotations
         */
        boolean doCreate = annotateVariantsCommandOptions.create, doLoad = annotateVariantsCommandOptions.load != null;
        if (!annotateVariantsCommandOptions.create && annotateVariantsCommandOptions.load == null) {
            doCreate = true;
            doLoad = true;
        }

        if (doCreate && !doLoad) {
            options.put(DefaultVariantAnnotationManager.CREATE, true);
        }
        if (doLoad) {
            options.put(DefaultVariantAnnotationManager.LOAD_FILE, annotateVariantsCommandOptions.load);
        }

//        URI annotationFile = null;
//        if (doCreate) {
//            long start = System.currentTimeMillis();
//            logger.info("Starting annotation creation ");
//            annotationFile = variantAnnotationManager.createAnnotation(outDir, fileName, query, new QueryOptions(options));
//            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
//        }
//
//        if (doLoad) {
//            long start = System.currentTimeMillis();
//            logger.info("Starting annotation load");
//            if (annotationFile == null) {
////                annotationFile = new URI(null, c.load, null);
//                annotationFile = Paths.get(annotateVariantsCommandOptions.load).toUri();
//            }
//            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions(options));
//
//            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
//        }

        variantStorageEngine.annotate(query, options);
    }

    private void annotationSave() throws VariantAnnotatorException, StorageEngineException {
        StorageVariantCommandOptions.AnnotationSaveCommandOptions cliOptions = variantCommandOptions.annotationSaveCommandOptions;

        ObjectMap options = storageConfiguration.getOptions();
        options.putAll(cliOptions.commonOptions.params);

        variantStorageEngine.saveAnnotation(cliOptions.annotationId, options);
    }

    private void annotationDelete() throws VariantAnnotatorException, StorageEngineException {
        StorageVariantCommandOptions.AnnotationDeleteCommandOptions cliOptions = variantCommandOptions.annotationDeleteCommandOptions;

        ObjectMap options = storageConfiguration.getOptions();
        options.putAll(cliOptions.commonOptions.params);

        variantStorageEngine.deleteAnnotation(cliOptions.annotationId, options);
    }

    private void annotationQuery() throws VariantAnnotatorException, StorageEngineException, IOException {
        StorageVariantCommandOptions.AnnotationQueryCommandOptions cliOptions  = variantCommandOptions.annotationQueryCommandOptions;

        QueryOptions options = new QueryOptions();
        options.put(QueryOptions.LIMIT, cliOptions.limit);
        options.put(QueryOptions.SKIP, cliOptions.skip);
        options.put(QueryOptions.INCLUDE, cliOptions.dataModelOptions.include);
        options.put(QueryOptions.EXCLUDE, cliOptions.dataModelOptions.exclude);
        options.putAll(cliOptions.commonOptions.params);

        Query query = new Query();
        query.put(VariantQueryParam.REGION.key(), cliOptions.region);
        query.put(VariantQueryParam.ID.key(), cliOptions.id);

        DataResult<VariantAnnotation> queryResult = variantStorageEngine.getAnnotation(cliOptions.annotationId, query, options);

        // WRITE
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        ObjectWriter writer = objectMapper.writer();
//        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        SequenceWriter sequenceWriter = writer.writeValues(System.out);
        for (VariantAnnotation annotation : queryResult.getResults()) {
            sequenceWriter.write(annotation);
            sequenceWriter.flush();
//            writer.writeValue(System.out, annotation);
            System.out.println();
        }
    }

    private void annotationMetadata() throws VariantAnnotatorException, StorageEngineException, IOException {
        StorageVariantCommandOptions.AnnotationMetadataCommandOptions cliOptions  = variantCommandOptions.annotationMetadataCommandOptions;


        DataResult<ProjectMetadata.VariantAnnotationMetadata> result = variantStorageEngine.getAnnotationMetadata(cliOptions.annotationId);

        // WRITE
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        ObjectWriter writer = objectMapper.writer();
//        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        SequenceWriter sequenceWriter = writer.writeValues(System.out);
        for (ProjectMetadata.VariantAnnotationMetadata metadata : result.getResults()) {
            sequenceWriter.write(metadata);
            sequenceWriter.flush();
//            writer.writeValue(System.out, annotation);
            System.out.println();
        }
    }

    private void stats() throws IOException, URISyntaxException, StorageEngineException, IllegalAccessException, InstantiationException,
            ClassNotFoundException {
        StorageVariantCommandOptions.VariantStatsCommandOptions statsVariantsCommandOptions = variantCommandOptions.statsVariantsCommandOptions;

        QueryOptions options = new QueryOptions(storageConfiguration.getOptions());
        options.put(VariantStorageOptions.STATS_OVERWRITE.key(), statsVariantsCommandOptions.overwriteStats);
        options.put(VariantStorageOptions.STATS_UPDATE.key(), statsVariantsCommandOptions.updateStats);
//        options.putIfNotEmpty(VariantStorageEngine.Options.FILE_ID.key(), statsVariantsCommandOptions.file);
        options.put(VariantStorageOptions.STUDY.key(), statsVariantsCommandOptions.study);
//        if (statsVariantsCommandOptions.studyConfigurationFile != null && !statsVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
//            options.put(FileStudyConfigurationAdaptor.STUDY_CONFIGURATION_PATH, statsVariantsCommandOptions.studyConfigurationFile);
//        }
        options.put(VariantQueryParam.REGION.key(), statsVariantsCommandOptions.region);
        options.put(VariantQueryParam.GENE.key(), statsVariantsCommandOptions.gene);
        options.put(VariantStorageOptions.RESUME.key(), statsVariantsCommandOptions.resume);

        if (statsVariantsCommandOptions.commonOptions.params != null) {
            options.putAll(statsVariantsCommandOptions.commonOptions.params);
        }

        Map<String, Set<String>> cohorts = null;
        List<String> cohortNames = null;
        if (statsVariantsCommandOptions.cohort != null && !statsVariantsCommandOptions.cohort.isEmpty()) {
            cohorts = new LinkedHashMap<>(statsVariantsCommandOptions.cohort.size());
            for (Map.Entry<String, String> entry : statsVariantsCommandOptions.cohort.entrySet()) {
                List<String> samples = Arrays.asList(entry.getValue().split(","));
                if (samples.size() == 1 && samples.get(0).isEmpty()) {
                    samples = Collections.emptyList();
                }
                cohorts.put(entry.getKey(), new LinkedHashSet<>(samples));
            }
            cohortNames = new ArrayList<>(cohorts.keySet());
        }

        options.put(VariantStorageOptions.STATS_AGGREGATION.key(), statsVariantsCommandOptions.aggregated);

        if (statsVariantsCommandOptions.aggregationMappingFile != null) {
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(statsVariantsCommandOptions.aggregationMappingFile));
                options.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", statsVariantsCommandOptions
                        .aggregationMappingFile);
            }
        }

        URI outputUri = UriUtils.createUri(statsVariantsCommandOptions.fileName == null ? "" : statsVariantsCommandOptions.fileName);
        URI directoryUri = outputUri.resolve(".");
        String studyName = statsVariantsCommandOptions.study;
        String filename = outputUri.equals(directoryUri)
                ? VariantStoragePipeline.buildFilename(studyName, 0)
                : Paths.get(outputUri.getPath()).getFileName().toString();
        filename += '.' + TimeUtils.getTime();
        outputUri = outputUri.resolve(filename);
        options.put(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, filename);
        options.put(DefaultVariantStatisticsManager.OUTPUT, outputUri.toString());

//        assertDirectoryExists(directoryUri);

        if (statsVariantsCommandOptions.load != null) {
            DefaultVariantStatisticsManager statisticsManager =
                    (DefaultVariantStatisticsManager) variantStorageEngine.newVariantStatisticsManager();
            statisticsManager.loadStats(
                    UriUtils.createUri(statsVariantsCommandOptions.load, true),
                    statsVariantsCommandOptions.study, options);
        } else {
            /*
             * Create and load stats
             */
            if (cohorts == null || cohorts.values().stream().allMatch(Set::isEmpty)) {
                variantStorageEngine.calculateStats(studyName, cohortNames, options);
            } else {
                variantStorageEngine.calculateStats(studyName, cohorts, options);
            }
        }
    }

    private void fillGaps() throws StorageEngineException {
        StorageVariantCommandOptions.AggregateFamilyCommandOptions fillGapsCommandOptions = variantCommandOptions.fillGapsCommandOptions;

        ObjectMap options = storageConfiguration.getOptions();
        options.put(VariantStorageOptions.RESUME.key(), fillGapsCommandOptions.resume);
        options.putAll(fillGapsCommandOptions.commonOptions.params);

        variantStorageEngine.aggregateFamily(fillGapsCommandOptions.study, fillGapsCommandOptions.samples, options);
    }

    private void fillMissing() throws StorageEngineException {
        StorageVariantCommandOptions.AggregateCommandOptions cliOptions = variantCommandOptions.fillMissingCommandOptions;

        ObjectMap options = storageConfiguration.getOptions();
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantStorageEngine.aggregate(cliOptions.study, cliOptions.overwrite, options);
    }

    private void export() throws URISyntaxException, StorageEngineException, IOException {
        StorageVariantCommandOptions.VariantExportCommandOptions exportVariantsCommandOptions = variantCommandOptions.exportVariantsCommandOptions;
//
//        ObjectMap options = storageConfiguration.getOptions();
//        if (exportVariantsCommandOptions.dbName != null && !exportVariantsCommandOptions.dbName.isEmpty()) {
//            options.put(VariantStorageEngine.Options.DB_NAME.key(), exportVariantsCommandOptions.dbName);
//        }
//        options.putIfNotEmpty(VariantStorageEngine.Options.FILE_ID.key(), exportVariantsCommandOptions.fileId);
//        options.put(VariantStorageEngine.Options.STUDY_UID.key(), exportVariantsCommandOptions.studyId);
//        if (exportVariantsCommandOptions.studyConfigurationFile != null && !exportVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
//            options.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, exportVariantsCommandOptions.studyConfigurationFile);
//        }
//
//        if (exportVariantsCommandOptions.commonOptions.params != null) {
//            options.putAll(exportVariantsCommandOptions.commonOptions.params);
//        }
//
//
//        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor(exportVariantsCommandOptions.dbName);
//
//        URI outputUri = UriUtils.createUri(exportVariantsCommandOptions.outFilename == null ? "" : exportVariantsCommandOptions.outFilename);
//        URI directoryUri = outputUri.resolve(".");
//        StudyConfiguration studyConfiguration = dbAdaptor.getStudyConfigurationManager()
//                .getStudyConfiguration(exportVariantsCommandOptions.studyId, new QueryOptions(options)).first();
//        if (studyConfiguration == null) {
//            studyConfiguration = new StudyConfiguration(Integer.parseInt(exportVariantsCommandOptions.studyId),
//                    exportVariantsCommandOptions.dbName);
//        }
//        String filename = outputUri.equals(directoryUri) ? VariantStorageEngine.buildFilename(studyConfiguration.getStudyName(),
//                Integer.parseInt(exportVariantsCommandOptions.fileId))
//                : Paths.get(outputUri.getPath()).getFileName().toString();
//


//        URI outputFile = Paths.get(exportVariantsCommandOptions.outFilename).toUri();
//        VariantWriterFactory.VariantOutputFormat outputFormat = VariantWriterFactory.toOutputFormat(null,
//                outputFile.getPath());
//
//        Query query = new Query();
//        QueryOptions queryOptions = new QueryOptions();
//
//        variantStorageEngine.exportData(outputFile, outputFormat, exportVariantsCommandOptions.dbName,
//                query, queryOptions);


//        storageConfiguration.getOptions().putAll(exportVariantsCommandOptions.commonOptions.params);


        List<String> studyNames = variantStorageEngine.getMetadataManager().getStudyNames();


        // TODO: JT
        try {
            Query query = VariantQueryCommandUtils.parseQuery(exportVariantsCommandOptions.queryOptions, studyNames);
            QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(exportVariantsCommandOptions.queryOptions);

            // create VCF header by getting information from metadata or study configuration
            List<String> cohortNames = null;
            List<String> annotations = null;
            List<String> formatFields = null;
            List<String> formatFieldsType = null;
            List<String> formatFieldsDescr = null;
            List<String> sampleNames = null;
            Function<String, String> converter = null;

            VCFHeader vcfHeader = VcfUtils.createVCFHeader(cohortNames, annotations, formatFields,
                    formatFieldsType, formatFieldsDescr, sampleNames, converter);

            // create the variant context writer
            try (OutputStream outputStream = new FileOutputStream(exportVariantsCommandOptions.outFilename);
                 VariantContextWriter writer = VcfUtils.createVariantContextWriter(outputStream,
                         vcfHeader.getSequenceDictionary(), null)) {

                // write VCF header
                writer.writeHeader(vcfHeader);

                // TODO: get study id/name
                VariantAvroToVariantContextConverter variantContextToAvroVariantConverter =
                        new VariantAvroToVariantContextConverter(0, Collections.emptyList(), Collections.emptyList());
                VariantDBIterator iterator = variantStorageEngine.iterator(query, options);
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    VariantContext variantContext = variantContextToAvroVariantConverter.convert(variant);
                    System.out.println(variantContext.toString());
                    writer.add(variantContext);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * search command
     */

    private void search() throws Exception {
        StorageVariantCommandOptions.VariantSearchCommandOptions searchOptions = variantCommandOptions.searchVariantsCommandOptions;

        //VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor(exportVariantsCommandOptions.dbName);
        // variantStorageEngine.getConfiguration().getSearch()

        // TODO: initialize solrUrl and database (i.e.: core/collection name) from the configuration file
        String solrUrl = (searchOptions.solrUrl == null ? "http://localhost:8983/solr/" : searchOptions.solrUrl);
        String dbName = (searchOptions.dbName == null ? "variants" : searchOptions.dbName);

        variantStorageEngine.getConfiguration().getSearch().setHost(solrUrl);

//        VariantSearchManager variantSearchManager = new VariantSearchManager(solrUrl, dbName);
//        VariantSearchManager variantSearchManager = new VariantSearchManager(variantStorageEngine.getStudyConfigurationManager(),
//                variantStorageEngine.getCellBaseUtils(), variantStorageEngine.getConfiguration());
        VariantSearchManager variantSearchManager = new VariantSearchManager(variantStorageEngine.getMetadataManager(),
                variantStorageEngine.getConfiguration());
        boolean querying = true;
        QueryOptions options = new QueryOptions();
        options.putAll(searchOptions.commonOptions.params);
        // create the database, this method checks if it exists and the solrConfig name
        if (searchOptions.create) {
            variantSearchManager.create(dbName, searchOptions.solrConfig);
            querying = false;
        }

        // index
        if (searchOptions.index) {
            querying = false;
            Query query = VariantQueryCommandUtils.parseQuery(searchOptions, new Query());
            variantStorageEngine.secondaryIndex(query, options, searchOptions.overwrite);
        }

        String mode = variantStorageEngine.getConfiguration().getSearch().getMode();
        if (querying) {
            if ("cloud".equals(mode)) {
                if (!variantSearchManager.existsCollection(dbName)) {
                    throw new IllegalArgumentException("Search " + mode + " '" + dbName + "' does not exists");
                }
            } else {
                if (!variantSearchManager.existsCore(dbName)) {
                    throw new IllegalArgumentException("Search " + mode + " '" + dbName + "' does not exists");
                }
            }
            int count = 0;
            try {
                Query query = new Query();
                query = VariantQueryCommandUtils.parseQuery(searchOptions, query);

                // TODO: create a function to parse searchOptions to queryOptions
                QueryOptions queryOptions = new QueryOptions(); //VariantQueryCommandUtils.parseQueryOptions(searchOptions.commonOptions);

                if (StringUtils.isNotEmpty(searchOptions.facet)) {
                    // update query options for facet
                    queryOptions.put(QueryOptions.LIMIT, 0);
                    queryOptions.put(QueryOptions.SKIP, 0);
                    // TODO: move this to the function mentioned in the previous TODO
                    queryOptions.put(QueryOptions.FACET, searchOptions.facet);
                    DataResult<FacetField> facetedQueryResult = variantSearchManager.facetedQuery(dbName, query, queryOptions);
                    if (facetedQueryResult.getResults() != null && CollectionUtils.isNotEmpty(facetedQueryResult.getResults())) {
                        System.out.println("Faceted fields (" + facetedQueryResult.getResults().size() + "):");
                        facetedQueryResult.getResults().forEach(f -> System.out.println(f.toString()));
                    }
                } else {
                    queryOptions.put(QueryOptions.LIMIT, Integer.MAX_VALUE);
                    queryOptions.put(QueryOptions.SKIP, 0);

                    SolrVariantDBIterator iterator = variantSearchManager.iterator(dbName, query, queryOptions);
                    System.out.print("[");
                    while (iterator.hasNext()) {
                        Variant variant = iterator.next();
                        System.out.print(variant.toJson());
                        if (iterator.hasNext()) {
                            System.out.print(",");
                        }
                        count++;
                    }
                    System.out.println("]");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void executeRank(Query query, VariantStorageEngine variantStorageEngine,
                             StorageVariantCommandOptions.VariantQueryCommandOptions variantQueryCommandOptions)
            throws JsonProcessingException, StorageEngineException {
        ObjectMapper objectMapper = new ObjectMapper();
        String field = variantQueryCommandOptions.rank;
        boolean asc = false;
        if (variantQueryCommandOptions.rank.contains(":")) {  //  eg. gene:-1
            String[] arr = variantQueryCommandOptions.rank.split(":");
            field = arr[0];
            if (arr[1].endsWith("-1")) {
                asc = true;
            }
        }
        int limit = 10;
        if (variantQueryCommandOptions.commonQueryOptions.limit > 0) {
            limit = variantQueryCommandOptions.commonQueryOptions.limit;
        }
        DataResult rank = variantStorageEngine.rank(query, field, limit, asc);
        System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
    }

    private void printJsonResult(VariantDBIterator variantDBIterator, OutputStream outputStream) throws IOException {
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            outputStream.write(variant.toJson().getBytes());
            outputStream.write('\n');
        }
    }

//    private void benchmark() throws StorageEngineException, InterruptedException, ExecutionException, InstantiationException,
//            IllegalAccessException, ClassNotFoundException {
//        StorageVariantCommandOptions.BenchmarkCommandOptions benchmarkCommandOptions = variantCommandOptions.benchmarkCommandOptions;
//
//// Overwrite default options from configuration.yaml with CLI parameters
//        if (benchmarkCommandOptions.commonOptions.storageEngine != null && !benchmarkCommandOptions.commonOptions.storageEngine.isEmpty()) {
//            configuration.getBenchmark().setStorageEngine(benchmarkCommandOptions.commonOptions.storageEngine);
//        } else {
//            configuration.getBenchmark().setStorageEngine(configuration.getDefaultStorageEngineId());
//            logger.debug("Storage Engine for benchmarking set to '{}'", configuration.getDefaultStorageEngineId());
//        }
//
//        if (benchmarkCommandOptions.repetition > 0) {
//            configuration.getBenchmark().setNumRepetitions(benchmarkCommandOptions.repetition);
//        }
//
//        if (benchmarkCommandOptions.database != null && !benchmarkCommandOptions.database.isEmpty()) {
//            configuration.getBenchmark().setDatabaseName(benchmarkCommandOptions.database);
//        }
//
//        if (benchmarkCommandOptions.table != null && !benchmarkCommandOptions.table.isEmpty()) {
//            configuration.getBenchmark().setTable(benchmarkCommandOptions.table);
//        }
//
//        if (benchmarkCommandOptions.queries != null) {
//            configuration.getBenchmark().setQueries(Arrays.asList(benchmarkCommandOptions.queries.split(",")));
//        }
//
//        DatabaseCredentials databaseCredentials = configuration.getBenchmark().getDatabase();
//        if (benchmarkCommandOptions.host != null && !benchmarkCommandOptions.host.isEmpty()) {
//            databaseCredentials.setHosts(Arrays.asList(benchmarkCommandOptions.host.split(",")));
//        }
//
//        if (benchmarkCommandOptions.concurrency > 0) {
//            configuration.getBenchmark().setConcurrency(benchmarkCommandOptions.concurrency);
//        }
//
//        logger.debug("Benchmark configuration: {}", configuration.getBenchmark());
//
//        // validate
//        checkParams();
//
////        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(benchmarkCommandOptions.storageEngine);
//        BenchmarkManager benchmarkManager = new BenchmarkManager(configuration);
//        benchmarkManager.variantBenchmark();
//    }

}
