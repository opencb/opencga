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

import com.beust.jcommander.ParameterException;
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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
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
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
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
import org.opencb.opencga.storage.core.variant.search.solr.VariantSolrIterator;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.CreateAnnotationSnapshotCommandOptions.COPY_ANNOTATION_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.DeleteAnnotationSnapshotCommandOptions.DELETE_ANNOTATION_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillGapsCommandOptions.FILL_GAPS_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillMissingCommandOptions.FILL_MISSING_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.QueryAnnotationCommandOptions.QUERY_ANNOTATION_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND;

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
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", this.storageEngine);

        this.storageConfiguration = configuration.getStorageEngine(storageEngine);

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
            case VARIANT_REMOVE_COMMAND:
                configure(variantCommandOptions.variantRemoveCommandOptions.commonOptions,
                        variantCommandOptions.variantRemoveCommandOptions.dbName);
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
            case COPY_ANNOTATION_COMMAND:
                configure(variantCommandOptions.createAnnotationSnapshotCommandOptions.commonOptions,
                        variantCommandOptions.createAnnotationSnapshotCommandOptions.dbName);
                copyAnnotation();
                break;
            case DELETE_ANNOTATION_COMMAND:
                configure(variantCommandOptions.deleteAnnotationSnapshotCommandOptions.commonOptions,
                        variantCommandOptions.deleteAnnotationSnapshotCommandOptions.dbName);
                deleteAnnotation();
                break;
            case QUERY_ANNOTATION_COMMAND:
                configure(variantCommandOptions.queryAnnotationCommandOptions.commonOptions,
                        variantCommandOptions.queryAnnotationCommandOptions.dbName);
                queryAnnotation();
                break;
            case "stats":
                configure(variantCommandOptions.statsVariantsCommandOptions.commonOptions,
                        variantCommandOptions.statsVariantsCommandOptions.dbName);
                stats();
                break;
            case FILL_GAPS_COMMAND:
                configure(variantCommandOptions.fillGapsCommandOptions.commonOptions,
                        variantCommandOptions.fillGapsCommandOptions.dbName);
                fillGaps();
                break;
            case FILL_MISSING_COMMAND:
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

    }

    private void index() throws URISyntaxException, IOException, StorageEngineException, FileFormatException {
        StorageVariantCommandOptions.VariantIndexCommandOptions indexVariantsCommandOptions = variantCommandOptions.indexVariantsCommandOptions;
        List<URI> inputUris = new LinkedList<>();
        String inputs[] = indexVariantsCommandOptions.commonIndexOptions.input.split(",");
        for (String uri : inputs) {
            URI variantsUri = UriUtils.createUri(uri);
            if (variantsUri.getScheme().startsWith("file") || variantsUri.getScheme().isEmpty()) {
                FileUtils.checkFile(Paths.get(variantsUri));
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

        /** Add CLi options to the variant options **/
        ObjectMap params = storageConfiguration.getVariant().getOptions();
        params.put(VariantStorageEngine.Options.MERGE_MODE.key(), indexVariantsCommandOptions.merge);
        params.put(VariantStorageEngine.Options.STUDY.key(), indexVariantsCommandOptions.study);
        params.put(VariantStorageEngine.Options.STUDY_TYPE.key(), indexVariantsCommandOptions.studyType);
        params.put(VariantStorageEngine.Options.CALCULATE_STATS.key(), indexVariantsCommandOptions.calculateStats);
        params.put(VariantStorageEngine.Options.INCLUDE_STATS.key(), indexVariantsCommandOptions.includeStats);
        params.put(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), indexVariantsCommandOptions.excludeGenotype);
        params.put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), indexVariantsCommandOptions.extraFields);
//        variantOptions.put(VariantStorageEngine.Options.INCLUDE_SRC.key(), indexVariantsCommandOptions.includeSrc);
//        variantOptions.put(VariantStorageEngine.Options.COMPRESS_GENOTYPES.key(), indexVariantsCommandOptions.compressGenotypes);
        params.put(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), indexVariantsCommandOptions.aggregated);

        params.put(VariantStorageEngine.Options.ANNOTATE.key(), indexVariantsCommandOptions.annotate);
        if (indexVariantsCommandOptions.annotator != null) {
            params.put(VariantAnnotationManager.ANNOTATOR, indexVariantsCommandOptions.annotator);
        }
        params.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, indexVariantsCommandOptions.overwriteAnnotations);
//        if (indexVariantsCommandOptions.studyConfigurationFile != null && !indexVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
//            params.put(FileStudyConfigurationAdaptor.STUDY_CONFIGURATION_PATH, indexVariantsCommandOptions.studyConfigurationFile);
//        }
        params.put(VariantStorageEngine.Options.RESUME.key(), indexVariantsCommandOptions.resume);
        params.put(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(), indexVariantsCommandOptions.loadSplitData);
        params.put(VariantStorageEngine.Options.POST_LOAD_CHECK_SKIP.key(), indexVariantsCommandOptions.skipPostLoadCheck);

        if (indexVariantsCommandOptions.aggregationMappingFile != null) {
            // TODO move this options to new configuration.yml
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(indexVariantsCommandOptions.aggregationMappingFile));
                params.put(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", indexVariantsCommandOptions
                        .aggregationMappingFile);
            }
        }

        if (indexVariantsCommandOptions.commonOptions.params != null) {
            params.putAll(indexVariantsCommandOptions.commonOptions.params);
        }
        logger.debug("Configuration options: {}", params.toJson());


        /** Execute ETL steps **/
        boolean doExtract, doTransform, doLoad;


        if (!indexVariantsCommandOptions.indexSearch) {
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
        } else {
            try {
                variantStorageEngine.searchIndex();
            } catch (VariantSearchException e) {
                e.printStackTrace();
            }
        }
    }

    private void remove() throws Exception {
        StorageVariantCommandOptions.VariantRemoveCommandOptions cliOptions = variantCommandOptions.variantRemoveCommandOptions;

        variantStorageEngine.getOptions().put(VariantStorageEngine.Options.RESUME.key(), cliOptions.resume);
        variantStorageEngine.getOptions().putAll(cliOptions.commonOptions.params);

        if (cliOptions.files.size() == 1 && cliOptions.files.get(0).equalsIgnoreCase(VariantQueryUtils.ALL)) {
            variantStorageEngine.removeStudy(cliOptions.study);
        } else {
            variantStorageEngine.removeFiles(cliOptions.study, cliOptions.files);
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

        storageConfiguration.getVariant().getOptions().putAll(variantQueryCommandOptions.commonOptions.params);

        List<String> studyNames = variantStorageEngine.getStudyConfigurationManager().getStudyNames(new QueryOptions());

        Query query = VariantQueryCommandUtils.parseQuery(variantQueryCommandOptions, studyNames);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(variantQueryCommandOptions);

        if (variantQueryCommandOptions.commonQueryOptions.count) {
            QueryResult<Long> result = variantStorageEngine.count(query);
            System.out.println("Num. results\t" + result.getResult().get(0));
        } else if (StringUtils.isNotEmpty(variantQueryCommandOptions.rank)) {
            executeRank(query, variantStorageEngine, variantQueryCommandOptions);
        } else if (StringUtils.isNotEmpty(variantQueryCommandOptions.groupBy)) {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResult groupBy = variantStorageEngine.groupBy(query, variantQueryCommandOptions.groupBy, options);
            System.out.println("groupBy = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
        } else {
            URI uri = StringUtils.isEmpty(variantQueryCommandOptions.commonQueryOptions.output)
                    ? null
                    : UriUtils.createUri(variantQueryCommandOptions.commonQueryOptions.output);

            if (variantQueryCommandOptions.annotations != null) {
                options.add("annotations", variantQueryCommandOptions.annotations);
            }
            VariantWriterFactory.VariantOutputFormat of = VariantWriterFactory
                    .toOutputFormat(variantQueryCommandOptions.outputFormat, variantQueryCommandOptions.commonQueryOptions.output);
            variantStorageEngine.exportData(uri, of, query, options);
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
        ObjectMap options = configuration.getStorageEngine(storageEngine).getVariant().getOptions();
        if (annotateVariantsCommandOptions.annotator != null) {
            options.put(VariantAnnotationManager.ANNOTATOR, annotateVariantsCommandOptions.annotator);
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

        String fileName = annotateVariantsCommandOptions.fileName == null
                ? annotateVariantsCommandOptions.dbName
                : annotateVariantsCommandOptions.fileName;
        options.put(DefaultVariantAnnotationManager.FILE_NAME, fileName);

        URI outputUri = UriUtils.createUri(annotateVariantsCommandOptions.outdir == null ? "." : annotateVariantsCommandOptions.outdir);
        Path outDir = Paths.get(outputUri.resolve(".").getPath());

        options.put(DefaultVariantAnnotationManager.OUT_DIR, outDir.toString());

        options.putAll(annotateVariantsCommandOptions.commonOptions.params);

        /*
         * Annotation options
         */
        Query query = new Query();
        if (annotateVariantsCommandOptions.filterRegion != null) {
            query.put(VariantQueryParam.REGION.key(), annotateVariantsCommandOptions.filterRegion);
        }
        if (annotateVariantsCommandOptions.filterGene != null) {
            query.put(VariantQueryParam.GENE.key(), annotateVariantsCommandOptions.filterGene);
        }
        if (annotateVariantsCommandOptions.filterAnnotConsequenceType != null) {
            query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(),
                    annotateVariantsCommandOptions.filterAnnotConsequenceType);
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

    private void copyAnnotation() throws VariantAnnotatorException, StorageEngineException {
        StorageVariantCommandOptions.CreateAnnotationSnapshotCommandOptions cliOptions = variantCommandOptions.createAnnotationSnapshotCommandOptions;

        ObjectMap options = storageConfiguration.getVariant().getOptions();
        options.putAll(cliOptions.commonOptions.params);

        variantStorageEngine.createAnnotationSnapshot(cliOptions.name, options);
    }

    private void deleteAnnotation() throws VariantAnnotatorException, StorageEngineException {
        StorageVariantCommandOptions.DeleteAnnotationSnapshotCommandOptions cliOptions = variantCommandOptions.deleteAnnotationSnapshotCommandOptions;

        ObjectMap options = storageConfiguration.getVariant().getOptions();
        options.putAll(cliOptions.commonOptions.params);

        variantStorageEngine.deleteAnnotationSnapshot(cliOptions.name, options);
    }

    private void queryAnnotation() throws VariantAnnotatorException, StorageEngineException, IOException {
        StorageVariantCommandOptions.QueryAnnotationCommandOptions cliOptions  = variantCommandOptions.queryAnnotationCommandOptions;

        QueryOptions options = new QueryOptions();
        options.put(QueryOptions.LIMIT, cliOptions.limit);
        options.put(QueryOptions.SKIP, cliOptions.skip);
        options.put(QueryOptions.INCLUDE, cliOptions.dataModelOptions.include);
        options.put(QueryOptions.EXCLUDE, cliOptions.dataModelOptions.exclude);
        options.putAll(cliOptions.commonOptions.params);

        Query query = new Query();
        query.put(VariantQueryParam.REGION.key(), cliOptions.region);
        query.put(VariantQueryParam.ID.key(), cliOptions.id);

        QueryResult<VariantAnnotation> queryResult = variantStorageEngine.getAnnotation(cliOptions.name, query, options);

        // WRITE
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        ObjectWriter writer = objectMapper.writer();
//        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        SequenceWriter sequenceWriter = writer.writeValues(System.out);
        for (VariantAnnotation annotation : queryResult.getResult()) {
            sequenceWriter.write(annotation);
            sequenceWriter.flush();
//            writer.writeValue(System.out, annotation);
            System.out.println();
        }
    }

    private void stats() throws IOException, URISyntaxException, StorageEngineException, IllegalAccessException, InstantiationException,
            ClassNotFoundException {
        StorageVariantCommandOptions.VariantStatsCommandOptions statsVariantsCommandOptions = variantCommandOptions.statsVariantsCommandOptions;

        QueryOptions options = new QueryOptions(storageConfiguration.getVariant().getOptions());
        options.put(VariantStorageEngine.Options.OVERWRITE_STATS.key(), statsVariantsCommandOptions.overwriteStats);
        options.put(VariantStorageEngine.Options.UPDATE_STATS.key(), statsVariantsCommandOptions.updateStats);
//        options.putIfNotEmpty(VariantStorageEngine.Options.FILE_ID.key(), statsVariantsCommandOptions.file);
        options.put(VariantStorageEngine.Options.STUDY.key(), statsVariantsCommandOptions.study);
//        if (statsVariantsCommandOptions.studyConfigurationFile != null && !statsVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
//            options.put(FileStudyConfigurationAdaptor.STUDY_CONFIGURATION_PATH, statsVariantsCommandOptions.studyConfigurationFile);
//        }
        options.put(VariantQueryParam.REGION.key(), statsVariantsCommandOptions.region);
        options.put(VariantStorageEngine.Options.RESUME.key(), statsVariantsCommandOptions.resume);

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

        options.put(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), statsVariantsCommandOptions.aggregated);

        if (statsVariantsCommandOptions.aggregationMappingFile != null) {
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(statsVariantsCommandOptions.aggregationMappingFile));
                options.put(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
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

        /*
         * Create and load stats
         */
        if (cohorts == null || cohorts.values().stream().allMatch(Set::isEmpty)) {
            variantStorageEngine.calculateStats(studyName, cohortNames, options);
        } else {
            variantStorageEngine.calculateStats(studyName, cohorts, options);
        }
    }

    private void fillGaps() throws StorageEngineException {
        StorageVariantCommandOptions.FillGapsCommandOptions fillGapsCommandOptions = variantCommandOptions.fillGapsCommandOptions;

        ObjectMap options = storageConfiguration.getVariant().getOptions();
        options.put(VariantStorageEngine.Options.RESUME.key(), fillGapsCommandOptions.resume);
        options.putAll(fillGapsCommandOptions.commonOptions.params);

        variantStorageEngine.fillGaps(fillGapsCommandOptions.study, fillGapsCommandOptions.samples, options);
    }

    private void fillMissing() throws StorageEngineException {
        StorageVariantCommandOptions.FillMissingCommandOptions cliOptions = variantCommandOptions.fillMissingCommandOptions;

        ObjectMap options = storageConfiguration.getVariant().getOptions();
        options.put(VariantStorageEngine.Options.RESUME.key(), cliOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantStorageEngine.fillMissing(cliOptions.study, options, cliOptions.overwrite);
    }

    private void export() throws URISyntaxException, StorageEngineException, IOException {
        StorageVariantCommandOptions.VariantExportCommandOptions exportVariantsCommandOptions = variantCommandOptions.exportVariantsCommandOptions;
//
//        ObjectMap options = storageConfiguration.getVariant().getOptions();
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


//        storageConfiguration.getVariant().getOptions().putAll(exportVariantsCommandOptions.commonOptions.params);


        List<String> studyNames = variantStorageEngine.getStudyConfigurationManager().getStudyNames(new QueryOptions());


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
        VariantSearchManager variantSearchManager = new VariantSearchManager(variantStorageEngine.getStudyConfigurationManager(),
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
            variantStorageEngine.searchIndex(query, options);
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
                    queryOptions.put(QueryOptions.FACET_RANGE, searchOptions.facetRange);
                    FacetedQueryResult facetedQueryResult = variantSearchManager.facetedQuery(dbName, query, queryOptions);
                    if (facetedQueryResult.getResult().getFields() != null
                            && CollectionUtils.isNotEmpty(facetedQueryResult.getResult().getFields())) {
                        System.out.println("Faceted fields (" + facetedQueryResult.getResult().getFields().size() + "):");
                        facetedQueryResult.getResult().getFields().forEach(f -> System.out.println(f.toString()));
                    }
                    if (facetedQueryResult.getResult().getRanges() != null
                            && CollectionUtils.isNotEmpty(facetedQueryResult.getResult().getRanges())) {
                        System.out.println("Faceted ranges (" + facetedQueryResult.getResult().getRanges().size() + "):");
                        facetedQueryResult.getResult().getRanges().forEach(f -> System.out.println(f.toString()));
                    }
                    if (facetedQueryResult.getResult().getIntersections() != null
                            && CollectionUtils.isNotEmpty(facetedQueryResult.getResult().getIntersections())) {
                        System.out.println("Faceted intersections (" + facetedQueryResult.getResult().getIntersections().size() + "):");
                        facetedQueryResult.getResult().getIntersections().forEach(f -> System.out.println(f.toString()));
                    }
                } else {
                    queryOptions.put(QueryOptions.LIMIT, Integer.MAX_VALUE);
                    queryOptions.put(QueryOptions.SKIP, 0);

                    VariantSolrIterator iterator = variantSearchManager.iterator(dbName, query, queryOptions);
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
        QueryResult rank = variantStorageEngine.rank(query, field, limit, asc);
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
