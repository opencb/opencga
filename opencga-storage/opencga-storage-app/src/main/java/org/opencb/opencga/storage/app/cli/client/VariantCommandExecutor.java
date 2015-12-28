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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.utils.FileUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private StorageEngineConfiguration storageConfiguration;
    private VariantStorageManager variantStorageManager;

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
        this.variantCommandOptions = variantCommandOptions;
    }

    private void init(CliOptionsParser.CommonOptions commonOptions) throws Exception {

        this.logFile = commonOptions.logFile;

        // Call to super init() method
        super.init(commonOptions.logLevel, commonOptions.verbose, commonOptions.configFile);

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

        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
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
                init(variantCommandOptions.indexVariantsCommandOptions.commonOptions);
                index();
                break;
            case "query":
                init(variantCommandOptions.queryVariantsCommandOptions.commonOptions);
                query();
                break;
            case "annotation":
                init(variantCommandOptions.annotateVariantsCommandOptions.commonOptions);
                annotation();
                break;
            case "stats":
                init(variantCommandOptions.statsVariantsCommandOptions.commonOptions);
                stats();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void index() throws URISyntaxException, IOException, StorageManagerException, FileFormatException {
        CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions = variantCommandOptions.indexVariantsCommandOptions;
        URI variantsUri = UriUtils.createUri(indexVariantsCommandOptions.input);
        if (variantsUri.getScheme().startsWith("file") || variantsUri.getScheme().isEmpty()) {
            FileUtils.checkFile(Paths.get(variantsUri));
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
                : variantsUri.resolve(".");
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
        URI nextFileUri = variantsUri;
        boolean extract, transform, load;

        if (!indexVariantsCommandOptions.load && !indexVariantsCommandOptions.transform) {
            extract = true;
            transform = true;
            load = true;
        } else {
            extract = indexVariantsCommandOptions.transform;
            transform = indexVariantsCommandOptions.transform;
            load = indexVariantsCommandOptions.load;
        }

        // Check the database connection before we start
        if (load) {
            if (!variantStorageManager.testConnection(variantOptions.getString(VariantStorageManager.Options.DB_NAME.key()))) {
                logger.error("Connection to database '{}' failed", variantOptions.getString(VariantStorageManager.Options.DB_NAME.key()));
                throw new ParameterException("Database connection test failed");
            }
        }

        if (extract) {
            logger.info("Extract variants '{}'", variantsUri);
            nextFileUri = variantStorageManager.extract(variantsUri, outdirUri);
        }

        if (transform) {
            logger.info("PreTransform variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.preTransform(nextFileUri);
            logger.info("Transform variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri);
            logger.info("PostTransform variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.postTransform(nextFileUri);
        }

        if (load) {
            logger.info("PreLoad variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri);
            logger.info("Load variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.load(nextFileUri);
            logger.info("PostLoad variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri);
        }
    }

    private void query() throws Exception {
        CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions = variantCommandOptions.queryVariantsCommandOptions;
        storageConfiguration.getVariant().getOptions().putAll(queryVariantsCommandOptions.commonOptions.params);
//        VariantStorageManager variantStorageManager = new StorageManagerFactory(configuration).getVariantStorageManager
// (queryVariantsCommandOptions.backend);

        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(queryVariantsCommandOptions.dbName);
        List<String> studyNames = variantDBAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions());

        Query query = new Query();
        QueryOptions options = new QueryOptions(new HashMap<>(queryVariantsCommandOptions.commonOptions.params));


        /**
         * Parse Variant parameters
         */
        if (queryVariantsCommandOptions.region != null && !queryVariantsCommandOptions.region.isEmpty()) {
            query.put(REGION.key(), queryVariantsCommandOptions.region);
        } else if (queryVariantsCommandOptions.regionFile != null && !queryVariantsCommandOptions.regionFile.isEmpty()) {
            Path gffPath = Paths.get(queryVariantsCommandOptions.regionFile);
            FileUtils.checkFile(gffPath);
            String regionsFromFile = Files.readAllLines(gffPath).stream().map(line -> {
                String[] array = line.split("\t");
                return new String(array[0].replace("chr", "") + ":" + array[3] + "-" + array[4]);
            }).collect(Collectors.joining(","));
            query.put(REGION.key(), regionsFromFile);
        }

        addParam(query, ID, queryVariantsCommandOptions.id);
        addParam(query, GENE, queryVariantsCommandOptions.gene);
        addParam(query, TYPE, queryVariantsCommandOptions.type);


        List<String> studies = new LinkedList<>();
        if (queryVariantsCommandOptions.study != null && !queryVariantsCommandOptions.study.isEmpty()) {
            query.put(STUDIES.key(), queryVariantsCommandOptions.study);
            for (String study : queryVariantsCommandOptions.study.split(",|;")) {
                if (!study.startsWith("!")) {
                    studies.add(study);
                }
            }
        }

        // If the studies to be returned is empty then we return the studies being queried
        if (queryVariantsCommandOptions.returnStudy != null && !queryVariantsCommandOptions.returnStudy.isEmpty()) {
            query.put(RETURNED_STUDIES.key(), Arrays.asList(queryVariantsCommandOptions.returnStudy.split(",")));
        } else {
            if (!studies.isEmpty()) {
                query.put(RETURNED_STUDIES.key(), studies);
            }
        }

        addParam(query, FILES, queryVariantsCommandOptions.file);
        addParam(query, GENOTYPE, queryVariantsCommandOptions.sampleGenotype);
        addParam(query, RETURNED_SAMPLES, queryVariantsCommandOptions.returnSample);
        addParam(query, UNKNOWN_GENOTYPE, queryVariantsCommandOptions.unknownGenotype);

        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_CONSEQUENCE_TYPE, queryVariantsCommandOptions.consequenceType);
        addParam(query, ANNOT_BIOTYPE, queryVariantsCommandOptions.biotype);
        addParam(query, ALTERNATE_FREQUENCY, queryVariantsCommandOptions.populationFreqs);
        addParam(query, CONSERVATION, queryVariantsCommandOptions.conservation);

        if (queryVariantsCommandOptions.proteinSubstitution != null && !queryVariantsCommandOptions.proteinSubstitution.isEmpty()) {
            String[] fields = queryVariantsCommandOptions.proteinSubstitution.split(",");
            for (String field : fields) {
                String[] arr = field
                        .replaceAll("==", " ")
                        .replaceAll(">=", " ")
                        .replaceAll("<=", " ")
                        .replaceAll("=", " ")
                        .replaceAll("<", " ")
                        .replaceAll(">", " ")
                        .split(" ");

                if (arr != null && arr.length > 1) {
                    switch (arr[0]) {
                        case "sift":
                            query.put(SIFT.key(), field.replaceAll("sift", ""));
                            break;
                        case "polyphen":
                            query.put(POLYPHEN.key(), field.replaceAll("polyphen", ""));
                            break;
                        default:
                            query.put(PROTEIN_SUBSTITUTION.key(), field.replaceAll(arr[0], ""));
                            break;
                    }
                }
            }
        }


        /*
         * Stats parameters
         */
        if (queryVariantsCommandOptions.stats != null && !queryVariantsCommandOptions.stats.isEmpty()) {
            Set<String> acceptedStatKeys = new HashSet<>(Arrays.asList(STATS_MAF.key(),
                    STATS_MGF.key(),
                    MISSING_ALLELES.key(),
                    MISSING_GENOTYPES.key()));

            for (String stat : queryVariantsCommandOptions.stats.split(",")) {
                int index = stat.indexOf("<");
                index = index >= 0 ? index : stat.indexOf("!");
                index = index >= 0 ? index : stat.indexOf("~");
                index = index >= 0 ? index : stat.indexOf("<");
                index = index >= 0 ? index : stat.indexOf(">");
                index = index >= 0 ? index : stat.indexOf("=");
                if (index < 0) {
                    throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
                }
                String name = stat.substring(0, index);
                String cond = stat.substring(index);

                if (acceptedStatKeys.contains(name)) {
                    query.put(name, cond);
                } else {
                    throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                }
                logger.info("Parsed stat filter: {} {}", name, cond);
            }
        }

        addParam(query, STATS_MAF, queryVariantsCommandOptions.maf);
        addParam(query, STATS_MGF, queryVariantsCommandOptions.mgf);
        addParam(query, MISSING_ALLELES, queryVariantsCommandOptions.missingAlleleCount);
        addParam(query, MISSING_GENOTYPES, queryVariantsCommandOptions.missingGenotypeCount);


        /*
         * Output parameters
         */
        String outputFormat = "vcf";
        boolean gzip = true;
        if (queryVariantsCommandOptions.outputFormat != null && !queryVariantsCommandOptions.outputFormat.isEmpty()) {
            switch (queryVariantsCommandOptions.outputFormat) {
                case "vcf":
                    gzip = false;
                case "vcf.gz":
                    outputFormat = "vcf";
                    break;
                case "json":
                    gzip = false;
                case "json.gz":
                    outputFormat = "json";
                    break;
                default:
                    logger.error("Format '{}' not supported", queryVariantsCommandOptions.outputFormat);
                    throw new ParameterException("Format '" + queryVariantsCommandOptions.outputFormat + "' not supported");
            }
        }

        boolean returnVariants = !queryVariantsCommandOptions.count
                && StringUtils.isEmpty(queryVariantsCommandOptions.groupBy)
                && StringUtils.isEmpty(queryVariantsCommandOptions.rank);

        if (returnVariants && outputFormat.equalsIgnoreCase("vcf")) {
            int returnedStudiesSize = query.getAsStringList(RETURNED_STUDIES.key()).size();
            if (returnedStudiesSize == 0 && studies.size() == 1) {
                query.put(RETURNED_STUDIES.key(), studies.get(0));
            } else if (returnedStudiesSize == 0 && studyNames.size() != 1 //If there are no returned studies, and there are more than one
                    // study
                    || returnedStudiesSize > 1) {     // Or is required more than one returned study
                throw new Exception("Only one study is allowed when returning VCF, please use '--return-study' to select the returned "
                        + "study. " + "Available studies: [ " + String.join(", ", studyNames) + " ]");
            } else {
                if (returnedStudiesSize == 0) {    //If there were no returned studies, set the study existing one
                    query.put(RETURNED_STUDIES.key(), studyNames.get(0));
                }
            }
        }

        // output format has priority over output name
        OutputStream outputStream;
        if (queryVariantsCommandOptions.output == null || queryVariantsCommandOptions.output.isEmpty()) {
            outputStream = System.out;
        } else {
            if (gzip && !queryVariantsCommandOptions.output.endsWith(".gz")) {
                queryVariantsCommandOptions.output += ".gz";
            }
            logger.debug("writing to %s", queryVariantsCommandOptions.output);
            outputStream = new FileOutputStream(queryVariantsCommandOptions.output);
        }
        if (gzip) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        logger.debug("using %s output stream", gzip ? "gzipped" : "plain");

        /*
         * Setting QueryOptions parameters
         */
        if (queryVariantsCommandOptions.include != null) {
            options.add("include", queryVariantsCommandOptions.include);
        }

        if (queryVariantsCommandOptions.exclude != null) {
            options.add("exclude", queryVariantsCommandOptions.exclude);
        }

        if (queryVariantsCommandOptions.skip > 0) {
            options.add("skip", queryVariantsCommandOptions.skip);
        }

        if (queryVariantsCommandOptions.limit > 0) {
            options.add("limit", queryVariantsCommandOptions.limit);
        }

        if (queryVariantsCommandOptions.count) {
            options.add("count", queryVariantsCommandOptions.count);
        }


        if (queryVariantsCommandOptions.count) {
            QueryResult<Long> result = variantDBAdaptor.count(query);
            System.out.println("Num. results\t" + result.getResult().get(0));
            return;
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
                        VariantVcfExporter variantVcfExporter = new VariantVcfExporter();
                        variantVcfExporter.export(iterator, studyConfigurationResult.first(), outputStream, options);
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
        if (annotateVariantsCommandOptions.species != null) {
            options.put(VariantAnnotationManager.SPECIES, annotateVariantsCommandOptions.species);
        }
        if (annotateVariantsCommandOptions.assembly != null) {
            options.put(VariantAnnotationManager.ASSEMBLY, annotateVariantsCommandOptions.assembly);
        }

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
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions());
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
        StudyConfiguration studyConfiguration = variantStorageManager.getStudyConfiguration(options);
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



    private void addParam(ObjectMap objectMap, VariantDBAdaptor.VariantQueryParams key, String value) {
        if (value != null && !value.isEmpty()) {
            objectMap.put(key.key(), value);
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

}
