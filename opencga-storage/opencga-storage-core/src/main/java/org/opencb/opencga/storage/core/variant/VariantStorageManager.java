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

package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.runner.StringDataReader;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager extends StorageManager<VariantWriter, VariantDBAdaptor> {

//    public static final String INCLUDE_STATS = "include.stats";              //Include existing stats on the original file.
//    public static final String INCLUDE_GENOTYPES = "include.genotypes";      //Include sample information (genotypes)
//    public static final String INCLUDE_SRC = "include.src";                  //Include original source file on the transformed file and the final db
//    public static final String COMPRESS_GENOTYPES = "compressGenotypes";    //Stores sample information as compressed genotypes
//    public static final String STUDY_CONFIGURATION = "studyConfiguration";      //
//    public static final String STUDY_CONFIGURATION_MANAGER_CLASS_NAME         = "studyConfigurationManagerClassName";
//    public static final String AGGREGATED_TYPE = "aggregatedType";
//    public static final String STUDY_NAME = "studyName";
//    public static final String STUDY_ID = "studyId";
//    public static final String FILE_ID = "fileId";
//    public static final String STUDY_TYPE = "studyType";
//    public static final String SAMPLE_IDS = "sampleIds";
//    public static final String COMPRESS_METHOD = "compressMethod";
//
//    public static final String CALCULATE_STATS = "calculateStats";          //Calculate stats on the postLoad step
//    public static final String OVERWRITE_STATS = "overwriteStats";          //Overwrite stats already present
//    public static final String AGGREGATION_MAPPING_PROPERTIES = "aggregationMappingFile";
//
//    public static final String DB_NAME = "database.name";
//
//    public static final String TRANSFORM_BATCH_SIZE = "transform.batch.size";
//    public static final String LOAD_BATCH_SIZE = "load.batch.size";
//    public static final String TRANSFORM_THREADS = "transform.threads";
//    public static final String LOAD_THREADS = "load.threads";
//    public static final String ANNOTATE = "annotate";

    public enum Options {
        INCLUDE_STATS ("include.stats", true),              //Include existing stats on the original file.
        @Deprecated
        INCLUDE_GENOTYPES ("include.genotypes", true),      //Include sample information (genotypes)
        @Deprecated
        INCLUDE_SRC ("include.src", false),                  //Include original source file on the transformed file and the final db
//        COMPRESS_GENOTYPES ("compressGenotypes", true),    //Stores sample information as compressed genotypes

        STUDY_CONFIGURATION ("studyConfiguration", ""),      //
        STUDY_CONFIGURATION_MANAGER_CLASS_NAME ("studyConfigurationManagerClassName", ""),

        STUDY_TYPE ("studyType", VariantStudy.StudyType.CASE_CONTROL),
        AGGREGATED_TYPE ("aggregatedType", VariantSource.Aggregation.NONE),
        STUDY_NAME ("studyName", ""),
        STUDY_ID ("studyId", ""),
        FILE_ID ("fileId", ""),
        SAMPLE_IDS ("sampleIds", ""),

        COMPRESS_METHOD ("compressMethod", "gzip"),
        AGGREGATION_MAPPING_PROPERTIES ("aggregationMappingFile", null),
        DB_NAME ("database.name", "opencga"),

        TRANSFORM_BATCH_SIZE ("transform.batch.size", 200),
        TRANSFORM_THREADS ("transform.threads", 4),
        LOAD_BATCH_SIZE ("load.batch.size", 100),
        LOAD_THREADS ("load.threads", 4),

        CALCULATE_STATS ("calculateStats", false),          //Calculate stats on the postLoad step
        OVERWRITE_STATS ("overwriteStats", false),          //Overwrite stats already present
        ANNOTATE ("annotate", false);

        private final String key;
        private final Object value;

        Options(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public <T> T defaultValue() {
            return (T) value;
        }

    }

//    protected static Logger logger;

    public VariantStorageManager() {
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    public VariantStorageManager(StorageConfiguration configuration) {
        super(configuration);
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    public VariantStorageManager(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    @Override
    public URI extract(URI input, URI ouput) {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        ObjectMap variantOptions = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(variantOptions);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            studyConfiguration = new StudyConfiguration(variantOptions.getInt(Options.STUDY_ID.key), variantOptions.getString(Options.STUDY_NAME.key));
            studyConfiguration.setAggregation(variantOptions.get(Options.AGGREGATED_TYPE.key, VariantSource.Aggregation.class));
            variantOptions.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);
        }
        String fileName = Paths.get(input.getPath()).getFileName().toString();
        Integer fileId = variantOptions.getInt(Options.FILE_ID.key);    //TODO: Transform into an optional field

        checkNewFile(studyConfiguration, fileId, fileName);

        return input;
    }

    /**
     * Transform raw variant files into biodata model.
     *
     * @param inputUri         Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigreeUri      Pedigree input file. Accepted formats: *.ped
     * @param outputUri
     * @throws IOException
     */
    @Override
    final public URI transform(URI inputUri, URI pedigreeUri, URI outputUri) throws StorageManagerException {
        // input: VcfReader
        // output: JsonWriter

        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        Path input = Paths.get(inputUri.getPath());
        Path pedigree = pedigreeUri == null ? null : Paths.get(pedigreeUri.getPath());
        Path output = Paths.get(outputUri.getPath());

        boolean includeSamples = options.getBoolean(Options.INCLUDE_GENOTYPES.key, false);
        boolean includeStats = options.getBoolean(Options.INCLUDE_STATS.key, false);
        boolean includeSrc = options.getBoolean(Options.INCLUDE_SRC.key, Options.INCLUDE_SRC.defaultValue());

        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        Integer fileId = options.getInt(Options.FILE_ID.key);    //TODO: Transform into an optional field
        VariantSource.Aggregation aggregation = options.get(Options.AGGREGATED_TYPE.key, VariantSource.Aggregation.class, Options.AGGREGATED_TYPE.defaultValue());
        String fileName = input.getFileName().toString();
        VariantStudy.StudyType type = options.get(Options.STUDY_TYPE.key, VariantStudy.StudyType.class, Options.STUDY_TYPE.defaultValue());
        VariantSource source = new VariantSource(
                fileName,
                fileId.toString(),
                Integer.toString(studyConfiguration.getStudyId()),
                studyConfiguration.getStudyName(), type, aggregation);

       
       
        int batchSize = options.getInt(Options.TRANSFORM_BATCH_SIZE.key, Options.TRANSFORM_BATCH_SIZE.defaultValue());

        String compression = options.getString(Options.COMPRESS_METHOD.key, Options.COMPRESS_METHOD.defaultValue());
        String extension = "";
        int numThreads = options.getInt(Options.TRANSFORM_THREADS.key, Options.TRANSFORM_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", numThreads*2);

        if (compression.equalsIgnoreCase("gzip") || compression.equalsIgnoreCase("gz")) {
            extension = ".gz";
        } else if (compression.equalsIgnoreCase("snappy") || compression.equalsIgnoreCase("snz")) {
            extension = ".snappy";
        } else if (!compression.isEmpty()) {
            throw new IllegalArgumentException("Unknown compression method " + compression);
        }

        // TODO Create a utility to determine which extensions are variants files
        final VariantVcfFactory factory;
        if (fileName.endsWith(".vcf") || fileName.endsWith(".vcf.gz") || fileName.endsWith(".vcf.snappy")) {
            if (VariantSource.Aggregation.NONE.equals(aggregation)) {
                factory = new VariantVcfFactory();
            } else {
                factory = new VariantAggregatedVcfFactory();
            }
        } else {
            throw new StorageManagerException("Variants input file format not supported");
        }


        Path outputVariantJsonFile = output.resolve(fileName + ".variants.json" + extension);
        Path outputFileJsonFile = output.resolve(fileName + ".file.json" + extension);

        logger.info("Transforming variants...");
        long start, end;
        if (numThreads == 1) { //Run transformation with a SingleThread runner. The legacy way
            if (!extension.equals(".gz")) { //FIXME: Add compatibility with snappy compression
                logger.warn("Force using gzip compression");
                extension = ".gz";
                outputVariantJsonFile = output.resolve(fileName + ".variants.json" + extension);
                outputFileJsonFile = output.resolve(fileName + ".file.json" + extension);
            }

            //Ped Reader
            PedigreeReader pedReader = null;
            if(pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
                pedReader = new PedigreePedReader(pedigree.toString());
            }

            //Reader
            VariantReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());

            //Writers
            VariantJsonWriter jsonWriter = new VariantJsonWriter(source, output);
            jsonWriter.includeSrc(includeSrc);
            jsonWriter.includeSamples(includeSamples);
            jsonWriter.includeStats(includeStats);

            List<VariantWriter> writers = Collections.<VariantWriter>singletonList(jsonWriter);

            //Runner
            VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, Collections.<Task<Variant>>emptyList(), batchSize);

            logger.info("Single thread transform...");
            start = System.currentTimeMillis();
            try {
                vr.run();
            } catch (IOException e) {
                throw new StorageManagerException("Fail runner execution", e);
            }
            end = System.currentTimeMillis();

        } else {
            //Read VariantSource
            source = readVariantSource(input, source);

            //Reader
            StringDataReader dataReader = new StringDataReader(input);

            //Writers
            StringDataWriter dataWriter = new StringDataWriter(outputVariantJsonFile);

            final VariantSource finalSource = source;
            final Path finalOutputFileJsonFile = outputFileJsonFile;
            ParallelTaskRunner<String, String> ptr;
            try {
                ptr = new ParallelTaskRunner<>(
                        dataReader,
                        new VariantJsonTransformTask(factory, finalSource, finalOutputFileJsonFile),
                        dataWriter,
                        new ParallelTaskRunner.Config(numThreads, batchSize, capacity, false)
                );
            } catch (Exception e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
            }
            logger.info("Multi thread transform...");
            start = System.currentTimeMillis();
            try {
                ptr.run();
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while executing TransformVariants in ParallelTaskRunner", e);
            }
            end = System.currentTimeMillis();
        }
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants transformed!");

        return outputUri.resolve(outputVariantJsonFile.getFileName().toString());
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            studyConfiguration = new StudyConfiguration(options.getInt(Options.STUDY_ID.key), options.getString(Options.STUDY_NAME.key));
            options.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);
        }

        //TODO: Expect JSON file
        VariantSource source = readVariantSource(Paths.get(input.getPath()), null);

        /*
         * Before load file, check and add fileName to the StudyConfiguration.
         * FileID and FileName is read from the VariantSource
         * Will fail if:
         *     fileId is not an integer
         *     fileId was already in the studyConfiguration.indexedFiles
         *     fileId was already in the studyConfiguration.fileIds with a different fileName
         *     fileName was already in the studyConfiguration.fileIds with a different fileId
         */

        int fileId;
        String fileName = source.getFileName();
        try {
            fileId = Integer.parseInt(source.getFileId());
        } catch (NumberFormatException e) {
            throw new StorageManagerException("FileId " + source.getFileId() + " is not an integer", e);
        }

        checkNewFile(studyConfiguration, fileId, fileName);
        studyConfiguration.getFileIds().put(source.getFileName(), fileId);
        studyConfiguration.getHeaders().put(fileId, source.getMetadata().get("variantFileHeader").toString());

        /*
         * Before load file, the StudyConfiguration has to be updated with the new sample names.
         *  Will read param SAMPLE_IDS like [<sampleName>:<sampleId>,]*
         *  If SAMPLE_IDS is missing, will auto-generate sampleIds
         *  Will fail if:
         *      param SAMPLE_IDS is malformed
         *      any given sampleId is not an integer
         *      any given sampleName is not in the input file
         *      any given sampleName was already in the StudyConfiguration (so, was already loaded)
         *      some sample was missing in the given SAMPLE_IDS param
         */

        if (options.containsKey(Options.SAMPLE_IDS.key) && !options.getAsStringList(Options.SAMPLE_IDS.key).isEmpty()) {
            for (String sampleEntry : options.getAsStringList(Options.SAMPLE_IDS.key)) {
                String[] split = sampleEntry.split(":");
                if (split.length != 2) {
                    throw new StorageManagerException("Param " + sampleEntry + " is malformed");
                }
                String sampleName = split[0];
                int sampleId;
                try {
                    sampleId = Integer.parseInt(split[1]);
                } catch (NumberFormatException e) {
                    throw new StorageManagerException("SampleId " + split[1] + " is not an integer", e);
                }

                if (!source.getSamplesPosition().containsKey(sampleName)) {
                    //ERROR
                    throw new StorageManagerException("Given sampleName is not in the input file");
                } else {
                    if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                        //Add sample to StudyConfiguration
                        studyConfiguration.getSampleIds().put(sampleName, sampleId);
                    } else {
                        if (studyConfiguration.getSampleIds().get(sampleName) == sampleId) {
                            //throw new StorageManagerException("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
//                            logger.warn("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
                        } else {
                            throw new StorageManagerException("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration with a different sampleId: " + studyConfiguration.getSampleIds().get(sampleName));
                        }
                    }
                }
            }

            //Check that all samples has a sampleId
            List<String> missingSamples = new LinkedList<>();
            for (String sampleName : source.getSamples()) {
                if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                    missingSamples.add(sampleName);
                } /*else {
                    Integer sampleId = studyConfiguration.getSampleIds().get(sampleName);
                    if (studyConfiguration.getIndexedSamples().contains(sampleId)) {
                        logger.warn("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration.indexedSamples");
                    }
                }*/
            }
            if (!missingSamples.isEmpty()) {
                throw new StorageManagerException("Samples " + missingSamples.toString() + " has not assigned sampleId");
            }

        } else {
            //Find the grader sample Id in the studyConfiguration, in order to add more sampleIds if necessary.
            int maxId = 0;
            for (Integer i : studyConfiguration.getSampleIds().values()) {
                if (i > maxId) {
                    maxId = i;
                }
            }
            //Assign new sampleIds
            for (String sample : source.getSamples()) {
                if (!studyConfiguration.getSampleIds().containsKey(sample)) {
                    //If the sample was not in the original studyId, a new SampleId is assigned.

                    int sampleId;
                    int samplesSize = studyConfiguration.getSampleIds().size();
                    Integer samplePosition = source.getSamplesPosition().get(sample);
                    if (!studyConfiguration.getSampleIds().containsValue(samplePosition)) {
                        //1- Use with the SamplePosition
                        sampleId = samplePosition;
                    } else if (!studyConfiguration.getSampleIds().containsValue(samplesSize)) {
                        //2- Use the number of samples in the StudyConfiguration.
                        sampleId = samplesSize;
                    } else {
                        //3- Use the maxId
                        sampleId = maxId + 1;
                    }
                    studyConfiguration.getSampleIds().put(sample, sampleId);
                    if (sampleId > maxId) {
                        maxId = sampleId;
                    }
                }
            }
        }

        if (studyConfiguration.getSamplesInFiles().containsKey(fileId)) {
            Set<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
            List<String> missingSamples = new LinkedList<>();
            for (String sampleName : source.getSamples()) {
                if (!sampleIds.contains(studyConfiguration.getSampleIds().get(sampleName))) {
                    missingSamples.add(sampleName);
                }
            }
            if (!missingSamples.isEmpty()) {
                throw new StorageManagerException("Samples "  + missingSamples.toString() + "where not in file " + fileId);
            }
            if (sampleIds.size() != source.getSamples().size()) {
                throw new StorageManagerException("Incorrect number of samples in file " + fileId);
            }
        } else {
            Set<Integer> sampleIdsInFile = new HashSet<>();
            for (String sample : source.getSamples()) {
                sampleIdsInFile.add(studyConfiguration.getSampleIds().get(sample));
            }
            studyConfiguration.getSamplesInFiles().put(fileId, sampleIdsInFile);
        }

        options.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);

        return input;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        String dbName = options.getString(Options.DB_NAME.key, null);
        int fileId = options.getInt(Options.FILE_ID.key);
        boolean annotate = options.getBoolean(Options.ANNOTATE.key, Options.ANNOTATE.defaultValue());

        //Update StudyConfiguration
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        studyConfiguration.getIndexedFiles().add(fileId);
        getStudyConfigurationManager(options).updateStudyConfiguration(studyConfiguration, new QueryOptions());


//        VariantSource variantSource = params.get(VARIANT_SOURCE, VariantSource.class);

        if (annotate) {

            VariantAnnotator annotator;
            try {
                annotator = VariantAnnotationManager.buildVariantAnnotator(configuration, storageEngineId);
            } catch (VariantAnnotatorException e) {
                e.printStackTrace();
                logger.error("Can't annotate variants." , e);
                return input;
            }

            VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(annotator, getDBAdaptor(dbName));

            QueryOptions annotationOptions = new QueryOptions();
            Query annotationQuery = new Query();
            if (!options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, false)) {
                annotationOptions.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
            }
            annotationOptions.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), Collections.singletonList(fileId));    // annotate just the indexed variants

            annotationOptions.add(VariantAnnotationManager.OUT_DIR, output.getPath());
            annotationOptions.add(VariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());
            variantAnnotationManager.annotate(annotationQuery, annotationOptions);
//            URI annotationFile = variantAnnotationManager.createAnnotation(Paths.get(output.getPath()), dbName + "." + TimeUtils.getTime(), annotationOptions);
//            variantAnnotationManager.loadAnnotation(annotationFile, annotationOptions);
        }

        if (options.getBoolean(Options.CALCULATE_STATS.key, Options.CALCULATE_STATS.defaultValue())) {
            // TODO add filters
            try {
                logger.debug("about to calculate stats");
                VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
                VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName);
                URI statsOutputUri = output.resolve(buildFilename(studyConfiguration.getStudyName(), fileId) + "." + TimeUtils.getTime());

                String defaultCohortName = VariantSourceEntry.DEFAULT_COHORT;
                Map<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
                Map<String, Set<String>> defaultCohort = Collections.singletonMap(defaultCohortName, indexedSamples.keySet());
                if (studyConfiguration.getCohortIds().containsKey(defaultCohortName)) { //Check if "defaultCohort" exists
                    Integer defaultCohortId = studyConfiguration.getCohortIds().get(defaultCohortName);
                    if (studyConfiguration.getCalculatedStats().contains(defaultCohortId)) { //Check if "defaultCohort" is calculated
                        if (!indexedSamples.values().equals(studyConfiguration.getCohorts().get(defaultCohortId))) { //Check if the samples number are different
                            logger.debug("Cohort \"{}\":{} was already calculated. Invalidating stats to recalculate.", defaultCohortName, defaultCohortId);
                            studyConfiguration.getCalculatedStats().remove(defaultCohortId);
                            studyConfiguration.getInvalidStats().add(defaultCohortId);
                        }
                    }
                }

                URI statsUri = variantStatisticsManager.createStats(dbAdaptor, statsOutputUri, defaultCohort, Collections.emptyMap(), studyConfiguration, new QueryOptions(options));
                variantStatisticsManager.loadStats(dbAdaptor, statsUri, studyConfiguration, new QueryOptions(options));
            } catch (Exception e) {
                logger.error("Can't calculate stats." , e);
                e.printStackTrace();
            }
        }

        return input;
    }

    @Override
    public boolean testConnection(String dbName) {
        return true;
    }

    public static String buildFilename(String studyName, int fileId) {
        return studyName + "_" + fileId;
    }

    public static VariantSource readVariantSource(Path input, VariantSource source) throws StorageManagerException {
        if (source == null) {
            source = new VariantSource("", "", "", "");
        }
        if (input.toFile().getName().contains("json")) {
            try {
                VariantJsonReader reader = getVariantJsonReader(input, source);
                reader.open();
                reader.pre();
                reader.post();
                reader.close();
            } catch (IOException e) {
                throw new StorageManagerException("Can not read VariantSource from " + input, e);
            }
        } else {
            VariantReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
            reader.open();
            reader.pre();
            source.addMetadata("variantFileHeader", reader.getHeader());
            reader.post();
            reader.close();
        }
        return source;
    }

    protected static VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws IOException {
        VariantJsonReader variantJsonReader;
        if (    input.toString().endsWith(".json") ||
                input.toString().endsWith(".json.gz") ||
                input.toString().endsWith(".json.snappy") ||
                input.toString().endsWith(".json.snz")) {
            String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw new IOException("Variants input file format not supported for file: " + input);
        }
        return variantJsonReader;
    }

    /* --------------------------------------- */
    /*  StudyConfiguration utils methods        */
    /* --------------------------------------- */

    final public StudyConfiguration getStudyConfiguration(ObjectMap params) throws StorageManagerException {
        if (params.containsKey(Options.STUDY_CONFIGURATION.key)) {
            return params.get(Options.STUDY_CONFIGURATION.key, StudyConfiguration.class);
        } else {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager(params);
            StudyConfiguration studyConfiguration;
            if (params.containsKey(Options.STUDY_NAME.key)) {
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(params.getString(Options.STUDY_NAME.key), new QueryOptions(params)).first();
            } else if (params.containsKey(Options.STUDY_ID.key)) {
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(params.getInt(Options.STUDY_ID.key), new QueryOptions(params)).first();
            } else {
                throw new StorageManagerException("Unable to get StudyConfiguration. Missing studyId or studyName");
            }
            params.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);
            return studyConfiguration;
        }
    }

    /**
     * Get the StudyConfigurationManager.
     *
     *  If there is no StudyConfigurationManager, try to build by dependency injection.
     *  If can't build, call to the method "buildStudyConfigurationManager", witch could be override.
     *
     * @param options
     * @return
     */
    final public StudyConfigurationManager getStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        StudyConfigurationManager studyConfigurationManager = null;
        if (studyConfigurationManager == null) {

            // We read the StudyMetadataManager from the passed 'options', if not exists then we read configuration file
            String studyConfigurationManagerClassName = null;
            if (options.containsKey(Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key)) {
                studyConfigurationManagerClassName = options.getString(Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key);
            } else {
                if (configuration.getStudyMetadataManager() != null && !configuration.getStudyMetadataManager().isEmpty()) {
                    studyConfigurationManagerClassName = configuration.getStudyMetadataManager();
                }
            }

            if (studyConfigurationManagerClassName != null && !studyConfigurationManagerClassName.isEmpty()) {
                try {
                    studyConfigurationManager = StudyConfigurationManager.build(studyConfigurationManagerClassName, options);
                } catch (ReflectiveOperationException e) {
                    e.printStackTrace();
                    logger.error("Error creating a StudyConfigurationManager. Creating default StudyConfigurationManager", e);
                    throw new RuntimeException(e);
                }
            }
            // This method can be override in children methods
            if (studyConfigurationManager == null) {
                studyConfigurationManager = buildStudyConfigurationManager(options);
            }
        }
        return studyConfigurationManager;
    }

    /**
     * Build the default StudyConfigurationManager. This method could be override by children classes if they want to use other class.
     *
     * @param options
     * @return
     */
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        return new FileStudyConfigurationManager(options);
    }

    /**
     * Check if the file(name,id) can be added to the StudyConfiguration.
     * Will fail if:
     *     fileName was already in the studyConfiguration.fileIds with a different fileId
     *     fileId was already in the studyConfiguration.fileIds with a different fileName
     *     fileId was already in the studyConfiguration.indexedFiles
     */
    protected void checkNewFile(StudyConfiguration studyConfiguration, int fileId, String fileName) throws StorageManagerException {
        Map<Integer, String> idFiles = StudyConfiguration.inverseMap(studyConfiguration.getFileIds());

        if (studyConfiguration.getFileIds().containsKey(fileName)) {
            if (studyConfiguration.getFileIds().get(fileName) != fileId) {
                throw new StorageManagerException("FileName " + fileName + " have a different fileId in the StudyConfiguration: " +
                        "(" + fileName + ":" + studyConfiguration.getFileIds().get(fileName) + ")");
            }
        }
        if (idFiles.containsKey(fileId)) {
            if (!idFiles.get(fileId).equals(fileName)) {
                throw new StorageManagerException("FileId " + fileId + " has a different fileName in the StudyConfiguration: " +
                        "(" + idFiles.containsKey(fileId) + ":" + fileId + ")");
            }
        }
        if (studyConfiguration.getIndexedFiles().contains(fileId)) {
            throw new StorageManagerException("File (" + fileName + ":" + fileId + ")"
                    + " was already already loaded ");
        }
    }

    /**
     * Check if the StudyConfiguration is correct
     * @param studyConfiguration    StudyConfiguration to check
     */
    public static void checkStudyConfiguration(StudyConfiguration studyConfiguration) throws StorageManagerException {
        if (studyConfiguration == null) {
            throw new StorageManagerException("StudyConfiguration is null");
        }
        if (studyConfiguration.getFileIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getFileIds()).size() ) {
            throw new StorageManagerException("StudyConfiguration has duplicated fileIds");
        }
        if (studyConfiguration.getCohortIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).size() ) {
            throw new StorageManagerException("StudyConfiguration has duplicated cohortIds");
        }
    }

}
