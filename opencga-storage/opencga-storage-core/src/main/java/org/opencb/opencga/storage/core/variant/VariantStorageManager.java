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

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.converter.VCFHeaderToAvroVcfHeaderConverter;
import org.opencb.biodata.tools.variant.converter.VariantFileMetadataToVCFHeaderConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;
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
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.core.variant.transform.VariantAvroTransformTask;
import org.opencb.opencga.storage.core.variant.transform.VariantJsonTransformTask;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;

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
        EXTRA_GENOTYPE_FIELDS("include.extra-fields", ""),  //Include other sample information (like DP, GQ, ...)
        @Deprecated
        INCLUDE_SRC ("include.src", false),                  //Include original source file on the transformed file and the final db
//        COMPRESS_GENOTYPES ("compressGenotypes", true),    //Stores sample information as compressed genotypes

        STUDY_CONFIGURATION ("studyConfiguration", ""),      //
        STUDY_CONFIGURATION_MANAGER_CLASS_NAME ("studyConfigurationManagerClassName", ""),

        STUDY_TYPE ("studyType", VariantStudy.StudyType.CASE_CONTROL),
        AGGREGATED_TYPE ("aggregatedType", VariantSource.Aggregation.NONE),
        STUDY_NAME ("studyName", "default"),
        STUDY_ID ("studyId", -1),
        FILE_ID ("fileId", -1),
        OVERRIDE_FILE_ID("overrideFileId", false),
        SAMPLE_IDS ("sampleIds", ""),
        ISOLATE_FILE_FROM_STUDY_CONFIGURATION("isolateStudyConfiguration", false),

        COMPRESS_METHOD ("compressMethod", "gzip"),
        AGGREGATION_MAPPING_PROPERTIES ("aggregationMappingFile", null),
        DB_NAME ("database.name", "opencga"),

        TRANSFORM_BATCH_SIZE ("transform.batch.size", 200),
        TRANSFORM_THREADS ("transform.threads", 4),
        TRANSFORM_FORMAT ("transform.format", "avro"),
        LOAD_BATCH_SIZE ("load.batch.size", 100),
        LOAD_THREADS ("load.threads", 4),

        CALCULATE_STATS ("calculateStats", false),          //Calculate stats on the postLoad step
        OVERWRITE_STATS ("overwriteStats", false),          //Overwrite stats already present
        UPDATE_STATS ("updateStats", false),                //Calculate missing stats
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

        @SuppressWarnings("unchecked")
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

        String fileName = Paths.get(input.getPath()).getFileName().toString();
        int fileId = variantOptions.getInt(Options.FILE_ID.key, Options.FILE_ID.defaultValue());
        int studyId = variantOptions.getInt(Options.STUDY_ID.key, Options.STUDY_ID.defaultValue());

        StudyConfiguration studyConfiguration;
        if (studyId < 0 && fileId < 0) {
            logger.debug("Isolated study configuration");
            studyConfiguration = new StudyConfiguration(Options.STUDY_ID.defaultValue(), "unknown", Options.FILE_ID.defaultValue(), fileName);
            studyConfiguration.setAggregation(variantOptions.get(Options.AGGREGATED_TYPE.key, VariantSource.Aggregation.class));
            variantOptions.put(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);
        } else {
            //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
            studyConfiguration = getStudyConfiguration(variantOptions);

            if (studyConfiguration == null) {
                logger.info("Creating a new StudyConfiguration");
                checkStudyId(studyId);
                studyConfiguration = new StudyConfiguration(studyId, variantOptions.getString(Options.STUDY_NAME.key));
                studyConfiguration.setAggregation(variantOptions.get(Options.AGGREGATED_TYPE.key, VariantSource.Aggregation.class));
            }

            checkNewFile(studyConfiguration, fileId, fileName);
        }
        variantOptions.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);


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
        String format = options.getString(Options.TRANSFORM_FORMAT.key(), Options.TRANSFORM_FORMAT.defaultValue());
        String parser = options.getString("transform.parser", "htsjdk");

        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        Integer fileId;
        if (options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.defaultValue())) {
            fileId = Options.FILE_ID.defaultValue();
        } else {
            fileId = options.getInt(Options.FILE_ID.key);
        }
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
        int numTasks = options.getInt(Options.TRANSFORM_THREADS.key, Options.TRANSFORM_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", numTasks*2);

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


        Path outputVariantsFile = output.resolve(fileName + ".variants." + format + extension);
        Path outputMetaFile = output.resolve(fileName + ".file." + format + extension);

        logger.info("Transforming variants...");
        long start, end;
        if (numTasks == 1 && format.equals("json")) { //Run transformation with a SingleThread runner. The legacy way
            if (!extension.equals(".gz")) { //FIXME: Add compatibility with snappy compression
                logger.warn("Force using gzip compression");
                extension = ".gz";
                outputVariantsFile = output.resolve(fileName + ".variants.json" + extension);
                outputMetaFile = output.resolve(fileName + ".file.json" + extension);
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
            VariantRunner vr = new VariantRunner(source, reader, pedReader, writers,
                    Collections.<Task<Variant>>singletonList(new VariantGlobalStatsCalculator(source)), batchSize);

            logger.info("Single thread transform...");
            start = System.currentTimeMillis();
            try {
                vr.run();
            } catch (IOException e) {
                throw new StorageManagerException("Fail runner execution", e);
            }
            end = System.currentTimeMillis();

        } else if (format.equals("avro")) {
            //Read VariantSource
            source = readVariantSource(input, source);

            //Reader
            StringDataReader dataReader = new StringDataReader(input);

            //Writer
            DataWriter<ByteBuffer> dataWriter;
            try {
                dataWriter = new AvroFileWriter<>(VariantAvro.getClassSchema(), compression, new FileOutputStream(outputVariantsFile.toFile()));
            } catch (FileNotFoundException e) {
                throw new StorageManagerException("Fail init writer", e);
            }
            Supplier<ParallelTaskRunner.Task<String, ByteBuffer>> taskSupplier;

            if (parser.equalsIgnoreCase("htsjdk")) {
                logger.info("Using HTSJDK to read variants.");
                FullVcfCodec codec = new FullVcfCodec();
                final VariantSource finalSource = source;
                final Path finalOutputMetaFile = output.resolve(fileName + ".file.json" + extension);   //TODO: Write META in avro too
                try (InputStream fileInputStream = input.toString().endsWith("gz")
                        ? new GZIPInputStream(new FileInputStream(input.toFile()))
                        : new FileInputStream(input.toFile())) {
                    LineIterator lineIterator = codec.makeSourceFromStream(fileInputStream);
                    VCFHeader header = (VCFHeader) codec.readActualHeader(lineIterator);
                    VCFHeaderVersion headerVersion = codec.getVCFHeaderVersion();
                    VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);
                    taskSupplier = () -> new VariantAvroTransformTask(header, headerVersion, finalSource, finalOutputMetaFile, statsCalculator);
                } catch (IOException e) {
                    throw new StorageManagerException("Unable to read VCFHeader", e);
                }
            } else {
                logger.info("Using Biodata to read variants.");
                final VariantSource finalSource = source;
                final Path finalOutputMetaFile = output.resolve(fileName + ".file.json" + extension);   //TODO: Write META in avro too
                VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);
                taskSupplier = () -> new VariantAvroTransformTask(factory, finalSource, finalOutputMetaFile, statsCalculator);
            }

            ParallelTaskRunner<String, ByteBuffer> ptr;
            try {
                ptr = new ParallelTaskRunner<>(
                        dataReader,
                        taskSupplier,
                        dataWriter,
                        new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false)
                );
            } catch (Exception e) {
                throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
            }
            logger.info("Multi thread transform... [1 reading, {} transforming, 1 writing]", numTasks);
            start = System.currentTimeMillis();
            try {
                ptr.run();
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while executing TransformVariants in ParallelTaskRunner", e);
            }
            end = System.currentTimeMillis();
        } else if (format.equals("json")) {
            //Read VariantSource
            source = readVariantSource(input, source);

            //Reader
            StringDataReader dataReader = new StringDataReader(input);

            //Writers
            StringDataWriter dataWriter = new StringDataWriter(outputVariantsFile);

            final VariantSource finalSource = source;
            final Path finalOutputFileJsonFile = outputMetaFile;
            ParallelTaskRunner<String, String> ptr;
            try {
                VariantJsonTransformTask variantJsonTransformTask = new VariantJsonTransformTask(factory, finalSource, finalOutputFileJsonFile);
                variantJsonTransformTask.setIncludeSrc(includeSrc);
                ptr = new ParallelTaskRunner<>(
                        dataReader,
                        variantJsonTransformTask,
                        dataWriter,
                        new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false)
                );
            } catch (Exception e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while creating ParallelTaskRunner", e);
            }
            logger.info("Multi thread transform... [1 reading, {} transforming, 1 writing]", numTasks);
            start = System.currentTimeMillis();
            try {
                ptr.run();
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new StorageManagerException("Error while executing TransformVariants in ParallelTaskRunner", e);
            }
            end = System.currentTimeMillis();
        } else {
            throw new IllegalArgumentException("Unknown format " + format);
        }
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants transformed!");

        return outputUri.resolve(outputVariantsFile.getFileName().toString());
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        // Delete isolated storage configuration
        if (options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key())) {
            options.remove(Options.STUDY_CONFIGURATION.key());
        }

        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            int studyId = options.getInt(Options.STUDY_ID.key, Options.STUDY_ID.defaultValue());
            String studyName = options.getString(Options.STUDY_NAME.key, Options.STUDY_NAME.defaultValue());
            checkStudyId(studyId);
            studyConfiguration = new StudyConfiguration(studyId, studyName);
            options.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);
        }

        //TODO: Expect JSON file
        VariantSource source = readVariantSource(Paths.get(input.getPath()), null);

        /*
         * Before load file, check and add fileName to the StudyConfiguration.
         * FileID and FileName is read from the VariantSource
         * If fileId is -1, read fileId from Options
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
            throw new StorageManagerException("FileId '" + source.getFileId() + "' is not an integer", e);
        }

        if (fileId < 0) {
            fileId = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
        } else {
            int fileIdFromParams = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
            if (fileIdFromParams >= 0) {
                if (fileIdFromParams != fileId) {
                    if (!options.getBoolean(Options.OVERRIDE_FILE_ID.key(), Options.OVERRIDE_FILE_ID.defaultValue())) {
                        throw new StorageManagerException("Wrong fileId! Unable to load using fileId: " + fileIdFromParams + ". " +
                                "The input file has fileId: " + fileId + ". Use " + Options.OVERRIDE_FILE_ID.key() + " to ignore original fileId.");
                    } else {
                        //Override the fileId
                        fileId = fileIdFromParams;
                    }
                }
            }
            options.put(Options.FILE_ID.key(), fileId);
        }

        checkNewFile(studyConfiguration, fileId, fileName);
        studyConfiguration.getFileIds().put(source.getFileName(), fileId);
        studyConfiguration.getHeaders().put(fileId, source.getMetadata().get("variantFileHeader").toString());


        checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);

        options.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);

        return input;
    }

    /**
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
    public static void checkAndUpdateStudyConfiguration(StudyConfiguration studyConfiguration, int fileId, VariantSource source, ObjectMap options)
            throws StorageManagerException {
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
                    throw new StorageManagerException("Given sampleName '" + sampleName + "' is not in the input file");
                } else {
                    if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                        //Add sample to StudyConfiguration
                        studyConfiguration.getSampleIds().put(sampleName, sampleId);
                    } else {
                        if (studyConfiguration.getSampleIds().get(sampleName) == sampleId) {
                            //throw new StorageManagerException("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
//                            logger.warn("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
                        } else {
                            throw new StorageManagerException("Sample " + sampleName + ":" + sampleId + " was already present. It was in the StudyConfiguration with a different sampleId: " + studyConfiguration.getSampleIds().get(sampleName));
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
            LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
            List<String> missingSamples = new LinkedList<>();
            for (String sampleName : source.getSamples()) {
                if (!sampleIds.contains(studyConfiguration.getSampleIds().get(sampleName))) {
                    missingSamples.add(sampleName);
                }
            }
            if (!missingSamples.isEmpty()) {
                throw new StorageManagerException("Samples "  + missingSamples.toString() + " were not in file " + fileId);
            }
            if (sampleIds.size() != source.getSamples().size()) {
                throw new StorageManagerException("Incorrect number of samples in file " + fileId);
            }
        } else {
            LinkedHashSet<Integer> sampleIdsInFile = new LinkedHashSet<>(source.getSamples().size());
            for (String sample : source.getSamples()) {
                sampleIdsInFile.add(studyConfiguration.getSampleIds().get(sample));
            }
            studyConfiguration.getSamplesInFiles().put(fileId, sampleIdsInFile);
        }
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


        checkLoadedVariants(input, fileId, studyConfiguration, options);

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
                annotationQuery.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
            }
            annotationQuery.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), Collections.singletonList(studyConfiguration.getStudyId()));    // annotate just the indexed variants
            annotationQuery.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), Collections.singletonList(fileId));    // annotate just the indexed variants

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

                String defaultCohortName = StudyEntry.DEFAULT_COHORT;
                Map<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
                Map<String, Set<String>> defaultCohort = new HashMap<>(Collections.singletonMap(defaultCohortName, indexedSamples.keySet()));

                QueryOptions statsOptions = new QueryOptions(options);
                if (studyConfiguration.getCohortIds().containsKey(defaultCohortName)) { //Check if "defaultCohort" exists
                    Integer defaultCohortId = studyConfiguration.getCohortIds().get(defaultCohortName);
                    if (studyConfiguration.getCalculatedStats().contains(defaultCohortId)) { //Check if "defaultCohort" is calculated
                        if (!indexedSamples.values().equals(studyConfiguration.getCohorts().get(defaultCohortId))) { //Check if the samples number are different
                            logger.debug("Cohort \"{}\":{} was already calculated. Invalidating stats to recalculate.", defaultCohortName, defaultCohortId);
                            studyConfiguration.getCalculatedStats().remove(defaultCohortId);
                            studyConfiguration.getInvalidStats().add(defaultCohortId);
                            statsOptions.put(Options.OVERWRITE_STATS.key(), true);
                        } else {
                            logger.debug("Cohort \"{}\":{} was already calculated. Just update stats.", defaultCohortName, defaultCohortId);
                            statsOptions.put(Options.UPDATE_STATS.key(), true);
                        }
                    }
                }
                statsOptions.remove(Options.FILE_ID.key());

                URI statsUri = variantStatisticsManager.createStats(dbAdaptor, statsOutputUri, defaultCohort, new HashMap<>(), studyConfiguration, statsOptions);
                variantStatisticsManager.loadStats(dbAdaptor, statsUri, studyConfiguration, statsOptions);
            } catch (Exception e) {
                logger.error("Can't calculate stats." , e);
                e.printStackTrace();
            }
        }

        return input;
    }

    protected abstract void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws StorageManagerException;

    @Override
    public boolean testConnection(String dbName) {
        return true;
    }

    public static String buildFilename(String studyName, int fileId) {
        int index = studyName.indexOf(":");
        if (index >= 0) {
            studyName = studyName.substring(index + 1);
        }
        return studyName + "_" + fileId;
    }

    public static VariantSource readVariantSource(Path input, VariantSource source) throws StorageManagerException {
        if (source == null) {
            source = new VariantSource("", "", "", "");
        }
        VariantReader reader = getVariantReader(input, source);

        try {
            reader.open();
            reader.pre();
            String variantFileHeader = reader.getHeader();
            source.addMetadata("variantFileHeader", variantFileHeader);
            if (source.getHeader() == null) {
                VCFHeader header = VariantFileMetadataToVCFHeaderConverter.parseVcfHeader(variantFileHeader);
                source.setHeader(new VCFHeaderToAvroVcfHeaderConverter().convert(header));
            }
            reader.post();
            reader.close();
        } catch (Exception e) {
            throw new StorageManagerException("Unable to read VariantSource", e);
        }

        return source;
    }

    protected static VariantReader getVariantReader(Path input, VariantSource source) throws StorageManagerException {
        String fileName = input.getFileName().toString();
        if (fileName.contains("json")) {
            return getVariantJsonReader(input, source);
        } else if (fileName.contains("avro")) {
            return getVariantAvroReader(input, source);
        } else if (fileName.endsWith("vcf") || fileName.endsWith("vcf.gz")) {
            return new VariantVcfReader(source, input.toAbsolutePath().toString());
        } else {
            throw new StorageManagerException("Variants input file format not supported for file: " + input);
        }
    }

    protected static VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws StorageManagerException {
        VariantJsonReader variantJsonReader;
        if (    input.toString().endsWith(".json") ||
                input.toString().endsWith(".json.gz") ||
                input.toString().endsWith(".json.snappy") ||
                input.toString().endsWith(".json.snz")) {
            String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw new StorageManagerException("Variants input file format not supported for file: " + input);
        }
        return variantJsonReader;
    }

    protected static VariantAvroReader getVariantAvroReader(Path input, VariantSource source) throws StorageManagerException {
        VariantAvroReader variantAvroReader;
        if (input.toString().matches(".*avro(\\..*)?$")) {
            String sourceFile = input.toAbsolutePath().toString().replace("variants.avro", "file.json");
            variantAvroReader = new VariantAvroReader(input.toAbsolutePath().toFile(), new File(sourceFile), source);
        } else {
            throw new StorageManagerException("Variants input file format not supported for file: " + input);
        }
        return variantAvroReader;
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
            if (!StringUtils.isEmpty(params.getString(Options.STUDY_NAME.key))
                    && !params.getString(Options.STUDY_NAME.key()).equals(Options.STUDY_NAME.defaultValue())) {
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(params.getString(Options.STUDY_NAME.key), new QueryOptions(params)).first();
                if (studyConfiguration != null && params.containsKey(Options.STUDY_ID.key)) {
                    //Check if StudyId matches
                    if (studyConfiguration.getStudyId() != params.getInt(Options.STUDY_ID.key)) {
                        throw new StorageManagerException("Invalid StudyConfiguration. StudyId mismatches");
                    }
                }
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

        if (fileId < 0) {
            throw new StorageManagerException("Invalid fileId " + fileId + " for file " + fileName + ". FileId must be positive.");
        }

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
        checkStudyId(studyConfiguration.getStudyId());
        if (studyConfiguration.getFileIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getFileIds()).size() ) {
            throw new StorageManagerException("StudyConfiguration has duplicated fileIds");
        }
        if (studyConfiguration.getCohortIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).size() ) {
            throw new StorageManagerException("StudyConfiguration has duplicated cohortIds");
        }
    }


    public static void checkStudyId(int studyId) throws StorageManagerException {
        if (studyId < 0) {
            throw new StorageManagerException("Invalid studyId : " + studyId);
        }
    }


}
