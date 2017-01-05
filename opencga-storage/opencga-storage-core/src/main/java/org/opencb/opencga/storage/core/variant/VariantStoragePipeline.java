/*
 * Copyright 2015-2016 OpenCB
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

import com.google.common.collect.BiMap;
import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.FullVcfCodec;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.opencga.storage.core.variant.transform.MalformedVariantHandler;
import org.opencb.opencga.storage.core.variant.transform.VariantAvroTransformTask;
import org.opencb.opencga.storage.core.variant.transform.VariantJsonTransformTask;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;

/**
 * Created on 30/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantStoragePipeline implements StoragePipeline {

    private static final String HTSJDK_PARSER = "htsjdk";
    protected final StorageConfiguration configuration;
    protected final String storageEngineId;
    protected final ObjectMap options;
    protected final VariantDBAdaptor dbAdaptor;
    protected final VariantReaderUtils variantReaderUtils;
    protected final Logger logger;
    protected final ObjectMap transformStats = new ObjectMap();


    public VariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, Logger logger, VariantDBAdaptor dbAdaptor,
                                  VariantReaderUtils variantReaderUtils) {
        this(configuration, storageEngineId, logger, dbAdaptor, variantReaderUtils,
                new ObjectMap(configuration.getStorageEngine(storageEngineId).getVariant().getOptions()));
    }

    /**
     * @param configuration     Storage Configuration
     * @param storageEngineId   StorageEngineID
     * @param logger            Logger
     * @param dbAdaptor         VariantDBAdaptor. Can be null if the load step is skipped
     * @param variantReaderUtils    VariantReaderUtils
     * @param options           Unique copy of the options to be used. This object can not be shared.
     */
    public VariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, Logger logger, VariantDBAdaptor dbAdaptor,
                                  VariantReaderUtils variantReaderUtils, ObjectMap options) {
        this.configuration = configuration;
        this.storageEngineId = storageEngineId;
        this.logger = logger;
        this.dbAdaptor = dbAdaptor;
        this.variantReaderUtils = variantReaderUtils;
        this.options = options;
        if (dbAdaptor == null) {
            options.put(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);
        }
    }

    @Override
    public URI extract(URI input, URI ouput) {
        return input;
    }

    @Override
    public ObjectMap getTransformStats() {
        return transformStats;
    }

    @Override
    public URI preTransform(URI input) throws StorageEngineException, IOException, FileFormatException {
        String fileName = VariantReaderUtils.getFileName(input);
        int fileId = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
        int studyId = options.getInt(Options.STUDY_ID.key(), Options.STUDY_ID.defaultValue());

        boolean isolate = options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(),
                Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.defaultValue());
        StudyConfiguration studyConfiguration;
        if (studyId < 0 && fileId < 0 || isolate) {
            logger.debug("Isolated study configuration");
            studyConfiguration = new StudyConfiguration(Options.STUDY_ID.defaultValue(), "unknown", Options.FILE_ID.defaultValue(),
                    fileName);
            studyConfiguration.setAggregation(options.get(Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.class));
            options.put(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);
        } else {
            long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);
            try {
                //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
                studyConfiguration = getStudyConfiguration(true);

                if (studyConfiguration == null) {
                    logger.info("Creating a new StudyConfiguration");
                    checkStudyId(studyId);
                    studyConfiguration = new StudyConfiguration(studyId, options.getString(Options.STUDY_NAME.key()));
                    studyConfiguration.setAggregation(options.get(Options.AGGREGATED_TYPE.key(),
                            VariantSource.Aggregation.class, Options.AGGREGATED_TYPE.defaultValue()));
                }
                if (studyConfiguration.getAggregation() == null) {
                    studyConfiguration.setAggregation(options.get(Options.AGGREGATED_TYPE.key(),
                            VariantSource.Aggregation.class, Options.AGGREGATED_TYPE.defaultValue()));
                }
                fileId = checkNewFile(studyConfiguration, fileId, fileName);
                options.put(Options.FILE_ID.key(), fileId);
                dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
            } finally {
                dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
            }
        }
        options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);

        return input;
    }

    protected VariantSource buildVariantSource(Path input, ObjectMap options) throws StorageEngineException {
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        Integer fileId;
        if (options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION
                .defaultValue())) {
            fileId = Options.FILE_ID.defaultValue();
        } else {
            fileId = options.getInt(Options.FILE_ID.key());
        }
        VariantSource.Aggregation aggregation = options.get(Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.class, Options
                .AGGREGATED_TYPE.defaultValue());
        String fileName = input.getFileName().toString();
        VariantStudy.StudyType type = options.get(Options.STUDY_TYPE.key(), VariantStudy.StudyType.class,
                Options.STUDY_TYPE.defaultValue());
        return new VariantSource(
                fileName,
                fileId.toString(),
                Integer.toString(studyConfiguration.getStudyId()),
                studyConfiguration.getStudyName(), type, aggregation);
    }


    public static Pair<VCFHeader, VCFHeaderVersion> readHtsHeader(Path input) throws StorageEngineException {
        try (InputStream fileInputStream = input.toString().endsWith("gz")
                ? new GZIPInputStream(new FileInputStream(input.toFile()))
                : new FileInputStream(input.toFile())) {
            FullVcfCodec codec = new FullVcfCodec();
            LineIterator lineIterator = codec.makeSourceFromStream(fileInputStream);
            VCFHeader header = (VCFHeader) codec.readActualHeader(lineIterator);
            VCFHeaderVersion headerVersion = codec.getVCFHeaderVersion();
            return new ImmutablePair<>(header, headerVersion);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to read VCFHeader", e);
        }
    }

    /**
     * Transform raw variant files into biodata model.
     *
     * @param inputUri Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigreeUri Pedigree input file. Accepted formats: *.ped
     * @param outputUri The destination folder
     * @throws StorageEngineException If any IO problem
     */
    @Override
    public URI transform(URI inputUri, URI pedigreeUri, URI outputUri) throws StorageEngineException {
        // input: VcfReader
        // output: JsonWriter


        Path input = Paths.get(inputUri.getPath());
        Path pedigree = pedigreeUri == null ? null : Paths.get(pedigreeUri.getPath());
        Path output = Paths.get(outputUri.getPath());

//        boolean includeSamples = options.getBoolean(Options.INCLUDE_GENOTYPES.key(), false);
        boolean includeStats = options.getBoolean(Options.INCLUDE_STATS.key(), false);
//        boolean includeSrc = options.getBoolean(Options.INCLUDE_SRC.key(), Options.INCLUDE_SRC.defaultValue());
        boolean includeSrc = false;
        boolean failOnError = options.getBoolean(Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(),
                Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.defaultValue());
        String format = options.getString(Options.TRANSFORM_FORMAT.key(), Options.TRANSFORM_FORMAT.defaultValue());
        String parser = options.getString("transform.parser", HTSJDK_PARSER);

        VariantSource source = buildVariantSource(input, options);
        String fileName = source.getFileName();
        boolean generateReferenceBlocks = options.getBoolean(Options.GVCF.key(), false);

        int batchSize = options.getInt(Options.TRANSFORM_BATCH_SIZE.key(), Options.TRANSFORM_BATCH_SIZE.defaultValue());

        String compression = options.getString(Options.COMPRESS_METHOD.key(), Options.COMPRESS_METHOD.defaultValue());
        String extension = "";
        int numTasks = options.getInt(Options.TRANSFORM_THREADS.key(), Options.TRANSFORM_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", numTasks * 2);

        if ("gzip".equalsIgnoreCase(compression) || "gz".equalsIgnoreCase(compression)) {
            extension = ".gz";
        } else if ("snappy".equalsIgnoreCase(compression) || "snz".equalsIgnoreCase(compression)) {
            extension = ".snappy";
        } else if (!compression.isEmpty()) {
            throw new IllegalArgumentException("Unknown compression method " + compression);
        }


        Path outputMalformedVariants = output.resolve(fileName + "." + VariantReaderUtils.MALFORMED_FILE + ".txt");
        Path outputVariantsFile = output.resolve(fileName + "." + VariantReaderUtils.VARIANTS_FILE + "." + format + extension);
        Path outputMetaFile = VariantReaderUtils.getMetaFromTransformedFile(outputVariantsFile);

        // Close at the end!
        final MalformedVariantHandler malformedHandler;
        try {
            malformedHandler = new MalformedVariantHandler(outputMalformedVariants);
        } catch (IOException e) {
            throw new StorageEngineException(e.getMessage(), e);
        }

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numTasks)
                .setBatchSize(batchSize)
                .setCapacity(capacity)
                .setSorted(true)
                .build();

        logger.info("Transforming variants using {} into {} ...", parser, format);
        long start, end;
        if (numTasks == 1 && "json".equals(format)) { //Run transformation with a SingleThread runner. The legacy way
            if (!".gz".equals(extension)) { //FIXME: Add compatibility with snappy compression
                logger.warn("Force using gzip compression");
                extension = ".gz";
                outputVariantsFile = output.resolve(fileName + ".variants.json" + extension);
            }

            //Ped Reader
            PedigreeReader pedReader = null;
            if (pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
                pedReader = new PedigreePedReader(pedigree.toString());
            }

            //Reader
            VariantReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());

            //Writers
            VariantJsonWriter jsonWriter = new VariantJsonWriter(source, output);
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
                throw new StorageEngineException("Fail runner execution", e);
            }
            end = System.currentTimeMillis();

        } else if ("avro".equals(format)) {
            //Read VariantSource
            source = VariantReaderUtils.readVariantSource(input, source);

            //Reader
            StringDataReader dataReader = new StringDataReader(input);

            //Writer
            DataWriter<ByteBuffer> dataWriter;
            try {
                dataWriter = new AvroFileWriter<>(VariantAvro.getClassSchema(), compression, new FileOutputStream(outputVariantsFile
                        .toFile()));
            } catch (FileNotFoundException e) {
                throw new StorageEngineException("Fail init writer", e);
            }
            Supplier<VariantTransformTask<ByteBuffer>> taskSupplier;

            if (parser.equalsIgnoreCase(HTSJDK_PARSER)) {
                logger.info("Using HTSJDK to read variants.");
                FullVcfCodec codec = new FullVcfCodec();
                final VariantSource finalSource = source;
                Pair<VCFHeader, VCFHeaderVersion> header = readHtsHeader(input);
                VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);
                taskSupplier = () -> new VariantAvroTransformTask(header.getKey(), header.getValue(), finalSource, outputMetaFile,
                        statsCalculator, includeSrc, generateReferenceBlocks)
                        .setFailOnError(failOnError).addMalformedErrorHandler(malformedHandler);
            } else {
                // TODO Create a utility to determine which extensions are variants files
                final VariantVcfFactory factory = createVariantVcfFactory(source, fileName);
                logger.info("Using Biodata to read variants.");
                final VariantSource finalSource = source;
                VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);
                taskSupplier = () -> new VariantAvroTransformTask(factory, finalSource, outputMetaFile, statsCalculator, includeSrc)
                        .setFailOnError(failOnError).addMalformedErrorHandler(malformedHandler);
            }

            logger.info("Generating output file {}", outputVariantsFile);

            ParallelTaskRunner<String, ByteBuffer> ptr;
            try {
                ptr = new ParallelTaskRunner<>(
                        dataReader,
                        taskSupplier,
                        dataWriter,
                        config
                );
            } catch (Exception e) {
                throw new StorageEngineException("Error while creating ParallelTaskRunner", e);
            }
            logger.info("Multi thread transform... [1 reading, {} transforming, 1 writing]", numTasks);
            start = System.currentTimeMillis();
            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageEngineException("Error while executing TransformVariants in ParallelTaskRunner", e);
            }
            end = System.currentTimeMillis();
        } else if ("json".equals(format)) {
            //Read VariantSource
            source = VariantReaderUtils.readVariantSource(input, source);

            //Reader
            StringDataReader dataReader = new StringDataReader(input);

            //Writers
            StringDataWriter dataWriter = new StringDataWriter(outputVariantsFile, true);

            final VariantSource finalSource = source;
            ParallelTaskRunner<String, String> ptr;

            Supplier<VariantTransformTask<String>> taskSupplier;
            if (parser.equalsIgnoreCase(HTSJDK_PARSER)) {
                logger.info("Using HTSJDK to read variants.");
                Pair<VCFHeader, VCFHeaderVersion> header = readHtsHeader(input);
                VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(finalSource);
                taskSupplier = () -> new VariantJsonTransformTask(header.getKey(), header.getValue(), finalSource,
                        outputMetaFile, statsCalculator, includeSrc, generateReferenceBlocks)
                        .setFailOnError(failOnError).addMalformedErrorHandler(malformedHandler);
            } else {
                // TODO Create a utility to determine which extensions are variants files
                final VariantVcfFactory factory = createVariantVcfFactory(source, fileName);
                logger.info("Using Biodata to read variants.");
                VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);
                taskSupplier = () -> new VariantJsonTransformTask(factory, finalSource, outputMetaFile, statsCalculator, includeSrc)
                        .setFailOnError(failOnError).addMalformedErrorHandler(malformedHandler);
            }

            logger.info("Generating output file {}", outputVariantsFile);

            try {
                ptr = new ParallelTaskRunner<>(
                        dataReader,
                        taskSupplier,
                        dataWriter,
                        config
                );
            } catch (Exception e) {
                throw new StorageEngineException("Error while creating ParallelTaskRunner", e);
            }

            logger.info("Multi thread transform... [1 reading, {} transforming, 1 writing]", numTasks);
            start = System.currentTimeMillis();
            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageEngineException("Error while executing TransformVariants in ParallelTaskRunner", e);
            }
            end = System.currentTimeMillis();
        } else if ("proto".equals(format)) {
            //Read VariantSource
            source = VariantReaderUtils.readVariantSource(input, source);
            Pair<Long, Long> times = processProto(input, fileName, output, source, outputVariantsFile, outputMetaFile,
                    includeSrc, parser, generateReferenceBlocks, batchSize, extension, compression, malformedHandler, failOnError);
            start = times.getKey();
            end = times.getValue();
        } else {
            throw new IllegalArgumentException("Unknown format " + format);
        }
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants transformed!");

        // Close the malformed variant handler
        malformedHandler.close();
        if (malformedHandler.getMalformedLines() > 0) {
            getTransformStats().put("malformed lines", malformedHandler.getMalformedLines());
        }

        return outputUri.resolve(outputVariantsFile.getFileName().toString());
    }

    protected VariantVcfFactory createVariantVcfFactory(VariantSource source, String fileName) throws StorageEngineException {
        VariantVcfFactory factory;
        if (fileName.endsWith(".vcf") || fileName.endsWith(".vcf.gz") || fileName.endsWith(".vcf.snappy")) {
            if (VariantSource.Aggregation.NONE.equals(source.getAggregation())) {
                factory = new VariantVcfFactory();
            } else {
                factory = new VariantAggregatedVcfFactory();
            }
        } else {
            throw new StorageEngineException("Variants input file format not supported");
        }
        return factory;
    }

    protected Pair<Long, Long> processProto(
            Path input, String fileName, Path output, VariantSource source, Path outputVariantsFile,
            Path outputMetaFile, boolean includeSrc, String parser, boolean generateReferenceBlocks,
            int batchSize, String extension, String compression, BiConsumer<String, RuntimeException> malformatedHandler,
            boolean failOnError)
            throws StorageEngineException {
        throw new NotImplementedException("Please request feature");
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        // Delete isolated storage configuration
        if (options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key())) {
            options.remove(Options.STUDY_CONFIGURATION.key());
        }

        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageEngineException {
        int studyId = options.getInt(Options.STUDY_ID.key(), -1);
        options.remove(Options.STUDY_CONFIGURATION.key());

        long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration;
        try {
            studyConfiguration = checkOrCreateStudyConfiguration(true);
            VariantSource source = readVariantSource(input, options);
            securePreLoad(studyConfiguration, source);
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        } finally {
            dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        }

        options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);
        return input;
    }

    /**
     * PreLoad step for modify the StudyConfiguration.
     * This step is executed inside a study lock.
     *
     * @see StudyConfigurationManager#lockStudy(int)
     * @param studyConfiguration    StudyConfiguration
     * @param source                VariantSource
     * @throws StorageEngineException  If any condition is wrong
     */
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantSource source) throws StorageEngineException {

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
            throw new StorageEngineException("FileId '" + source.getFileId() + "' is not an integer", e);
        }

        if (fileId < 0) {
            fileId = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
        } else {
            int fileIdFromParams = options.getInt(Options.FILE_ID.key(), Options.FILE_ID.defaultValue());
            if (fileIdFromParams >= 0 && fileIdFromParams != fileId) {
                if (!options.getBoolean(Options.OVERRIDE_FILE_ID.key(), Options.OVERRIDE_FILE_ID.defaultValue())) {
                    throw new StorageEngineException("Wrong fileId! Unable to load using fileId: "
                            + fileIdFromParams + ". "
                            + "The input file has fileId: " + fileId
                            + ". Use " + Options.OVERRIDE_FILE_ID.key() + " to ignore original fileId.");
                } else {
                    //Override the fileId
                    fileId = fileIdFromParams;
                }
            }
        }

        if (studyConfiguration.getIndexedFiles().isEmpty()) {
            // First indexed file
            // Use the EXCLUDE_GENOTYPES value from CLI. Write in StudyConfiguration.attributes
            boolean excludeGenotypes = options.getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());
            studyConfiguration.setAggregation(options.get(Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.class,
                    Options.AGGREGATED_TYPE.defaultValue()));
            studyConfiguration.getAttributes().put(Options.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
        } else {
            // Not first indexed file
            // Use the EXCLUDE_GENOTYPES value from StudyConfiguration. Ignore CLI value
            boolean excludeGenotypes = studyConfiguration.getAttributes()
                    .getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());
            options.put(Options.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
        }


        fileId = checkNewFile(studyConfiguration, fileId, fileName);
        options.put(Options.FILE_ID.key(), fileId);
        studyConfiguration.getFileIds().put(source.getFileName(), fileId);
//        studyConfiguration.getHeaders().put(fileId, source.getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString());

        checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);

        // Check Extra genotype fields
        if (options.containsKey(Options.EXTRA_GENOTYPE_FIELDS.key())
                && StringUtils.isNotEmpty(options.getString(Options.EXTRA_GENOTYPE_FIELDS.key()))) {
            List<String> extraFields = options.getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());
            if (studyConfiguration.getIndexedFiles().isEmpty()) {
                studyConfiguration.getAttributes().put(Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields);
            } else {
                if (!extraFields.equals(studyConfiguration.getAttributes().getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key()))) {
                    throw new StorageEngineException("Unable to change Stored Extra Fields if there are already indexed files.");
                }
            }
            if (!studyConfiguration.getAttributes().containsKey(Options.EXTRA_GENOTYPE_FIELDS_TYPE.key())) {
                List<String> extraFieldsType = new ArrayList<>(extraFields.size());
                for (String extraField : extraFields) {
                    List<Map<String, Object>> formats = (List) source.getHeader().getMeta().get("FORMAT");
                    VCFHeaderLineType type = VCFHeaderLineType.String;
                    for (Map<String, Object> format : formats) {
                        if (format.get("ID").toString().equals(extraField)) {
                            if ("1".equals(format.get("Number"))) {
                                try {
                                    type = VCFHeaderLineType.valueOf(Objects.toString(format.get("Type")));
                                } catch (IllegalArgumentException ignore) {
                                    type = VCFHeaderLineType.String;
                                }
                            } else {
                                //Fields with arity != 1 are loaded as String
                                type = VCFHeaderLineType.String;
                            }
                            break;
                        }
                    }
                    switch (type) {
                        case String:
                        case Float:
                        case Integer:
                            break;
                        case Character:
                        default:
                            type = VCFHeaderLineType.String;
                            break;

                    }
                    extraFieldsType.add(type.toString());
                    logger.debug(extraField + " : " + type);
                }
                studyConfiguration.getAttributes().put(Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), extraFieldsType);
            }
        }
    }

    protected StudyConfiguration checkOrCreateStudyConfiguration(boolean forceFetch) throws StorageEngineException {
        StudyConfiguration studyConfiguration = getStudyConfiguration(forceFetch);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            int studyId = options.getInt(Options.STUDY_ID.key(), Options.STUDY_ID.defaultValue());
            String studyName = options.getString(Options.STUDY_NAME.key(), Options.STUDY_NAME.defaultValue());
            checkStudyId(studyId);
            studyConfiguration = new StudyConfiguration(studyId, studyName);
            options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);
        }
        return studyConfiguration;
    }

    /*
     * Before load file, the StudyConfiguration has to be updated with the new sample names.
     * Will read param SAMPLE_IDS like [<sampleName>:<sampleId>,]*
     * If SAMPLE_IDS is missing, will auto-generate sampleIds
     * Will fail if:
     * param SAMPLE_IDS is malformed
     * any given sampleId is not an integer
     * any given sampleName is not in the input file
     * any given sampleName was already in the StudyConfiguration (so, was already loaded)
     * some sample was missing in the given SAMPLE_IDS param
     *
     */
    public static void checkAndUpdateStudyConfiguration(StudyConfiguration studyConfiguration, int fileId, VariantSource source,
                                                        ObjectMap options)
            throws StorageEngineException {
        if (options.containsKey(Options.SAMPLE_IDS.key()) && !options.getAsStringList(Options.SAMPLE_IDS.key()).isEmpty()) {
            for (String sampleEntry : options.getAsStringList(Options.SAMPLE_IDS.key())) {
                String[] split = sampleEntry.split(":");
                if (split.length != 2) {
                    throw new StorageEngineException("Param " + sampleEntry + " is malformed");
                }
                String sampleName = split[0];
                int sampleId;
                try {
                    sampleId = Integer.parseInt(split[1]);
                } catch (NumberFormatException e) {
                    throw new StorageEngineException("SampleId " + split[1] + " is not an integer", e);
                }

                if (!source.getSamplesPosition().containsKey(sampleName)) {
                    //ERROR
                    throw new StorageEngineException("Given sampleName '" + sampleName + "' is not in the input file");
                } else {
                    if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                        //Add sample to StudyConfiguration
                        studyConfiguration.getSampleIds().put(sampleName, sampleId);
                    } else {
                        if (studyConfiguration.getSampleIds().get(sampleName) == sampleId) {
                            //throw new StorageEngineException("Sample " + sampleName + ":" + sampleId
                            // + " was already loaded. It was in the StudyConfiguration");
//                          logger.warn("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
                            System.out.println("Empty block");
                        } else {
                            throw new StorageEngineException("Sample " + sampleName + ":" + sampleId
                                    + " was already present. It was in the StudyConfiguration with a different sampleId: "
                                    + studyConfiguration.getSampleIds().get(sampleName));
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
                        logger.warn("Sample " + sampleName + ":" + sampleId + " was already loaded.
                        It was in the StudyConfiguration.indexedSamples");
                    }
                }*/
            }
            if (!missingSamples.isEmpty()) {
                throw new StorageEngineException("Samples " + missingSamples.toString() + " has not assigned sampleId");
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
                throw new StorageEngineException("Samples " + missingSamples.toString() + " were not in file " + fileId);
            }
            if (sampleIds.size() != source.getSamples().size()) {
                throw new StorageEngineException("Incorrect number of samples in file " + fileId);
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
    public URI postLoad(URI input, URI output) throws StorageEngineException {
//        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        List<Integer> fileIds = options.getAsIntegerList(Options.FILE_ID.key());

        int studyId = options.getInt(Options.STUDY_ID.key(), -1);
        long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);

        // Check loaded variants BEFORE updating the StudyConfiguration
        checkLoadedVariants(input, fileIds, getStudyConfiguration(), options);

        StudyConfiguration studyConfiguration;
        try {
            //Update StudyConfiguration
            studyConfiguration = getStudyConfiguration(true);
            securePostLoad(fileIds, studyConfiguration);
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, new QueryOptions());
        } finally {
            dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        }

        return input;
    }

    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageEngineException {
        // Update indexed files
        studyConfiguration.getIndexedFiles().addAll(fileIds);

        // Update the cohort ALL. Invalidate if needed
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
        final Integer defaultCohortId;
        if (studyConfiguration.getCohortIds().containsKey(defaultCohortName)) { //Check if "defaultCohort" exists
            defaultCohortId = studyConfiguration.getCohortIds().get(defaultCohortName);
            if (studyConfiguration.getCalculatedStats().contains(defaultCohortId)) { //Check if "defaultCohort" is calculated
                //Check if the samples number are different
                if (!indexedSamples.values().equals(studyConfiguration.getCohorts().get(defaultCohortId))) {
                    logger.debug("Cohort \"{}\":{} was already calculated. Invalidating stats.",
                            defaultCohortName, defaultCohortId);
                    studyConfiguration.getCalculatedStats().remove(defaultCohortId);
                    studyConfiguration.getInvalidStats().add(defaultCohortId);
                }
            }
        } else {
            // Default cohort does not exist. Create cohort.
            defaultCohortId = studyConfiguration.getCohortIds().values().stream().max(Integer::compareTo).orElse(1);
            studyConfiguration.getCohortIds().put(StudyEntry.DEFAULT_COHORT, defaultCohortId);
        }
        studyConfiguration.getCohorts().put(defaultCohortId, indexedSamples.values());

    }

    @Override
    public void close() throws StorageEngineException {
        if (dbAdaptor != null) {
            try {
                dbAdaptor.close();
            } catch (IOException e) {
                throw new StorageEngineException("Error closing DBAdaptor", e);
            }
        }
    }

    protected abstract void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options)
            throws StorageEngineException;

    protected void checkLoadedVariants(URI input, List<Integer> fileIds, StudyConfiguration studyConfiguration, ObjectMap options)
            throws StorageEngineException {
        for (Integer fileId : fileIds) {
            checkLoadedVariants(input, fileId, studyConfiguration, options);
        }
    }


    public static String buildFilename(String studyName, int fileId) {
        int index = studyName.indexOf(":");
        if (index >= 0) {
            studyName = studyName.substring(index + 1);
        }
        return studyName + "_" + fileId;
    }

    public VariantSource readVariantSource(URI input, ObjectMap options) throws StorageEngineException {
        return variantReaderUtils.readVariantSource(input);
    }

    /* --------------------------------------- */
    /*  StudyConfiguration utils methods        */
    /* --------------------------------------- */

    public final StudyConfiguration getStudyConfiguration() throws StorageEngineException {
        return getStudyConfiguration(false);
    }

    /**
     * Reads the study configuration.
     *
     * @param forceFetch If true, forces to get the StudyConfiguration from the database. Ignores current one.
     * @return           The study configuration.
     * @throws StorageEngineException If the study configuration is not found
     */
    public final StudyConfiguration getStudyConfiguration(boolean forceFetch) throws StorageEngineException {
        if (forceFetch) {
            options.remove(Options.STUDY_CONFIGURATION.key());
        }
        return getStudyConfiguration(options);
    }

    public final StudyConfiguration getStudyConfiguration(ObjectMap params) throws StorageEngineException {
        if (params.containsKey(Options.STUDY_CONFIGURATION.key())) {
            return params.get(Options.STUDY_CONFIGURATION.key(), StudyConfiguration.class);
        } else {
            StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
            StudyConfiguration studyConfiguration;
            if (!StringUtils.isEmpty(params.getString(Options.STUDY_NAME.key()))
                    && !params.getString(Options.STUDY_NAME.key()).equals(Options.STUDY_NAME.defaultValue())) {
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(params.getString(Options.STUDY_NAME.key()),
                        new QueryOptions(params)).first();
                if (studyConfiguration != null && params.containsKey(Options.STUDY_ID.key())) {
                    //Check if StudyId matches
                    if (studyConfiguration.getStudyId() != params.getInt(Options.STUDY_ID.key())) {
                        throw new StorageEngineException("Invalid StudyConfiguration. StudyId mismatches");
                    }
                }
            } else if (params.containsKey(Options.STUDY_ID.key())) {
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(params.getInt(Options.STUDY_ID.key()),
                        new QueryOptions(params)).first();
            } else {
                throw new StorageEngineException("Unable to get StudyConfiguration. Missing studyId or studyName");
            }
            params.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);
            return studyConfiguration;
        }
    }


    /**
     * Check if the file(name,id) can be added to the StudyConfiguration.
     *
     * Will fail if:
     * fileName was already in the studyConfiguration.fileIds with a different fileId
     * fileId was already in the studyConfiguration.fileIds with a different fileName
     * fileId was already in the studyConfiguration.indexedFiles
     *
     * @param studyConfiguration Study Configuration
     * @param fileId    FileId to add. If negative, will generate a new one
     * @param fileName  File name
     * @return fileId related to that file.
     * @throws StorageEngineException if the file is not valid for being loaded
     */
    protected int checkNewFile(StudyConfiguration studyConfiguration, int fileId, String fileName) throws StorageEngineException {
        Map<Integer, String> idFiles = StudyConfiguration.inverseMap(studyConfiguration.getFileIds());

        if (fileId < 0) {
            if (studyConfiguration.getFileIds().containsKey(fileName)) {
                fileId = studyConfiguration.getFileIds().get(fileName);
            } else {
                fileId = studyConfiguration.getFileIds().values().stream().max(Integer::compareTo).orElse(-1) + 1;
                studyConfiguration.getFileIds().put(fileName, fileId);
            }
            //throw new StorageEngineException("Invalid fileId " + fileId + " for file " + fileName + ". FileId must be positive.");
        }

        if (studyConfiguration.getFileIds().containsKey(fileName)) {
            if (studyConfiguration.getFileIds().get(fileName) != fileId) {
                throw new StorageEngineException("File " + fileName + " (" + fileId + ") "
                        + "has a different fileId in the study "
                        + studyConfiguration.getStudyName() + " (" + studyConfiguration.getStudyId() + ") : "
                        + fileName + " (" + studyConfiguration.getFileIds().get(fileName) + ")");
            }
        }
        if (idFiles.containsKey(fileId)) {
            if (!idFiles.get(fileId).equals(fileName)) {
                throw new StorageEngineException("File " + fileName + " (" + fileId + ") "
                        + "has a different fileName in the StudyConfiguration: "
                        + idFiles.get(fileId) + " (" + fileId + ")");
            }
        }

        if (studyConfiguration.getIndexedFiles().contains(fileId)) {
            throw StorageEngineException.alreadyLoaded(fileId, fileName);
        }
        return fileId;
    }

    /**
     * Check if the StudyConfiguration is correct.
     *
     * @param studyConfiguration StudyConfiguration to check
     * @throws StorageEngineException If object is null
     */
    public static void checkStudyConfiguration(StudyConfiguration studyConfiguration) throws StorageEngineException {
        if (studyConfiguration == null) {
            throw new StorageEngineException("StudyConfiguration is null");
        }
        checkStudyId(studyConfiguration.getStudyId());
        if (studyConfiguration.getFileIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getFileIds()).size()) {
            throw new StorageEngineException("StudyConfiguration has duplicated fileIds");
        }
        if (studyConfiguration.getCohortIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).size()) {
            throw new StorageEngineException("StudyConfiguration has duplicated cohortIds");
        }
    }


    public static void checkStudyId(int studyId) throws StorageEngineException {
        if (studyId < 0) {
            throw new StorageEngineException("Invalid studyId : " + studyId);
        }
    }

    public Thread newShutdownHook(String jobOperationName, List<Integer> files) {
        return new Thread(() -> {
            try {
                logger.error("Shutdown hook!");
                setStatus(BatchFileOperation.Status.ERROR, jobOperationName, files);
            } catch (StorageEngineException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public void setStatus(BatchFileOperation.Status status, String operationName, List<Integer> files) throws StorageEngineException {
        int studyId = getStudyId();
        long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);
        try {
            StudyConfiguration studyConfiguration = getStudyConfiguration(true);
            secureSetStatus(studyConfiguration, status, operationName, files);
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        } finally {
            dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        }
    }

    public BatchFileOperation.Status secureSetStatus(StudyConfiguration studyConfiguration, BatchFileOperation.Status status,
                                                        String operationName, List<Integer> files)
            throws StorageEngineException {
        List<BatchFileOperation> batches = studyConfiguration.getBatches();
        BatchFileOperation operation = null;
        for (int i = batches.size() - 1; i >= 0; i--) {
            operation = batches.get(i);
            if (operation.getOperationName().equals(operationName) && operation.getFileIds().equals(files)) {
                break;
            }
            operation = null;
        }
        if (operation == null) {
            throw new IllegalStateException("Batch operation " + operationName + " for files " + files + " not found!");
        }
        BatchFileOperation.Status previousStatus = operation.currentStatus();
        operation.addStatus(Calendar.getInstance().getTime(), status);
        return previousStatus;
    }

    public VariantDBAdaptor getDBAdaptor() {
        return dbAdaptor;
    }

    protected int getStudyId() {
        return options.getInt(Options.STUDY_ID.key());
    }

    public ObjectMap getOptions() {
        return options;
    }
}
