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

package org.opencb.opencga.storage.core.variant;

import com.google.common.collect.BiMap;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.io.avro.AvroFileWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.transform.MalformedVariantHandler;
import org.opencb.opencga.storage.core.variant.transform.VariantAvroTransformTask;
import org.opencb.opencga.storage.core.variant.transform.VariantJsonTransformTask;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;

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
    private final Logger logger = LoggerFactory.getLogger(VariantStoragePipeline.class);
    protected final ObjectMap transformStats = new ObjectMap();
    protected Integer privateFileId;
    protected Integer privateStudyId;
//    protected StudyMetadata privateStudyMetadata;


    public VariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, VariantDBAdaptor dbAdaptor,
                                  VariantReaderUtils variantReaderUtils) {
        this(configuration, storageEngineId, dbAdaptor, variantReaderUtils,
                new ObjectMap(configuration.getStorageEngine(storageEngineId).getVariant().getOptions()));
    }

    /**
     * @param configuration     Storage Configuration
     * @param storageEngineId   StorageEngineID
     * @param dbAdaptor         VariantDBAdaptor. Can be null if the load step is skipped
     * @param variantReaderUtils    VariantReaderUtils
     * @param options           Unique copy of the options to be used. This object can not be shared.
     */
    public VariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, VariantDBAdaptor dbAdaptor,
                                  VariantReaderUtils variantReaderUtils, ObjectMap options) {
        this.configuration = configuration;
        this.storageEngineId = storageEngineId;
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
        String study = options.getString(Options.STUDY.key());

        boolean isolate = options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(),
                Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.defaultValue());
        StudyMetadata metadata;
        if (isolate) {
            logger.debug("Isolated study configuration");
            metadata = new StudyMetadata(-1, "unknown");
            metadata.setAggregationStr(options.getString(Options.AGGREGATED_TYPE.key(),
                    Options.AGGREGATED_TYPE.defaultValue().toString()));
            options.put(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);
            privateStudyId = -1;
        } else {
            VariantStorageMetadataManager smm = dbAdaptor.getMetadataManager();
            ensureStudyMetadataExists(null);

            StudyMetadata studyMetadata = smm.lockAndUpdate(study, existingStudyMetadata -> {
                if (existingStudyMetadata.getAggregation() == null) {
                    existingStudyMetadata.setAggregationStr(options.getString(Options.AGGREGATED_TYPE.key(),
                            Options.AGGREGATED_TYPE.defaultValue().toString()));
                }
                return existingStudyMetadata;
            });
            setFileId(smm.registerFile(studyMetadata.getId(), input.getPath()));
        }

        return input;
    }

    protected VariantFileMetadata createEmptyVariantFileMetadata(Path input) {
        VariantFileMetadata fileMetadata = VariantReaderUtils.createEmptyVariantFileMetadata(input);
        int fileId;
        if (options.getBoolean(Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION
                .defaultValue())) {
            fileId = -1;
        } else {
            fileId = getFileId();
        }
        return fileMetadata.setId(Integer.toString(fileId));
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

        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());
        boolean stdout = options.getBoolean(STDOUT.key(), STDOUT.defaultValue());

        // Create empty VariantFileMetadata
        VariantFileMetadata metadataTemplate = createEmptyVariantFileMetadata(input);
        // Read VariantFileMetadata
        final VariantFileMetadata metadata = VariantReaderUtils.readVariantFileMetadata(input, metadataTemplate, stdin);


        VariantFileHeader variantMetadata = metadata.getHeader();
        String filePath = metadata.getPath();
        String fileName = Paths.get(filePath).getFileName().toString();
        String studyId = String.valueOf(getStudyId());
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


        Path outputMalformedVariants = output.resolve(fileName + '.' + VariantReaderUtils.MALFORMED_FILE + ".txt");
        Path outputVariantsFile = output.resolve(fileName + '.' + VariantReaderUtils.VARIANTS_FILE + '.' + format + extension);
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
//        if (numTasks == 1 && "json".equals(format)) { //Run transformation with a SingleThread runner. The legacy way
//            if (!".gz".equals(extension)) { //FIXME: Add compatibility with snappy compression
//                logger.warn("Force using gzip compression");
//                extension = ".gz";
//                outputVariantsFile = output.resolve(fileName + ".variants.json" + extension);
//            }
//
//            //Ped Reader
//            PedigreeReader pedReader = null;
//            if (pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
//                pedReader = new PedigreePedReader(pedigree.toString());
//            }
//
//            //Reader
//            VariantReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
//
//            //Writers
//            VariantJsonWriter jsonWriter = new VariantJsonWriter(source, output);
//            jsonWriter.includeStats(includeStats);
//
//            List<VariantWriter> writers = Collections.singletonList(jsonWriter);
//
//            //Runner
//            VariantRunner vr = new VariantRunner(source, reader, pedReader, writers,
//                    Collections.singletonList(new VariantSetStatsCalculator(source)), batchSize);
//
//            logger.info("Single thread transform...");
//            start = System.currentTimeMillis();
//            try {
//                vr.run();
//            } catch (IOException e) {
//                throw new StorageEngineException("Fail runner execution", e);
//            }
//            end = System.currentTimeMillis();
//
//        } else
        if ("avro".equals(format)) {

            //Reader
            StringDataReader dataReader = stdin ? new StringDataReader(System.in) : new StringDataReader(input);
            long fileSize = 0;
            try {
                fileSize = dataReader.getFileSize();
            } catch (IOException e) {
                throw new StorageEngineException("Error reading file " + input, e);
            }
            ProgressLogger progressLogger = new ProgressLogger("Transforming file:", fileSize, 200);
            dataReader.setReadBytesListener((totalRead, delta) -> progressLogger.increment(delta, "Bytes"));

            //Writer
            DataWriter<ByteBuffer> dataWriter;
            if (stdout) {
                dataWriter = new AvroFileWriter<>(VariantAvro.getClassSchema(), compression, System.out);
            } else {
                dataWriter = new AvroFileWriter<>(VariantAvro.getClassSchema(), compression, outputVariantsFile);
            }
            Supplier<VariantTransformTask<ByteBuffer>> taskSupplier;

            if (parser.equalsIgnoreCase(HTSJDK_PARSER)) {
                logger.info("Using HTSJDK to read variants.");
                Pair<VCFHeader, VCFHeaderVersion> header = VariantReaderUtils.readHtsHeader(input, stdin);
                VariantSetStatsCalculator statsCalculator = new VariantSetStatsCalculator(studyId, metadata);
                taskSupplier = () -> new VariantAvroTransformTask(header.getKey(), header.getValue(), studyId, metadata, outputMetaFile,
                        statsCalculator, includeSrc, generateReferenceBlocks)
                        .setFailOnError(failOnError)
                        .addMalformedErrorHandler(malformedHandler)
                        .configureNormalizer(variantMetadata);
            } else {
                // TODO Create a utility to determine which extensions are variants files
                final VariantVcfFactory factory = createVariantVcfFactory(fileName);
                logger.info("Using Biodata to read variants.");
                VariantSetStatsCalculator statsCalculator = new VariantSetStatsCalculator(studyId, metadata);
                taskSupplier = () -> new VariantAvroTransformTask(factory, studyId, metadata, outputMetaFile, statsCalculator,
                        includeSrc, generateReferenceBlocks)
                        .setFailOnError(failOnError)
                        .addMalformedErrorHandler(malformedHandler)
                        .configureNormalizer(variantMetadata);
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
            //Reader
            StringDataReader dataReader = stdin ? new StringDataReader(System.in) : new StringDataReader(input);
            long fileSize = 0;
            try {
                fileSize = dataReader.getFileSize();
            } catch (IOException e) {
                throw new StorageEngineException("Error reading file " + input, e);
            }
            ProgressLogger progressLogger = new ProgressLogger("Transforming file:", fileSize, 200);
            dataReader.setReadBytesListener((totalRead, delta) -> progressLogger.increment(delta, "Bytes"));

            //Writers
            StringDataWriter dataWriter;
            if (stdout) {
                dataWriter = new StringDataWriter(System.out, true);
            } else {
                dataWriter = new StringDataWriter(outputVariantsFile, true);
            }

            ParallelTaskRunner<String, String> ptr;

            Supplier<VariantTransformTask<String>> taskSupplier;
            if (parser.equalsIgnoreCase(HTSJDK_PARSER)) {
                logger.info("Using HTSJDK to read variants.");
                Pair<VCFHeader, VCFHeaderVersion> header = VariantReaderUtils.readHtsHeader(input, stdin);
                VariantSetStatsCalculator statsCalculator = new VariantSetStatsCalculator(studyId, metadata);
                taskSupplier = () -> new VariantJsonTransformTask(header.getKey(), header.getValue(), studyId, metadata,
                        outputMetaFile, statsCalculator, includeSrc, generateReferenceBlocks)
                        .setFailOnError(failOnError)
                        .addMalformedErrorHandler(malformedHandler)
                        .configureNormalizer(variantMetadata);
            } else {
                // TODO Create a utility to determine which extensions are variants files
                final VariantVcfFactory factory = createVariantVcfFactory(fileName);
                logger.info("Using Biodata to read variants.");
                VariantSetStatsCalculator statsCalculator = new VariantSetStatsCalculator(studyId, metadata);
                taskSupplier = () -> new VariantJsonTransformTask(factory, studyId, metadata, outputMetaFile, statsCalculator,
                        includeSrc, generateReferenceBlocks)
                        .setFailOnError(failOnError)
                        .addMalformedErrorHandler(malformedHandler)
                        .configureNormalizer(variantMetadata);
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
            Pair<Long, Long> times = transformProto(input, fileName, output, metadata, outputVariantsFile, outputMetaFile,
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

    protected VariantVcfFactory createVariantVcfFactory(String fileName) throws StorageEngineException {
        VariantVcfFactory factory;
        if (fileName.endsWith(".vcf") || fileName.endsWith(".vcf.gz") || fileName.endsWith(".vcf.snappy")) {
            factory = new VariantVcfFactory();
        } else {
            throw new StorageEngineException("Variants input file format not supported");
        }
        return factory;
    }

    protected Pair<Long, Long> transformProto(
            Path input, String fileName, Path output, VariantFileMetadata metadata, Path outputVariantsFile,
            Path outputMetaFile, boolean includeSrc, String parser, boolean generateReferenceBlocks,
            int batchSize, String extension, String compression, BiConsumer<String, RuntimeException> malformatedHandler,
            boolean failOnError)
            throws StorageEngineException {
        throw new NotImplementedException("Please request feature");
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageEngineException {
        getOrCreateStudyMetadata();
        int studyId = getStudyId();

        VariantFileMetadata fileMetadata = readVariantFileMetadata(input);
        //Get the studyConfiguration. If there is no StudyMetadata, create a empty one.
        dbAdaptor.getMetadataManager().lockAndUpdate(studyId, study -> {
            securePreLoad(study, fileMetadata);
            return study;
        });

        return input;
    }

    /**
     * PreLoad step for modify the StudyMetadata and register the file to be loaded.
     * This step is executed inside a study lock.
     *
     * @see VariantStorageMetadataManager#lockStudy(int)
     * @param studyMetadata         StudyMetadata
     * @param fileMetadata          VariantFileMetadata
     * @throws StorageEngineException  If any condition is wrong
     */
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata fileMetadata) throws StorageEngineException {

        /*
         * Before load file, check and add fileName to the StudyMetadata.
         * FileName is read from the VariantFileMetadata
         * Will fail if:
         *     fileId was already in the studyConfiguration.indexedFiles
         */

        int studyId = studyMetadata.getId();
        int fileId = getMetadataManager().registerFile(studyId, fileMetadata);
        setFileId(fileId);

        final boolean excludeGenotypes;
        if (getMetadataManager().getIndexedFiles(studyId).isEmpty()) {
            // First indexed file
            // Use the EXCLUDE_GENOTYPES value from CLI. Write in StudyMetadata.attributes
            excludeGenotypes = options.getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());
            studyMetadata.setAggregationStr(options.getString(Options.AGGREGATED_TYPE.key(),
                    Options.AGGREGATED_TYPE.defaultValue().toString()));
            studyMetadata.getAttributes().put(Options.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
        } else {
            // Not first indexed file
            // Use the EXCLUDE_GENOTYPES value from StudyMetadata. Ignore CLI value
            excludeGenotypes = studyMetadata.getAttributes()
                    .getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());
            options.put(Options.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
        }

        // Get Extra genotype fields
        Stream<String> stream;
        if (StringUtils.isNotEmpty(options.getString(Options.EXTRA_GENOTYPE_FIELDS.key()))
                && !options.getString(Options.EXTRA_GENOTYPE_FIELDS.key()).equals(VariantQueryUtils.ALL)) {
            // If ExtraGenotypeFields are provided by command line, check that those fields are going to be loaded.

            if (options.getString(Options.EXTRA_GENOTYPE_FIELDS.key()).equals(VariantQueryUtils.NONE)) {
                stream = Stream.empty();
            } else {
                stream = options.getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key()).stream();
            }
        } else {
            // Otherwise, add all format fields
            stream = fileMetadata.getHeader().getComplexLines()
                    .stream()
                    .filter(line -> line.getKey().equals("FORMAT"))
                    .map(VariantFileHeaderComplexLine::getId);

        }
        List<String> extraGenotypeFields = studyMetadata.getAttributes().getAsStringList(EXTRA_GENOTYPE_FIELDS.key());
        stream.forEach(format -> {
            if (!extraGenotypeFields.contains(format) && !format.equals(VariantMerger.GT_KEY)) {
                extraGenotypeFields.add(format);
            }
        });
        studyMetadata.getAttributes().put(EXTRA_GENOTYPE_FIELDS.key(), extraGenotypeFields);
        getOptions().put(EXTRA_GENOTYPE_FIELDS.key(), extraGenotypeFields);

        List<String> extraFormatFields = studyMetadata.getAttributes().getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());

        List<String> formatsFields;
        if (excludeGenotypes) {
            formatsFields = extraFormatFields;
        } else {
            formatsFields = new ArrayList<>(extraFormatFields.size() + 1);
            formatsFields.add(VCFConstants.GENOTYPE_KEY);
            formatsFields.addAll(extraFormatFields);
        }
        studyMetadata.addVariantFileHeader(fileMetadata.getHeader(), formatsFields);


        // Check if EXTRA_GENOTYPE_FIELDS_TYPE is filled
        if (!studyMetadata.getAttributes().containsKey(Options.EXTRA_GENOTYPE_FIELDS_TYPE.key())) {
            List<String> extraFieldsType = new ArrayList<>(extraFormatFields.size());
            Map<String, VariantFileHeaderComplexLine> formatsMap = studyMetadata.getVariantHeaderLines("FORMAT");
            for (String extraFormatField : extraFormatFields) {
                VariantFileHeaderComplexLine line = formatsMap.get(extraFormatField);
                if (line == null) {
                    if (extraFormatField.equals(VariantMerger.GENOTYPE_FILTER_KEY)) {
                        line = new VariantFileHeaderComplexLine(
                                "FORMAT",
                                VariantMerger.GENOTYPE_FILTER_KEY,
                                "Sample genotype filter. Similar in concept to the FILTER field.",
                                ".",
                                VCFHeaderLineType.String.toString(), null);
                        studyMetadata.getVariantHeader().getComplexLines().add(line);
                    } else {
                        throw new StorageEngineException("Unknown FORMAT field '" + extraFormatField + '\'');
                    }
                }

                VCFHeaderLineType type;
                if (Objects.equals(line.getNumber(), "1")) {
                    try {
                        type = VCFHeaderLineType.valueOf(line.getType());
                    } catch (IllegalArgumentException ignore) {
                        type = VCFHeaderLineType.String;
                    }
                } else {
                    //Fields with arity != 1 are loaded as String
                    type = VCFHeaderLineType.String;
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
                logger.debug(extraFormatField + " : " + type);
            }

            studyMetadata.getAttributes().put(Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), extraFieldsType);
        }

        int currentRelease = getMetadataManager().getProjectMetadata(options).getRelease();
        if (options.containsKey(Options.RELEASE.key())) {
            int release = options.getInt(Options.RELEASE.key(), Options.RELEASE.defaultValue());
            // Update current release
            if (currentRelease != release) {
                getMetadataManager().lockAndUpdateProject(pm -> {
                    if (release < pm.getRelease() || release <= 0) {
                        //ERROR, asking to use a release lower than currentRelease
                        throw StorageEngineException.invalidReleaseException(release, pm.getRelease());
                    } else {
                        // Update currentRelease in ProjectMetadata
                        pm.setRelease(release);
                    }
                    return pm;

                });
            }
        } else {
            options.put(Options.RELEASE.key(), currentRelease);
        }
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        return postLoad(input, output, null);
    }

    protected final URI postLoad(URI input, URI output, List<Integer> fileIds) throws StorageEngineException {
        List<Integer> finalFileIds;
        if (fileIds == null || fileIds.isEmpty()) {
            finalFileIds = Collections.singletonList(getFileId());
        } else {
            finalFileIds = fileIds;
        }

        // Check loaded variants BEFORE updating the StudyMetadata
        checkLoadedVariants(finalFileIds, getStudyMetadata());

        //Update StudyMetadata
        getMetadataManager().lockAndUpdate(getStudyId(), sm -> {
            securePostLoad(finalFileIds, sm);
            return sm;
        });
        return input;
    }

    public void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = getStudyId();

        // Update indexed files
        metadataManager.updateIndexedFiles(studyId, fileIds);

        // Update the cohort ALL. Invalidate if needed
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        BiMap<String, Integer> indexedSamples = metadataManager.getIndexedSamplesMap(studyId);

        // Register or update default cohort
        metadataManager.registerCohorts(studyId, Collections.singletonMap(defaultCohortName, indexedSamples.keySet()));

        logger.info("Add " + indexedSamples.size() + " loaded samples to Default Cohort \"" + defaultCohortName + '"');
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

    protected abstract void checkLoadedVariants(int fileId, StudyMetadata studyMetadata)
            throws StorageEngineException;

    protected void checkLoadedVariants(List<Integer> fileIds, StudyMetadata studyMetadata)
            throws StorageEngineException {
        for (Integer fileId : fileIds) {
            checkLoadedVariants(fileId, studyMetadata);
        }
    }


    public static String buildFilename(String studyName, int fileId) {
        int index = studyName.indexOf(":");
        if (index >= 0) {
            studyName = studyName.substring(index + 1);
        }
        if (fileId > 0) {
            return studyName + "_" + fileId;
        } else {
            return studyName;
        }
    }

    public VariantFileMetadata readVariantFileMetadata(URI input) throws StorageEngineException {
        VariantFileMetadata variantFileMetadata = variantReaderUtils.readVariantFileMetadata(input);
        // Ensure correct fileId
        // FIXME
//        variantFileMetadata.setId(String.valueOf(getFileId()));
        variantFileMetadata.setId(null);
        return variantFileMetadata;
    }

    /* --------------------------------------- */
    /*  StudyMetadata utils methods        */
    /* --------------------------------------- */

    protected StudyMetadata getOrCreateStudyMetadata() throws StorageEngineException {
        return ensureStudyMetadataExists(getStudyMetadata());
    }

    protected StudyMetadata ensureStudyMetadataExists(StudyMetadata studyMetadata) throws StorageEngineException {
        if (studyMetadata == null) {
            studyMetadata = getStudyMetadata();
            if (studyMetadata == null) {
                String studyName = options.getString(Options.STUDY.key(), Options.STUDY.defaultValue());
                logger.info("Creating a new StudyMetadata '{}'", studyName);
                studyMetadata = getMetadataManager().createStudy(studyName);
            }
        }
//        privateStudyMetadata = studyMetadata;
        setStudyId(studyMetadata.getId());
        return studyMetadata;
    }

    /**
     * Reads the study metadata.
     *
     * @return           The study metadata.
     * @throws StorageEngineException If the study metadata is not found
     */
    public final StudyMetadata getStudyMetadata() throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        final StudyMetadata studyMetadata;
        String study = options.getString(Options.STUDY.key());
        if (!StringUtils.isEmpty(study)) {
            studyMetadata = metadataManager.getStudyMetadata(study);
        } else if (privateStudyId != null) {
            studyMetadata = metadataManager.getStudyMetadata(privateStudyId);
        } else {
            throw new StorageEngineException("Unable to get StudyMetadata. Missing studyId or studyName");
        }
        privateStudyId = studyMetadata == null ? null : studyMetadata.getId();
        return studyMetadata;
    }

    public Thread newShutdownHook(String jobOperationName, List<Integer> files) throws StorageEngineException {
        return getMetadataManager().buildShutdownHook(jobOperationName, getStudyId(), files);
    }

    public VariantDBAdaptor getDBAdaptor() {
        return dbAdaptor;
    }


    public int getFileId() {
        return privateFileId;
    }

    protected void setFileId(int fileId) {
        privateFileId = fileId;
    }


    protected int getStudyId() throws StorageEngineException {
        if (privateStudyId == null) {
            privateStudyId = getStudyMetadata().getId();
            return privateStudyId;
        } else {
            return privateStudyId;
        }
    }

    protected void setStudyId(int studyId) {
        privateStudyId = studyId;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public VariantStorageMetadataManager getMetadataManager() {
        return getDBAdaptor().getMetadataManager();
    }
}
