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

import com.fasterxml.jackson.databind.ObjectMapper;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantReferenceBlockCreatorTask;
import org.opencb.biodata.tools.variant.VariantSorterTask;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.biodata.tools.variant.normalizer.extensions.VariantNormalizerExtensionFactory;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.io.avro.AvroEncoder;
import org.opencb.commons.io.avro.AvroFileWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.transform.MalformedVariantHandler;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;

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
    protected final IOConnectorProvider ioConnectorProvider;
    private final Logger logger = LoggerFactory.getLogger(VariantStoragePipeline.class);
    protected final ObjectMap transformStats = new ObjectMap();
    protected final ObjectMap loadStats = new ObjectMap();
    protected Integer privateFileId;
    protected Integer privateStudyId;
//    protected StudyMetadata privateStudyMetadata;

    /**
     * @param configuration     Storage Configuration
     * @param storageEngineId   StorageEngineID
     * @param dbAdaptor         VariantDBAdaptor. Can be null if the load step is skipped
     * @param ioConnectorProvider IOConnector
     * @param options           Unique copy of the options to be used. This object can not be shared.
     */
    public VariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, VariantDBAdaptor dbAdaptor,
                                  IOConnectorProvider ioConnectorProvider, ObjectMap options) {
        this.configuration = configuration;
        this.storageEngineId = storageEngineId;
        this.dbAdaptor = dbAdaptor;
        this.variantReaderUtils = new VariantReaderUtils(ioConnectorProvider);
        this.ioConnectorProvider = ioConnectorProvider;
        this.options = options;
        if (dbAdaptor == null) {
            options.put(VariantStorageOptions.TRANSFORM_ISOLATE.key(), true);
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
    public ObjectMap getLoadStats() {
        return loadStats;
    }

    @Override
    public URI preTransform(URI input) throws StorageEngineException, IOException, FileFormatException {
        String fileName = VariantReaderUtils.getFileName(input);
        String study = options.getString(VariantStorageOptions.STUDY.key());

        if (VariantReaderUtils.isGvcf(fileName)) {
            logger.info("Detected gVCF input file.");
            options.put(VariantStorageOptions.GVCF.key(), true);
        }

        boolean isolate = options.getBoolean(VariantStorageOptions.TRANSFORM_ISOLATE.key(),
                VariantStorageOptions.TRANSFORM_ISOLATE.defaultValue());
        StudyMetadata metadata;
        if (isolate) {
            logger.debug("Isolated study configuration");
            metadata = new StudyMetadata(-1, "unknown");
            metadata.setAggregationStr(options.getString(VariantStorageOptions.STATS_AGGREGATION.key(),
                    VariantStorageOptions.STATS_AGGREGATION.defaultValue().toString()));
            options.put(VariantStorageOptions.TRANSFORM_ISOLATE.key(), true);
            privateStudyId = -1;
        } else {
            VariantStorageMetadataManager smm = dbAdaptor.getMetadataManager();
            ensureStudyMetadataExists(null);

            StudyMetadata studyMetadata = smm.updateStudyMetadata(study, existingStudyMetadata -> {
                if (existingStudyMetadata.getAggregation() == null) {
                    existingStudyMetadata.setAggregationStr(options.getString(VariantStorageOptions.STATS_AGGREGATION.key(),
                            VariantStorageOptions.STATS_AGGREGATION.defaultValue().toString()));
                }
                return existingStudyMetadata;
            });
            setFileId(smm.registerFile(studyMetadata.getId(), input.getPath()));
        }

        return input;
    }

    protected VariantFileMetadata createEmptyVariantFileMetadata(URI input) {
        VariantFileMetadata fileMetadata = VariantReaderUtils.createEmptyVariantFileMetadata(input);
        int fileId;
        if (options.getBoolean(VariantStorageOptions.TRANSFORM_ISOLATE.key(),
                VariantStorageOptions.TRANSFORM_ISOLATE
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
     * @param input Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigree Pedigree input file. Accepted formats: *.ped
     * @param output The destination folder
     * @throws StorageEngineException If any IO problem
     */
    @Override
    public URI transform(URI input, URI pedigree, URI output) throws StorageEngineException {
        // input: VcfReader
        // output: JsonWriter


        boolean failOnError = options.getBoolean(
                VariantStorageOptions.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(),
                VariantStorageOptions.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.defaultValue());
        String format = options.getString(
                VariantStorageOptions.TRANSFORM_FORMAT.key(),
                VariantStorageOptions.TRANSFORM_FORMAT.defaultValue());
        String parser = options.getString("transform.parser", HTSJDK_PARSER);

        boolean stdin = options.getBoolean(STDIN.key(), STDIN.defaultValue());
        boolean stdout = options.getBoolean(STDOUT.key(), STDOUT.defaultValue());

        // Create empty VariantFileMetadata
        VariantFileMetadata metadataTemplate = createEmptyVariantFileMetadata(input);
        // Read VariantFileMetadata
        final VariantFileMetadata metadata = variantReaderUtils.readVariantFileMetadata(input, metadataTemplate, stdin);

        String fileName = UriUtils.fileName(input);
        String studyId = String.valueOf(getStudyId());

        int batchSize = options.getInt(
                VariantStorageOptions.TRANSFORM_BATCH_SIZE.key(),
                VariantStorageOptions.TRANSFORM_BATCH_SIZE.defaultValue());

        String compression = options.getString(
                VariantStorageOptions.TRANSFORM_COMPRESSION.key(),
                VariantStorageOptions.TRANSFORM_COMPRESSION.defaultValue());
        String extension = "";
        int numTasks = options.getInt(
                VariantStorageOptions.TRANSFORM_THREADS.key(),
                VariantStorageOptions.TRANSFORM_THREADS.defaultValue());
        int capacity = options.getInt("blockingQueueCapacity", numTasks * 2);

        if ("gzip".equalsIgnoreCase(compression) || "gz".equalsIgnoreCase(compression)) {
            extension = ".gz";
        } else if ("snappy".equalsIgnoreCase(compression) || "snz".equalsIgnoreCase(compression)) {
            extension = ".snappy";
        } else if (!compression.isEmpty()) {
            throw new IllegalArgumentException("Unknown compression method " + compression);
        }

        URI outputMalformedVariants = output.resolve(fileName + '.' + VariantReaderUtils.MALFORMED_FILE + ".txt");
        URI outputVariantsFile = output.resolve(fileName + '.' + VariantReaderUtils.VARIANTS_FILE + '.' + format + extension);
        URI outputMetaFile = VariantReaderUtils.getMetaFromTransformedFile(outputVariantsFile);

        // Close at the end!
        final MalformedVariantHandler malformedHandler;
        try {
            malformedHandler = new MalformedVariantHandler(() -> {
                try {
                    return ioConnectorProvider.newOutputStream(outputMalformedVariants);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numTasks)
                .setBatchSize(batchSize)
                .setCapacity(capacity)
                .setSorted(true)
                .build();

        logger.info("Transforming variants using {} into {} ...", parser, format);
        StopWatch stopWatch;


        //Reader
        long fileSize;
        StringDataReader stringReader;
        try {
            stringReader = stdin ? new StringDataReader(System.in) : new StringDataReader(input, ioConnectorProvider);
            fileSize = stdin ? -1 : ioConnectorProvider.size(input);
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }
        ProgressLogger progressLogger = new ProgressLogger("Transforming file:", fileSize, 200);
        stringReader.setReadBytesListener((totalRead, delta) -> progressLogger.increment(delta, "Bytes"));

        VariantSetStatsCalculator statsCalculator = new VariantSetStatsCalculator(studyId, metadata);

        logger.info("Using HTSJDK to read variants.");
        Pair<VCFHeader, VCFHeaderVersion> header = variantReaderUtils.readHtsHeader(input, stdin);

        boolean parallelParse = true;
        Task<Variant, Variant> normalizer;
        if (getOptions().getBoolean(NORMALIZATION_SKIP.key())) {
            normalizer = null;
        } else {
            boolean generateReferenceBlocks = options.getBoolean(VariantStorageOptions.GVCF.key(), false);
            // Do not run parallelParse when generating reference blocks, as the task is stateful
            parallelParse = !generateReferenceBlocks;
            normalizer = initNormalizer(metadata);
        }

        Supplier<Task<String, Variant>> task = () ->
                new VariantTransformTask(header.getKey(), header.getValue(), studyId, metadata, statsCalculator, normalizer)
                .setFailOnError(failOnError)
                .addMalformedErrorHandler(malformedHandler)
                .setIncludeSrc(false);

        ParallelTaskRunner ptr;
        if ("avro".equals(format)) {
            Supplier<Task<Variant, ByteBuffer>> encoder = () -> Task.forEach(Variant::getImpl)
                    .then(new AvroEncoder<>(VariantAvro.getClassSchema(), true));

            //Writer
            DataWriter<ByteBuffer> dataWriter;
            try {
                if (stdout) {
                    dataWriter = new AvroFileWriter<>(VariantAvro.getClassSchema(), compression, System.out);
                } else {
                    dataWriter = new AvroFileWriter<>(VariantAvro.getClassSchema(), compression,
                            ioConnectorProvider.newOutputStreamRaw(outputVariantsFile), true);
                }
            } catch (IOException e) {
                throw StorageEngineException.ioException(e);
            }

            ptr = buildTransformPtr(parallelParse, stringReader, task, encoder, dataWriter, config);
        } else if ("json".equals(format)) {
            Supplier<Task<Variant, String>> encoder = () -> Task.forEach(Variant::toJson);

            //Writers
            StringDataWriter dataWriter;
            if (stdout) {
                dataWriter = new StringDataWriter(System.out, true);
            } else {
                try {
                    dataWriter = new StringDataWriter(
                            ioConnectorProvider.newOutputStream(outputVariantsFile), true, true);
                } catch (IOException e) {
                    throw StorageEngineException.ioException(e);
                }
            }

            ptr = buildTransformPtr(parallelParse, stringReader, task, encoder, dataWriter, config);
        } else if ("proto".equals(format)) {
            ptr = transformProto(metadata, outputVariantsFile, stringReader, task);
        } else {
            throw new IllegalArgumentException("Unknown format " + format);
        }

        stopWatch = StopWatch.createStarted();
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error while executing TransformVariants in ParallelTaskRunner", e);
        }
        stopWatch.stop();

        logger.info("Variants transformed in " + TimeUtils.durationToString(stopWatch));

        try (OutputStream outputMetadataStream = ioConnectorProvider.newOutputStream(outputMetaFile)) {
            ObjectMapper jsonObjectMapper = new ObjectMapper();
            jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
            jsonObjectMapper.writeValue(outputMetadataStream, metadata.getImpl());
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }


        // Close the malformed variant handler
        malformedHandler.close();
        if (malformedHandler.getMalformedLines() > 0) {
            getTransformStats().put("malformed lines", malformedHandler.getMalformedLines());
        }

        return outputVariantsFile;
    }

    protected Task<Variant, Variant> initNormalizer(VariantFileMetadata metadata) throws StorageEngineException {
        boolean generateReferenceBlocks = options.getBoolean(GVCF.key(), false);
        Collection<String> enabledExtensions = getOptions()
                .getAsStringList(NORMALIZATION_EXTENSIONS.key());
        VariantNormalizer.VariantNormalizerConfig normalizerConfig = new VariantNormalizer.VariantNormalizerConfig()
                .setReuseVariants(true)
                .setNormalizeAlleles(true)
                .setDecomposeMNVs(false)
                .setGenerateReferenceBlocks(generateReferenceBlocks);
        String referenceGenome = getOptions().getString(NORMALIZATION_REFERENCE_GENOME.key());
        if (StringUtils.isNotEmpty(referenceGenome)) {
            try {
                logger.info("Enable left alignment with reference genome file '{}'", referenceGenome);
                normalizerConfig.enableLeftAlign(referenceGenome);
            } catch (IOException e) {
                throw StorageEngineException.ioException(e);
            }
        }

        Task<Variant, Variant> normalizer = new VariantNormalizer(normalizerConfig)
                .configure(metadata.getHeader());
        if (generateReferenceBlocks) {
            normalizer = normalizer
                    .then(new VariantSorterTask(100)) // Sort before generating reference blocks
                    .then(new VariantReferenceBlockCreatorTask(metadata.getHeader()));
        }
        if (CollectionUtils.isNotEmpty(enabledExtensions)) {
            VariantNormalizerExtensionFactory extensionFactory;
            if (enabledExtensions.size() == 1 && enabledExtensions.contains(ParamConstants.ALL)) {
                extensionFactory = new VariantNormalizerExtensionFactory();
            } else {
                extensionFactory = new VariantNormalizerExtensionFactory(new HashSet<>(enabledExtensions));
            }
            Task<Variant, Variant> extension = extensionFactory.buildExtensions(metadata);
            if (extension != null) {
                normalizer = normalizer.then(extension);
            }
        }

        return normalizer;
    }

    protected <W> ParallelTaskRunner<?, W> buildTransformPtr(boolean parallelParse,
                                                     DataReader<String> stringReader,
                                                     Supplier<Task<String, Variant>> task,
                                                     Supplier<Task<Variant, W>> encoder,
                                                     DataWriter<W> dataWriter,
                                                     ParallelTaskRunner.Config config) {

        logger.info("Multi thread transform... [1 reading, {} transforming, 1 writing]", config.getNumTasks());
        if (parallelParse) {
            return new ParallelTaskRunner<String, W>(
                    stringReader,
                    () -> task.get().then(encoder.get()),
                    dataWriter,
                    config
            );
        } else {
            return new ParallelTaskRunner<Variant, W>(
                    stringReader.then(task.get()),
                    encoder,
                    dataWriter,
                    config
            );
        }
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

    protected ParallelTaskRunner transformProto(
            VariantFileMetadata metadata, URI outputVariantsFile,
            DataReader<String> stringReader, Supplier<Task<String, Variant>> task)
            throws StorageEngineException {
        throw new NotImplementedException("Please request feature");
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return input;
    }

    /**
     * PreLoad step for modify the StudyMetadata and register the file to be loaded.
     * This step is executed inside a study lock.
     *
     * @param input         input
     * @param output        output
     * @throws StorageEngineException  If any condition is wrong
     */
    @Override
    public URI preLoad(URI input, URI output) throws StorageEngineException {
        getOrCreateStudyMetadata();
        int studyId = getStudyId();

        int currentRelease = getMetadataManager().getProjectMetadata(options).getRelease();
        if (options.containsKey(VariantStorageOptions.RELEASE.key())) {
            int release = options.getInt(VariantStorageOptions.RELEASE.key(), VariantStorageOptions.RELEASE.defaultValue());
            // Update current release
            if (currentRelease != release) {
                getMetadataManager().updateProjectMetadata(pm -> {
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
            options.put(VariantStorageOptions.RELEASE.key(), currentRelease);
        }

        if (getOptions().getBoolean(FAMILY.key()) || getOptions().getBoolean(SOMATIC.key())) {
            if (YesNoAuto.parse(getOptions(), LOAD_HOM_REF.key()) == YesNoAuto.AUTO) {
                getOptions().put(LOAD_HOM_REF.key(), YesNoAuto.YES);
            }
        }

        VariantFileMetadata fileMetadata = readVariantFileMetadata(input);

        preLoadRegisterAndValidateFile(studyId, fileMetadata);

        //Get the studyConfiguration. If there is no StudyMetadata, create a empty one.
        dbAdaptor.getMetadataManager().updateStudyMetadata(studyId, study -> {
            securePreLoad(study, fileMetadata);
            return study;
        });
        return input;
    }

    protected void preLoadRegisterAndValidateFile(int studyId, VariantFileMetadata fileMetadata) throws StorageEngineException {
        int fileId = getMetadataManager().registerFile(studyId, fileMetadata);
        setFileId(fileId);
    }

    /**
     * Secure preLoad step for modify the StudyMetadata.
     * This step is executed inside a study lock.
     *
     * @param studyMetadata         StudyMetadata
     * @param fileMetadata          VariantFileMetadata
     * @throws StorageEngineException  If any condition is wrong
     */
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata fileMetadata) throws StorageEngineException {
//        final boolean excludeGenotypes = options.getBoolean(
//                VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
//                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
//        if (getMetadataManager().getIndexedFiles(studyId).isEmpty()) {
//            // First indexed file
//            // Use the EXCLUDE_GENOTYPES value from CLI. Write in StudyMetadata.attributes
//            studyMetadata.setAggregationStr(options.getString(VariantStorageOptions.STATS_AGGREGATION.key(),
//                    VariantStorageOptions.STATS_AGGREGATION.defaultValue().toString()));
//            studyMetadata.getAttributes().put(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
//        } else {
//            // Not first indexed file
//            // Use the EXCLUDE_GENOTYPES value from StudyMetadata. Ignore CLI value
//            excludeGenotypes = studyMetadata.getAttributes()
//                    .getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
//            options.put(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), excludeGenotypes);
//        }

        // Get Extra genotype fields
        Stream<String> stream;
        if (StringUtils.isNotEmpty(options.getString(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()))
                && !options.getString(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()).equals(VariantQueryUtils.ALL)) {
            // If ExtraGenotypeFields are provided by command line, check that those fields are going to be loaded.

            if (options.getString(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()).equals(VariantQueryUtils.NONE)) {
                stream = Stream.empty();
            } else {
                stream = options.getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()).stream();
            }
        } else {
            // Otherwise, add all format fields
            stream = fileMetadata.getHeader().getComplexLines()
                    .stream()
                    .filter(line -> line.getKey().equals("FORMAT"))
                    .map(VariantFileHeaderComplexLine::getId);

        }
        List<String> extraFormatFields = studyMetadata.getAttributes().getAsStringList(EXTRA_FORMAT_FIELDS.key());
        stream.forEach(format -> {
            if (!extraFormatFields.contains(format) && !format.equals(VariantMerger.GT_KEY)) {
                extraFormatFields.add(format);
            }
        });
        studyMetadata.getAttributes().put(EXTRA_FORMAT_FIELDS.key(), extraFormatFields);
        getOptions().put(EXTRA_FORMAT_FIELDS.key(), extraFormatFields);

        List<String> formatsFields = new ArrayList<>(extraFormatFields.size() + 1);
        formatsFields.add(VCFConstants.GENOTYPE_KEY);
        formatsFields.addAll(extraFormatFields);
        studyMetadata.addVariantFileHeader(fileMetadata.getHeader(), formatsFields);


        // Check if EXTRA_FORMAT_FIELDS_TYPE is filled
        if (!studyMetadata.getAttributes().containsKey(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key())) {
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

            studyMetadata.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), extraFieldsType);
        }

        if (studyMetadata.getVariantHeaderLine("INFO", StudyEntry.VCF_ID) == null) {
            studyMetadata.getVariantHeader().getComplexLines().add(new VariantFileHeaderComplexLine("INFO", StudyEntry.VCF_ID, "", "1",
                    VCFHeaderLineType.String.toString(), Collections.emptyMap()));
        }
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        return postLoad(input, output, null);
    }

    protected final URI postLoad(URI input, URI output, List<Integer> fileIds) throws StorageEngineException {
        int studyId = getStudyId();
        List<Integer> finalFileIds;
        if (fileIds == null || fileIds.isEmpty()) {
            finalFileIds = Collections.singletonList(getFileId());
        } else {
            finalFileIds = fileIds;
        }

        // Check loaded variants BEFORE updating the StudyMetadata
        checkLoadedVariants(finalFileIds, getStudyMetadata());

        // Update the cohort ALL. Invalidate if needed
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;

        // Register or update default cohort
        Set<Integer> samples = new HashSet<>();
        for (Integer fileId : finalFileIds) {
            samples.addAll(getMetadataManager().getFileMetadata(studyId, fileId).getSamples());
        }
        getMetadataManager().addSamplesToCohort(studyId, defaultCohortName, samples);

        logger.info("Add " + samples.size() + " loaded samples to Default Cohort \"" + defaultCohortName + '"');

        // Update indexed files
        getMetadataManager().addIndexedFiles(studyId, finalFileIds);

        //Update StudyMetadata
        getMetadataManager().updateStudyMetadata(studyId, sm -> {
            securePostLoad(finalFileIds, sm);
            return sm;
        });
        return input;
    }

    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
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
                String studyName = options.getString(VariantStorageOptions.STUDY.key(), VariantStorageOptions.STUDY.defaultValue());
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
        String study = options.getString(VariantStorageOptions.STUDY.key());
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
