package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantNormalizer;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.hpg.bigdata.core.io.ProtoFileWriter;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.runner.StringDataReader;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.runner.VcfVariantReader;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbaseTransformTask;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.HADOOP_BIN;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.HADOOP_LOAD_VARIANT_PENDING_FILES;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager
        .OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES;

/**
 * Created by mh719 on 13/05/2016.
 */
public abstract class AbstractHadoopVariantStorageETL extends VariantStorageETL {
    public static final String ARCHIVE_TABLE_PREFIX = "opencga_study_";
    protected final VariantHadoopDBAdaptor dbAdaptor;
    protected final Configuration conf;
    protected final HBaseCredentials archiveTableCredentials;
    protected final HBaseCredentials variantsTableCredentials;
    protected MRExecutor mrExecutor = null;

    public AbstractHadoopVariantStorageETL(
            StorageConfiguration configuration, String storageEngineId, Logger logger,
            VariantHadoopDBAdaptor dbAdaptor,
            VariantReaderUtils variantReaderUtils, ObjectMap options,
            HBaseCredentials archiveCredentials, MRExecutor mrExecutor,
            Configuration conf) {
        super(configuration, storageEngineId, logger, dbAdaptor, variantReaderUtils, options);
        this.archiveTableCredentials = archiveCredentials;
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        this.variantsTableCredentials = dbAdaptor == null ? null : dbAdaptor.getCredentials();
        this.conf = new Configuration(conf);
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        logger.info("PreTransform: " + input);
//        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        if (!options.containsKey(VariantStorageManager.Options.TRANSFORM_FORMAT.key())) {
            options.put(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "avro");
        }
        String transVal = options.getString(VariantStorageManager.Options.TRANSFORM_FORMAT.key());
        switch (transVal){
            case "avro":
            case "proto":
                break;
            default:
                throw new NotImplementedException(String.format("Output format %s not supported for Hadoop!", transVal));
        }
        if (!options.containsKey(VariantStorageManager.Options.GVCF.key())) {
            options.put(VariantStorageManager.Options.GVCF.key(), true);
        }
        boolean isGvcf = options.getBoolean(VariantStorageManager.Options.GVCF.key());
        if (!isGvcf) {
            throw new NotImplementedException("Only GVCF format supported!!!");
        }
        return super.preTransform(input);
    }

    @Override
    protected Pair<Long, Long> processProto(Path input, String fileName, Path output, VariantSource source, Path
            outputVariantsFile, Path outputMetaFile, boolean includeSrc, String parser, boolean
            generateReferenceBlocks, int batchSize, String extension, String compression) throws StorageManagerException {

        //Writer
        DataWriter<VcfSliceProtos.VcfSlice> dataWriter = new ProtoFileWriter<VcfSliceProtos.VcfSlice>(outputVariantsFile, compression);

        final Pair<VCFHeader, VCFHeaderVersion> header;
        final Path finalOutputMetaFile = output.resolve(fileName + ".file.json" + extension);   //TODO: Write META in
        // Normalizer
        VariantNormalizer normalizer = new VariantNormalizer();
        normalizer.setGenerateReferenceBlocks(generateReferenceBlocks);

        // Converter
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter(
                source.getStudyId(), source.getFileId(), source.getSamples());

        // Stats calculator
        VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(source);
        // final VariantVcfFactory factory = createVariantVcfFactory(source, fileName);


        if (parser.equalsIgnoreCase("htsjdk")) {
            logger.info("Using HTSJDK to read variants.");
            header = readHtsHeader(input);
        } else {
            throw new NotImplementedException("Please request to read other than vcf file format");
        }

        VcfVariantReader dataReader = new VcfVariantReader(
                new StringDataReader(input), header.getKey(), header.getValue(), converter, statsCalculator,
                normalizer);

        // Transformer
        VcfMeta meta = new VcfMeta(source);
        ArchiveHelper helper = new ArchiveHelper(conf, meta);
        VariantHbaseTransformTask transformTask = new VariantHbaseTransformTask(helper, null);

        logger.info("Generating output file {}", outputVariantsFile);

        long[] t = new long[]{0, 0, 0};
        long last = System.nanoTime();
        Long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        try {
            dataReader.open();
            dataReader.pre();
            dataWriter.open();
            dataWriter.pre();
            transformTask.pre();

            start = System.currentTimeMillis();
            last = System.nanoTime();
            // Process data
            List<Variant> read = dataReader.read(batchSize);
            t[0] += System.nanoTime() - last;
            last = System.nanoTime();
            while (!read.isEmpty()) {
                List<VcfSliceProtos.VcfSlice> slices = transformTask.apply(read);
                t[1] += System.nanoTime() - last;
                last = System.nanoTime();
                dataWriter.write(slices);
                t[2] += System.nanoTime() - last;
                last = System.nanoTime();
                read = dataReader.read(batchSize);
                t[0] += System.nanoTime() - last;
                last = System.nanoTime();
            }
            List<VcfSliceProtos.VcfSlice> drain = transformTask.drain();
            t[1] += System.nanoTime() - last;
            last = System.nanoTime();
            dataWriter.write(drain);
            t[2] += System.nanoTime() - last;

            end = System.currentTimeMillis();
            transformTask.post();
            dataReader.post();
            dataWriter.post();
        } catch (Exception e) {
            throw new StorageManagerException(
                    String.format("Error while Transforming file %s into %s ", input, outputVariantsFile), e);
        } finally {
            dataWriter.close();
            dataReader.close();
        }
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        ObjectWriter variantSourceObjectWriter = jsonObjectMapper.writerFor(VariantSource.class);
        try {
            String sourceJsonString = variantSourceObjectWriter.writeValueAsString(source);
            StringDataWriter.write(finalOutputMetaFile, Collections.singletonList(sourceJsonString));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        logger.info("Times for reading: {}, transforming {}, writing {}",
                TimeUnit.NANOSECONDS.toSeconds(t[0]),
                TimeUnit.NANOSECONDS.toSeconds(t[1]),
                TimeUnit.NANOSECONDS.toSeconds(t[2]));
        return new ImmutablePair<>(start, end);
    }

    public void merge(int studyId, List<Integer> pendingFiles) throws StorageManagerException {
        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = options.getString(OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES);
        }
        options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);

        Class execClass = VariantTableDriver.class;
        String args = VariantTableDriver.buildCommandLineArgs(variantsTableCredentials.getHostAndPort(),
                archiveTableCredentials.getTable(),
                variantsTableCredentials.getTable(), studyId, pendingFiles, options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Loading file {} into analysis table '{}'", pendingFiles, variantsTableCredentials.getTable());
        logger.info(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageManagerException("Error loading files " + pendingFiles + " into variant table \""
                    + variantsTableCredentials.getTable() + "\"");
        }
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws
            StorageManagerException {
        logger.warn("Skip check loaded variants");
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return super.postTransform(input);
    }
}
