package org.opencb.opencga.storage.hadoop.variant.archive;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VariantToVcfSliceMapper;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VcfSliceCombiner;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VcfSliceReducer;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VcfSliceWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * @author mh719
 */
public class ArchiveDriver extends Configured implements Tool {

    public static final String CONFIG_ARCHIVE_FILE_ID             = "opencga.archive.file.id";
    public static final String CONFIG_ARCHIVE_INPUT_FILE_VCF      = "opencga.archive.input.file.vcf";
    public static final String CONFIG_ARCHIVE_INPUT_FILE_VCF_META = "opencga.archive.input.file.vcf.meta";
    public static final String CONFIG_ARCHIVE_TABLE_NAME          = "opencga.archive.table.name";
    public static final String CONFIG_ARCHIVE_TABLE_COMPRESSION   = "opencga.archive.table.compression";
    public static final String CONFIG_ARCHIVE_CHUNK_SIZE          = "opencga.archive.chunk_size";
    public static final String CONFIG_ARCHIVE_ROW_KEY_SEPARATOR   = "opencga.archive.row_key_sep";

    public static final int DEFAULT_CHUNK_SIZE = 1000;

    private final Logger logger = LoggerFactory.getLogger(ArchiveDriver.class);

    public ArchiveDriver() {
    }

    public ArchiveDriver(Configuration conf) {
        super(conf);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        HBaseConfiguration.addHbaseResources(conf);

        URI inputFile = URI.create(conf.get(CONFIG_ARCHIVE_INPUT_FILE_VCF));
        URI inputMetaFile = URI.create(conf.get(CONFIG_ARCHIVE_INPUT_FILE_VCF_META));
        String tableName = conf.get(CONFIG_ARCHIVE_TABLE_NAME);
        int studyId = conf.getInt(GenomeHelper.CONFIG_STUDY_ID, -1);
        int fileId = conf.getInt(CONFIG_ARCHIVE_FILE_ID, -1);
        GenomeHelper genomeHelper = new GenomeHelper(conf);

/*  SERVER details  */
        if (createArchiveTableIfNeeded(genomeHelper, tableName)) {
            logger.info(String.format("Create table '%s' in hbase!", tableName));
        } else {
            logger.info(String.format("Table '%s' exists in hbase!", tableName));
        }

        // add metadata config as string
        VcfMeta meta = readMetaData(conf, inputMetaFile);
        // StudyID and FileID may not be correct. Use the given through the CLI and overwrite the values from meta.
        meta.getVariantSource().setStudyId(Integer.toString(studyId));
        meta.getVariantSource().setFileId(Integer.toString(fileId));
        storeMetaData(meta, conf);

        /* JOB setup */
        final Job job = Job.getInstance(conf, "opencga: Load file [" + fileId + "] to ArchiveTable '" + tableName + "'");
        job.setJarByClass(getClass());
        conf = job.getConfiguration();
        conf.set("mapreduce.job.user.classpath.first", "true");

        // input
        FileInputFormat.addInputPath(job, new Path(inputFile));

        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // mapper
        job.setMapperClass(VariantToVcfSliceMapper.class);

        // combiner
        job.setCombinerClass(VcfSliceCombiner.class);


        TableMapReduceUtil.initTableReducerJob(tableName, VcfSliceReducer.class, job, null, null, null, null,
                conf.getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true));
        job.setMapOutputValueClass(VcfSliceWritable.class);

        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);

        try (HadoopVariantSourceDBAdaptor manager = new HadoopVariantSourceDBAdaptor(conf)) {
            manager.updateLoadedFilesSummary(studyId, Collections.singletonList(fileId));
        }
        return succeed ? 0 : 1;
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createArchiveTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con) throws IOException {
        Algorithm compression = Compression.getCompressionAlgorithmByName(
                genomeHelper.getConf().get(CONFIG_ARCHIVE_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName()));
        return HBaseManager.createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(), compression);
    }

    private void storeMetaData(VcfMeta meta, Configuration conf) throws IOException {
        try (HadoopVariantSourceDBAdaptor manager = new HadoopVariantSourceDBAdaptor(conf)) {
            manager.updateVcfMetaData(meta);
        }
    }

    private VcfMeta readMetaData(Configuration conf, URI inputMetaFile) throws IOException {
        Path from = new Path(inputMetaFile);
        FileSystem fs = FileSystem.get(conf);
        DatumReader<VariantFileMetadata> userDatumReader = new SpecificDatumReader<>(VariantFileMetadata.class);
        VariantFileMetadata variantFileMetadata;
        if (inputMetaFile.toString().endsWith("json") || inputMetaFile.toString().endsWith("json.gz")) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

            try (InputStream ids = inputMetaFile.toString().endsWith("json.gz") ? new GZIPInputStream(fs.open(from)) : fs.open(from)) {
                variantFileMetadata = objectMapper.readValue(ids, VariantSource.class).getImpl();
            }
        } else {
            try (FSDataInputStream ids = fs.open(from);
                 DataFileStream<VariantFileMetadata> dataFileReader = new DataFileStream<>(ids, userDatumReader)) {
                Iterator<VariantFileMetadata> iter = dataFileReader.iterator();
                if (!iter.hasNext()) {
                    throw new IllegalStateException(String.format("No Meta data object found in %s !!!", inputMetaFile));
                }
                variantFileMetadata = iter.next();
                if (iter.hasNext()) {
                    logger.warn(String.format("More than 1 entry found in metadata file %s", inputMetaFile));
                }
            }
        }
        return new VcfMeta(new VariantSource(variantFileMetadata));
    }

    public static String buildCommandLineArgs(URI input, URI inputMeta, String server, String outputTable,
                                              int studyId, int fileId, Map<String, Object> other) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(input).append(' ')
                .append(inputMeta).append(' ')
                .append(server).append(' ')
                .append(outputTable).append(' ')
                .append(studyId).append(' ')
                .append(fileId);
        addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
    }

    public static void addOtherParams(Map<String, Object> other, StringBuilder stringBuilder) {
        for (Map.Entry<String, Object> entry : other.entrySet()) {
            Object value = entry.getValue();
            if (value != null && (value instanceof Number
                    || value instanceof Boolean
                    || value instanceof String && !((String) value).contains(" ") && !((String) value).isEmpty())) {
                stringBuilder.append(' ').append(entry.getKey()).append(' ').append(value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(privateMain(args, null));
    }

    public static int privateMain(String[] args, Configuration conf) throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        ArchiveDriver driver = new ArchiveDriver();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();

        int fixedSizeArgs = 6;
        if (toolArgs.length < fixedSizeArgs || (toolArgs.length - fixedSizeArgs) % 2 != 0) {
            System.err.printf("Usage: %s [generic options] <avro> <avro-meta> <server> <output-table> <study-id> <file-id>"
                    + " [<key> <value>]*\n",
                    ArchiveDriver.class.getSimpleName());
            System.err.println("Found argc:" + toolArgs.length + ", argv: " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        conf.set(CONFIG_ARCHIVE_INPUT_FILE_VCF, toolArgs[0]);
        conf.set(CONFIG_ARCHIVE_INPUT_FILE_VCF_META, toolArgs[1]);
        conf = HBaseManager.addHBaseSettings(conf, toolArgs[2]);
        conf.set(CONFIG_ARCHIVE_TABLE_NAME, toolArgs[3]);
        conf.set(GenomeHelper.CONFIG_STUDY_ID, toolArgs[4]);
        conf.set(CONFIG_ARCHIVE_FILE_ID, toolArgs[5]);
        for (int i = fixedSizeArgs; i < toolArgs.length; i = i + 2) {
            conf.set(toolArgs[i], toolArgs[i + 1]);
        }
        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);

        return driver.run(toolArgs);
    }

}
