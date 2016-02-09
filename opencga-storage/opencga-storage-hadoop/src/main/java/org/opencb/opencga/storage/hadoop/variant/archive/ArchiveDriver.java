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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
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
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class ArchiveDriver extends Configured implements Tool {

    @Deprecated
    public static final String HBASE_MASTER = "hbase.master";

    public static final String OPT_VCF_FILE = "opencga.file.vcf";
    public static final String OPT_VCF_META_FILE = "opencga.file.vcfmeta";

    private final Logger logger = LoggerFactory.getLogger(ArchiveDriver.class);

    public ArchiveDriver() {
    }

    public ArchiveDriver(Configuration conf) {
        super(conf);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        URI inputFile = URI.create(conf.get(OPT_VCF_FILE));
        URI inputMetaFile = URI.create(conf.get(OPT_VCF_META_FILE));
        String tableName = conf.get(GenomeHelper.CONFIG_ARCHIVE_TABLE);
        int studyId = conf.getInt(GenomeHelper.OPENCGA_STORAGE_HADOOP_STUDY_ID, -1);
        int fileId = conf.getInt(GenomeHelper.CONFIG_FILE_ID, -1);
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
        storeMetaData(meta, tableName, conf);

        GenomeHelper.setChunkSize(conf, 1000);

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


        TableMapReduceUtil.initTableReducerJob(tableName, null, job, null, null, null, null, conf.getBoolean("addDependencyJars", true));
        job.setMapOutputValueClass(Put.class);


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

        try (ArchiveFileMetadataManager manager = new ArchiveFileMetadataManager(tableName, conf, null)) {
            manager.updateLoadedFilesSummary(Collections.singletonList(fileId));
        }

        return succeed ? 0 : 1;
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createArchiveTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con) throws IOException {
        return genomeHelper.getHBaseManager().createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(),
                Compression.Algorithm.GZ);
    }

    private void storeMetaData(VcfMeta meta, String tableName, Configuration conf) throws IOException {
        try (ArchiveFileMetadataManager manager = new ArchiveFileMetadataManager(tableName, conf, null)) {
            manager.updateVcfMetaData(meta);
        }
    }

//    private void addMetaData(Configuration conf, URI inputMetaFile) throws IOException {
//        Class<GeneratedMessage> clazz = com.google.protobuf.GeneratedMessage.class;
//        logger.debug(clazz.getProtectionDomain().getCodeSource().getLocation().toString());
//        URL url = clazz.getResource('/'+clazz.getName().replace('.', '/')+".class");
//        logger.debug(url.toString());
//        VcfMeta meta = getMetaData(conf, inputMetaFile);
//        String protocFile = inputMetaFile + ".protoc3";
//        Path to = new Path(protocFile);
//        FileSystem fs = FileSystem.get(conf);
//        try (FSDataOutputStream os = fs.create(to, true)) {
//            os.write(meta.toByteArray());
//        }
//        ArchiveHelper.setMetaProtoFile(conf, URI.create(protocFile));
////        GenomeVariantHelper.setMetaProtoString(conf, meta.getStudyIdBytes().toStringUtf8());
//    }

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

    public static String buildCommandLineArgs(URI input, URI inputMeta, String server, String outputTable, int studyId, int fileId) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(input).append(' ')
                .append(inputMeta).append(' ')
                .append(server).append(' ')
                .append(outputTable).append(' ')
                .append(studyId).append(' ')
                .append(fileId);
        return stringBuilder.toString();
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

        if (toolArgs.length != 6) {
            System.err.printf("Usage: %s [generic options] <avro> <avro-meta> <server> <output-table> <study-id> <file-id>\n",
                    ArchiveDriver.class.getSimpleName());
            System.err.println("Found " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        conf.set(OPT_VCF_FILE, toolArgs[0]);
        conf.set(OPT_VCF_META_FILE, toolArgs[1]);
        VariantTableDriver.addHBaseSettings(conf, toolArgs[2]);
        conf.set(GenomeHelper.CONFIG_ARCHIVE_TABLE, toolArgs[3]);
        conf.set(GenomeHelper.OPENCGA_STORAGE_HADOOP_STUDY_ID, toolArgs[4]);
        conf.set(GenomeHelper.CONFIG_FILE_ID, toolArgs[5]);

        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);

        return driver.run(toolArgs);
    }

}
