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
import org.apache.hadoop.hbase.client.Put;
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
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class ArchiveDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveDriver.class);

    @Deprecated public static final String HBASE_MASTER = "hbase.master";
    public static final String OPT_VCF_FILE = "opencga.file.vcf";
    public static final String OPT_VCF_META_FILE = "opencga.file.vcfmeta";

    public ArchiveDriver () {
    }

    public ArchiveDriver (Configuration conf) {
        super(conf);
    }

    public int run (String[] args) throws Exception {
        Configuration conf = getConf();
        URI inputFile = URI.create(conf.get(OPT_VCF_FILE));
        URI inputMetaFile = URI.create(conf.get(OPT_VCF_META_FILE));
        String tablename = conf.get(GenomeHelper.CONFIG_ARCHIVE_TABLE);

/*  SERVER details  */
        
        // add metadata config as string
        VcfMeta meta = readMetaData(conf, inputMetaFile);
        storeMetaData(meta, tablename, conf);
        conf.set(GenomeHelper.CONFIG_FILE_ID, meta.getVariantSource().getFileId());
        ArchiveHelper archiveHelper = new ArchiveHelper(conf, meta);

        /* JOB setup */
        Job job = Job.getInstance(conf, "Genome Variant to HBase");
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
      

        TableMapReduceUtil.initTableReducerJob(tablename, null, job);
        job.setMapOutputValueClass(Put.class);

        if( HBaseUtils.createTableIfNeeded(tablename, archiveHelper.getColumnFamily(), job.getConfiguration())){
            LOG.info(String.format("Create table '%s' in hbase!", tablename));
        }
        // TODO: Update list of indexed files
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void storeMetaData(VcfMeta meta, String tablename, Configuration conf) throws IOException {
        try (ArchiveFileMetadataManager manager = new ArchiveFileMetadataManager(tablename, conf, null)) {
            manager.updateVcfMetaData(meta);
        }
    }

//    private void addMetaData(Configuration conf, URI inputMetaFile) throws IOException {
//        Class<GeneratedMessage> clazz = com.google.protobuf.GeneratedMessage.class;
//        LOG.debug(clazz.getProtectionDomain().getCodeSource().getLocation().toString());
//        URL url = clazz.getResource('/'+clazz.getName().replace('.', '/')+".class");
//        LOG.debug(url.toString());
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

    private VcfMeta readMetaData (Configuration conf, URI inputMetaFile) throws IOException {
        Path from = new Path(inputMetaFile);
        FileSystem fs = FileSystem.get(conf);
        DatumReader<VariantFileMetadata> userDatumReader = new SpecificDatumReader<>(VariantFileMetadata.class);
        VariantFileMetadata variantFileMetadata;
        if (inputMetaFile.toString().endsWith("json") || inputMetaFile.toString().endsWith("json.gz")) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

            try (InputStream ids = inputMetaFile.toString().endsWith("json.gz")? new GZIPInputStream(fs.open(from)) : fs.open(from)) {
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
                    LOG.warn(String.format("More than 1 entry found in metadata file %s", inputMetaFile));
                }
            }
        }
        VcfMeta meta = new VcfMeta(new VariantSource(variantFileMetadata));
        return meta;
    }


    public static void main (String[] args) throws Exception {
        System.exit(Main(args));
    }

    public static int Main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        ArchiveDriver driver = new ArchiveDriver();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        
        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();
        
        if (toolArgs.length != 4) {
            System.err.printf("Usage: %s [generic options] <avro> <avro-meta> <server> <output-table>\n",
                    ArchiveDriver.class.getSimpleName());
            System.err.println("Found " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        conf.set(OPT_VCF_FILE, toolArgs[0]);
        conf.set(OPT_VCF_META_FILE, toolArgs[1]);
        VariantTableDriver.addHBaseSettings(conf, toolArgs[2]);
        conf.set(GenomeHelper.CONFIG_ARCHIVE_TABLE, toolArgs[3]);

        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);
        
        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);

        return exitCode;
    }

}
