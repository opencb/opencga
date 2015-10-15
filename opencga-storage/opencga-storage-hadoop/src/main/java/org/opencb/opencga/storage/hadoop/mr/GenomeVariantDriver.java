package org.opencb.opencga.storage.hadoop.mr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.opencb.biodata.tools.variant.converter.VariantFileMetadataToVcfMeta;
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;
import com.sun.tools.internal.jxc.gen.config.Config;

public class GenomeVariantDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(GenomeVariantDriver.class);

    public static final String HBASE_MASTER = "hbase.master";
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String OPT_TABLE_NAME = "opencga.table.name";
    public static final String OPT_VCF_FILE = "opencga.file.vcf";
    public static final String OPT_VCF_META_FILE = "opencga.file.vcfmeta";

    public GenomeVariantDriver () {
    }

    public GenomeVariantDriver (Configuration conf) {
        super(conf);
    }

    public int run (String[] args) throws Exception {
        Configuration conf = getConf();
        URI inputFile = URI.create(conf.get(OPT_VCF_FILE));
        URI inputMetaFile = URI.create(conf.get(OPT_VCF_META_FILE));
        String tablename = conf.get(OPT_TABLE_NAME);

/*  SERVER details  */
        
        // add metadata config as string
        addMetaData(conf,inputMetaFile); // TODO store in HBase
        GenomeVariantHelper variantHelper = new GenomeVariantHelper(conf);
        VcfMeta meta = variantHelper.getMeta(); // testing

        /* JOB setup */
        Job job = Job.getInstance(conf, "Genome Variant to HBase");
        job.setJarByClass(getClass());
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

        // input
        FileInputFormat.addInputPath(job, new Path(inputFile));

        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // mapper
        job.setMapperClass(GenomeVariantConverter.class);

        // combiner
        job.setCombinerClass(GenomeVariantCombiner.class);

        // output -> using utils
        TableMapReduceUtil.initTableReducerJob(tablename, null, job);

        // If utils is not used
        // conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
        // job.setOutputKeyClass(ImmutableBytesWritable.class);
        // job.setOutputValueClass(Writable.class);
        // TableMapReduceUtil.addDependencyJars(job);

        job.setNumReduceTasks(0);

        if( HBaseUtils.createTableIfNeeded(tablename, variantHelper.getColumnFamily(), job.getConfiguration())){
            LOG.info(String.format("Create table '%s' in hbase!", tablename));
        }
        storeMetaData(variantHelper, tablename, job.getConfiguration());
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void storeMetaData(GenomeVariantHelper variantHelper, String tablename, Configuration conf) throws IOException {
        Put put = variantHelper.getMetaAsPut();
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(conf);
                Table table = con.getTable(tname);){
            table.put(put);
        }
    }

    private void addMetaData(Configuration conf, URI inputMetaFile) throws IOException {
        Class<GeneratedMessage> clazz = com.google.protobuf.GeneratedMessage.class;
        LOG.debug(clazz.getProtectionDomain().getCodeSource().getLocation().toString());
        URL url = clazz.getResource('/'+clazz.getName().replace('.', '/')+".class");
        LOG.debug(url.toString());
        VcfMeta meta = getMetaData(conf, inputMetaFile);
        String protocFile = inputMetaFile+".protoc3";
        Path to = new Path(protocFile);
        FileSystem fs = FileSystem.get(conf);
        try(FSDataOutputStream os = fs.create(to,true);){
            os.write(meta.toByteArray());
        }
        GenomeVariantHelper.setMetaProtoFile(conf, URI.create(protocFile));
//        GenomeVariantHelper.setMetaProtoString(conf, meta.getStudyIdBytes().toStringUtf8());
    }

    private VcfMeta getMetaData (Configuration conf, URI inputMetaFile) throws IOException {
        String ret = StringUtils.EMPTY;
        Path from = new Path(inputMetaFile);
        FileSystem fs = FileSystem.get(conf);
        VariantFileMetadataToVcfMeta conv = new VariantFileMetadataToVcfMeta();
        DatumReader<VariantFileMetadata> userDatumReader = new SpecificDatumReader<VariantFileMetadata>(VariantFileMetadata.class);
        try (FSDataInputStream ids = fs.open(from);
                DataFileStream<VariantFileMetadata> dataFileReader = new DataFileStream<VariantFileMetadata>(ids, userDatumReader);) {
            Iterator<VariantFileMetadata> iter = dataFileReader.iterator();
            if(!iter.hasNext()){
                throw new IllegalStateException(String.format("No Meta data object found in %s !!!",inputMetaFile));
            }
            VcfMeta meta = conv.convert(iter.next());
            if(iter.hasNext())
                LOG.warn(String.format("More than 1 entry found in metadata file %s", inputMetaFile));
            return meta;
        }
    }
    
    private static void addServerAndTableSettings(Configuration conf, String target) throws URISyntaxException{
        URI uri = new URI(target);
        String server = uri.getHost();
        Integer port = uri.getPort() > 0?uri.getPort() : 60000;
        String tablename = uri.getPath();
        tablename = tablename.startsWith("/") ? tablename.substring(1) : tablename; // Remove leading /
        String master = String.join(":", server, port.toString());

        conf.set(HBASE_ZOOKEEPER_QUORUM, server);
        conf.set(HBASE_MASTER, master);
        conf.set(OPT_TABLE_NAME,tablename);
    }

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenomeVariantDriver driver = new GenomeVariantDriver();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        
        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();
        
        if (toolArgs.length != 3) {
            System.err.printf("Usage: %s [generic options] <avro> <avro-meta> <output-table>\n", 
                    GenomeVariantDriver.class.getSimpleName());
            System.err.println("Found " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        conf.set(OPT_VCF_FILE,args[0]);
        conf.set(OPT_VCF_META_FILE,args[1]);
        addServerAndTableSettings(conf,toolArgs[2]);

        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);
        
        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);
        
        System.exit(exitCode);
    }

}
