package org.opencb.opencga.storage.hadoop.mr;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.avro.Variant;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.models.variant.converter.VariantFileMetadataToVcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenomeVariantDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(GenomeVariantDriver.class);

    public GenomeVariantDriver () {
    }

    public GenomeVariantDriver (Configuration conf) {
        super(conf);
    }

    @Override
    public int run (String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <avro> <avro-meta> <output-table>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String inputfile = args[0]; 
        String inputMetaFile = args[1];
        String tablename = args[2];

        Configuration conf = new Configuration();

        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* JOB setup */
        Job job = Job.getInstance(conf, "Genome Variant to HBase");
        job.setJarByClass(getClass());

        // add metadata config as string
        addMetaData(conf,inputMetaFile); // TODO store in HBase

        // input
        FileInputFormat.addInputPath(job, new Path(inputfile));

        AvroJob.setInputKeySchema(job, Variant.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // mapper
        job.setMapperClass(GenomeVariantConverter.class);

        // combiner
         job.setCombinerClass(GenomeVariantCombiner.class);

        // output -> using utils
         TableMapReduceUtil.initTableReducerJob(tablename, null, job);

        // If utils is not used
//         conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
//         job.setOutputKeyClass(ImmutableBytesWritable.class);
//         job.setOutputValueClass(Writable.class);
//         TableMapReduceUtil.addDependencyJars(job);

         job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private void addMetaData(Configuration conf, String inputMetaFile) throws IOException {
        GenomeVariantHelper.setMetaProtoString(conf, getMetaData(conf, inputMetaFile));
    }

    private String getMetaData (Configuration conf, String inputMetaFile) throws IOException {
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
            ret = meta.toByteString().toStringUtf8();
            if(iter.hasNext())
                LOG.warn(String.format("More than 1 entry found in metadata file %s", inputMetaFile));
            return ret;
        }
    }

    public static void main (String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GenomeVariantDriver(), args);
        System.exit(exitCode);
    }

}
