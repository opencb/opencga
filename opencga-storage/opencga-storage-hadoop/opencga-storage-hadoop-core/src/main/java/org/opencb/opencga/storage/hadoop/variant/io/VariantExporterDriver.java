package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VcfOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

/**
 * Created on 14/06/18.
 *
 * export HADOOP_USER_CLASSPATH_FIRST=true
 * hbase_conf=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
 * export HADOOP_CLASSPATH=${hbase_conf}:$PWD/libs/avro-1.7.7.jar:$PWD/libs/jackson-databind-2.6.6.jar:$PWD/libs/jackson-core-2.6.6.jar
 * export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$PWD/libs/jackson-annotations-2.6.6.jar
 * yarn jar opencga-storage-hadoop-core-1.4.0-rc-dev-jar-with-dependencies.jar \
 *      org.opencb.opencga.storage.hadoop.variant.io.VariantExporterDriver opencga_archive_2  \
 *      opencga_variants 2 . --of avro --output my.variants.avro --region 22
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExporterDriver extends AbstractAnalysisTableDriver {

    private OutputFormat outputFormat;
    private String outFile;
    private Query query = new Query();
    private QueryOptions options = new QueryOptions();

    enum OutputFormat {
        AVRO,
        AVRO_GZ,
        PARQUET,
        VCF,
        VCF_GZ
    }

    @Override
    protected void parseAndValidateParameters() {
        outputFormat = OutputFormat.valueOf(getConf().get("--of", "avro").toUpperCase());
        outFile = getConf().get("--output");
        if (outFile == null || outFile.isEmpty()) {
            throw new IllegalArgumentException(outFile);
        }
        getQueryFromConfig(query, getConf());
        getQueryOptionsFromConfig(options, getConf());
    }

    @Override
    protected Class<? extends VariantMapper> getMapperClass() {
        switch (outputFormat) {
            case VCF:
            case VCF_GZ:
                return VcfVariantExporterMapper.class;
            case PARQUET:
                return ParquetVariantExporterMapper.class;
            case AVRO:
            case AVRO_GZ:
                return AvroVariantExporterMapper.class;
            default:
                throw new IllegalArgumentException(outputFormat.toString());
        }
    }
    private final Logger logger = LoggerFactory.getLogger(VariantExporterDriver.class);

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        int caching;
        boolean useSampleIndex = !getConf().getBoolean("skipSampleIndex", false) && SampleIndexQuery.validSampleIndexQuery(query);
        if (useSampleIndex) {
            // Remove extra fields from the query
            SampleIndexQuery.extractSampleIndexQuery(query, getStudyConfigurationManager());

            logger.info("Use sample index to read from HBase");
            caching = 100;
        } else {
            caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);
        }

        Scan scan = new VariantHBaseQueryParser(getHelper(), getStudyConfigurationManager()).parseQuery(query, options);
        scan.setCaching(caching);

        logger.info("Set SCAN caching to " + caching);

        VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTable, scan, getMapperClass(), useSampleIndex);
        VariantMapReduceUtil.setNoneReduce(job);

        FileOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path

        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."));

        switch (outputFormat) {
            case VCF_GZ:
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
                // do not break
            case VCF:
                job.setOutputFormatClass(VcfOutputFormat.class);
                job.setOutputKeyClass(Variant.class);
                break;

            case AVRO_GZ:
                FileOutputFormat.setCompressOutput(job, true);
                job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC);
                // do not break
            case AVRO:
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
                break;

            case PARQUET:
                job.setOutputFormatClass(AvroParquetOutputFormat.class);
                AvroParquetOutputFormat.setSchema(job, VariantAvro.getClassSchema());
                break;
            default:
                throw new IllegalArgumentException("Unknown output format " + outputFormat);
        }

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "export_" + outputFormat;
    }

    public static class VcfVariantExporterMapper extends VariantMapper<Variant, NullWritable> {
        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            context.write(value, NullWritable.get());
        }
    }

    public static class AvroVariantExporterMapper extends VariantMapper<AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            context.write(new AvroKey<>(value.getImpl()), NullWritable.get());
        }
    }

    public static class ParquetVariantExporterMapper extends VariantMapper<AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            context.write(new AvroKey<>(value.getImpl()), NullWritable.get());
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new VariantExporterDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
