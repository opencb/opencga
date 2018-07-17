package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Log;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantFileOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper.COUNTER_GROUP_NAME;
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
 *      org.opencb.opencga.storage.hadoop.variant.io.VariantExporterDriver \
 *      opencga_variants study myStudy --of avro --output my.variants.avro --region 22
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExporterDriver extends AbstractVariantsTableDriver {

    public static final String OUTPUT_FORMAT_PARAM = "--of";
    public static final String OUTPUT_PARAM = "--output";
    private VariantOutputFormat outputFormat;
    private String outFile;
    private Query query = new Query();
    private QueryOptions options = new QueryOptions();

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        outputFormat = VariantOutputFormat.valueOf(getConf().get(OUTPUT_FORMAT_PARAM, "avro").toUpperCase());
        outFile = getConf().get(OUTPUT_PARAM);
        if (outFile == null || outFile.isEmpty()) {
            throw new IllegalArgumentException(outFile);
        }
        getQueryFromConfig(query, getConf());
        getQueryOptionsFromConfig(options, getConf());
    }

    @Override
    protected Class<? extends VariantMapper> getMapperClass() {
        switch (outputFormat) {
            case PARQUET_GZ:
            case PARQUET:
                return ParquetVariantExporterMapper.class;
            case AVRO:
            case AVRO_GZ:
                return AvroVariantExporterMapper.class;
            default:
                return VariantExporterMapper.class;
        }
    }
    private final Logger logger = LoggerFactory.getLogger(VariantExporterDriver.class);

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        if (VariantHBaseQueryParser.isSupportedQuery(query)) {
            logger.info("Init MapReduce job reading from HBase");
            int caching;
            boolean useSampleIndex = !getConf().getBoolean("skipSampleIndex", false) && SampleIndexQuery.validSampleIndexQuery(query);
            if (useSampleIndex) {
                // Remove extra fields from the query
                SampleIndexQuery.extractSampleIndexQuery(query, getStudyConfigurationManager());

                logger.info("Use sample index to read from HBase");
            }
            caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);

            VariantHBaseQueryParser parser = new VariantHBaseQueryParser(getHelper(), getStudyConfigurationManager());
            List<Scan> scans = parser.parseQueryMultiRegion(query, options);
            for (Scan scan : scans) {
                scan.setCaching(caching);
                scan.setCacheBlocks(false);
            }
            logger.info("Set scan caching to " + caching);

            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTable, scans, getMapperClass(), useSampleIndex);
        } else {
            logger.info("Init MapReduce job reading from Phoenix");
            String sql = new VariantSqlQueryParser(getHelper(), variantTable, getStudyConfigurationManager())
                    .parse(query, options).getSql();

            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTable, sql, getMapperClass());
        }

        VariantMapReduceUtil.setNoneReduce(job);

        FileOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path

        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."));

        switch (outputFormat) {
            case AVRO_GZ:
                FileOutputFormat.setCompressOutput(job, true);
                job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC);
                // do not break
            case AVRO:
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
                break;

            case PARQUET_GZ:
                ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
                // do not break
            case PARQUET:
                job.setOutputFormatClass(AvroParquetOutputFormat.class);
                AvroParquetOutputFormat.setSchema(job, VariantAvro.getClassSchema());
                break;
            default:
                if (outputFormat.isGzip()) {
                    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
                } else if (outputFormat.isSnappy()) {
                    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); // compression
                }
                job.setOutputFormatClass(VariantFileOutputFormat.class);
                job.setOutputKeyClass(Variant.class);
                break;
        }

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "export_" + outputFormat;
    }

    public static class VariantExporterMapper extends VariantMapper<Variant, NullWritable> {
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

    public static class ParquetVariantExporterMapper extends VariantMapper<Void, VariantAvro> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            silenceParquet();
        }

        public static void silenceParquet() {
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Log.class.getPackage().getName());
            logger.setLevel(Level.WARNING);
            Handler[] handlers = logger.getHandlers();
            if (handlers != null) {
                for (Handler handler : handlers) {
                    handler.setLevel(Level.WARNING);
//                    if (handler instanceof StreamHandler) {
//                        logger.removeHandler(handler);
//                    }
                }
            }
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            context.write(null, value.getImpl());
        }
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = ToolRunner.run(new VariantExporterDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }
}
