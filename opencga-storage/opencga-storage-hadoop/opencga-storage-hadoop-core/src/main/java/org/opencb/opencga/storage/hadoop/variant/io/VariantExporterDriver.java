package org.opencb.opencga.storage.hadoop.variant.io;

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
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME;

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
        PARQUET
        // VCF
        // VCF_GZ
    }

    @Override
    protected void parseAndValidateParameters() {
        outputFormat = OutputFormat.valueOf(getConf().get("--of", "avro").toUpperCase());
        outFile = getConf().get("--output");
        if (outFile == null || outFile.isEmpty()) {
            throw new IllegalArgumentException(outFile);
        }
        for (VariantQueryParam param : VariantQueryParam.values()) {
            String value = getConf().get(param.key(), getConf().get("--" + param.key()));
            if (value != null && !value.isEmpty()) {
                query.put(param.key(), value);
            }
        }
        options.put(QueryOptions.INCLUDE, getConf().get(QueryOptions.INCLUDE));
        options.put(QueryOptions.EXCLUDE, getConf().get(QueryOptions.EXCLUDE));
    }

    @Override
    protected Class<? extends VariantMapper> getMapperClass() {
        switch (outputFormat) {
            case PARQUET:
                return ParquetVariantExporterMapper.class;
            case AVRO:
            case AVRO_GZ:
                return AvroVariantExporterMapper.class;
            default:
                throw new IllegalArgumentException(outputFormat.toString());
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new VariantHBaseQueryParser(getHelper(), getStudyConfigurationManager()).parseQuery(query, options);

        scan.setCaching(1);

        VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTable, scan, getMapperClass());
        VariantMapReduceUtil.setNoneReduce(job);

        FileOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression

        switch (outputFormat) {
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
