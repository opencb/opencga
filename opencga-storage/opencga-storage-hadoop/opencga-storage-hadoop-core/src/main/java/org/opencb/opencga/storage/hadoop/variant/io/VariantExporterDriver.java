package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.Log;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantFileOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Handler;
import java.util.logging.Level;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

/**
 * Created on 14/06/18.
 *
 * export HADOOP_USER_CLASSPATH_FIRST=true
 * hbase_conf=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
 * export HADOOP_CLASSPATH=${hbase_conf}:$PWD/libs/avro-1.7.7.jar:$PWD/libs/jackson-databind-2.6.6.jar:$PWD/libs/jackson-core-2.6.6.jar
 * export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$PWD/libs/jackson-annotations-2.6.6.jar
 * yarn jar opencga-storage-hadoop-core-1.4.0-jar-with-dependencies.jar \
 *      org.opencb.opencga.storage.hadoop.variant.io.VariantExporterDriver \
 *      opencga_variants study myStudy --of avro --output my.variants.avro --region 22
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExporterDriver extends AbstractVariantsTableDriver {

    public static final String OUTPUT_FORMAT_PARAM = "of";
    public static final String OUTPUT_PARAM = "output";
    public static final String CONCAT_OUTPUT_PARAM = "concat-output";
    private VariantOutputFormat outputFormat;
    private Path outdir;
    private Path localOutput;
    private Query query = new Query();
    private QueryOptions options = new QueryOptions();
    private final static Logger logger = LoggerFactory.getLogger(VariantExporterDriver.class);
    private boolean useReduceStep;

    @Override
    protected void parseAndValidateParameters() throws IOException {
        setStudyId(-1);
        super.parseAndValidateParameters();
        outputFormat = VariantOutputFormat.valueOf(getParam(OUTPUT_FORMAT_PARAM, "avro").toUpperCase());
        String outdirStr = getParam(OUTPUT_PARAM);
        if (StringUtils.isEmpty(outdirStr)) {
            throw new IllegalArgumentException("Missing argument " + OUTPUT_PARAM);
        }

        useReduceStep = Boolean.valueOf(getParam(CONCAT_OUTPUT_PARAM));
        outdir = new Path(outdirStr);
        if (isLocal(outdir)) {
            localOutput = getLocalOutput(outdir);
            outdir = getTempOutdir("opencga_export", localOutput.getName());
            outdir.getFileSystem(getConf()).deleteOnExit(outdir);
        }
        if (localOutput != null) {
            useReduceStep = true;
            logger.info(" * Outdir file: " + localOutput.toUri());
            logger.info(" * Temporary outdir file: " + outdir.toUri());
        } else {
            logger.info(" * Outdir file: " + outdir.toUri());
        }

        getQueryFromConfig(query, getConf());
        getQueryOptionsFromConfig(options, getConf());

        logger.info(" * Query:");
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            logger.info("   * " + entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        Class<? extends VariantMapper> mapperClass;
        Class<? extends Reducer> reducerClass;

        switch (outputFormat) {
            case AVRO_GZ:
                FileOutputFormat.setCompressOutput(job, true);
                job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC);
                // do not break
            case AVRO:
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                if (useReduceStep) {
                    job.setMapOutputKeyClass(NullWritable.class);
                    AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
                    AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
                    job.setOutputValueClass(NullWritable.class);

                    mapperClass = AvroVariantExporterMapper.class;
                    reducerClass = AvroKeyVariantExporterReducer.class;
                } else {
                    AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
                    job.setMapOutputValueClass(NullWritable.class);
                    mapperClass = AvroVariantExporterDirectMapper.class;
                    reducerClass = null;
                }
                break;

            case PARQUET_GZ:
                ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
                // do not break
            case PARQUET:
                job.setOutputFormatClass(AvroParquetOutputFormat.class);
                AvroParquetOutputFormat.setSchema(job, VariantAvro.getClassSchema());
                if (useReduceStep) {
                    job.setMapOutputKeyClass(NullWritable.class);
                    AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
                    mapperClass = ParquetVariantExporterMapper.class;
                    reducerClass = ParquetVariantExporterReducer.class;
                } else {
                    mapperClass = ParquetVariantExporterDirectMapper.class;
                    reducerClass = null;
                }
                break;
            default:
                if (useReduceStep) {
                    job.setMapOutputKeyClass(NullWritable.class);
                    AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
                    mapperClass = AvroVariantExporterMapper.class;
                    reducerClass = VariantExporterReducer.class;
                } else {
                    AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
                    mapperClass = VariantExporterDirectMapper.class;
                    reducerClass = null;
                }
                if (outputFormat.isGzip()) {
                    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
                } else if (outputFormat.isSnappy()) {
                    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); // compression
                }
                job.setOutputFormatClass(VariantFileOutputFormat.class);
                job.setOutputKeyClass(Variant.class);
                break;
        }

        if (useReduceStep) {
            logger.info("Use one Reduce task to produce a single file");
            job.setReducerClass(reducerClass);
            job.setNumReduceTasks(1);
        } else {
            VariantMapReduceUtil.setNoneReduce(job);
        }

        VariantQueryParser variantQueryParser = new VariantQueryParser(null, getMetadataManager());
        ParsedVariantQuery variantQuery = variantQueryParser.parseQuery(query, options);
        Query query = variantQuery.getQuery();
        if (VariantHBaseQueryParser.isSupportedQuery(query)) {
            logger.info("Init MapReduce job reading from HBase");
            boolean useSampleIndex = !getConf().getBoolean("skipSampleIndex", false) && SampleIndexQueryParser.validSampleIndexQuery(query);
            if (useSampleIndex) {
                // Remove extra fields from the query
                new SampleIndexDBAdaptor(getHBaseManager(), getTableNameGenerator(), getMetadataManager()).parseSampleIndexQuery(query);

                logger.info("Use sample index to read from HBase");
            }

            VariantHBaseQueryParser parser = new VariantHBaseQueryParser(getMetadataManager());
            List<Scan> scans = parser.parseQueryMultiRegion(variantQuery, options);
            VariantMapReduceUtil.configureMapReduceScans(scans, getConf());

            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTable, scans, mapperClass, useSampleIndex);
        } else {
            logger.info("Init MapReduce job reading from Phoenix");
            String sql = new VariantSqlQueryParser(variantTable, getMetadataManager(), getHelper().getConf())
                    .parse(variantQuery, options);

            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTable, sql, mapperClass);
        }

        setNoneTimestamp(job);

        FileOutputFormat.setOutputPath(job, outdir); // set Path

        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."));

        job.getConfiguration().set(VariantFileOutputFormat.VARIANT_OUTPUT_FORMAT, outputFormat.name());

        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            if (localOutput != null) {
                concatMrOutputToLocal(outdir, localOutput);
            }
        }
        if (localOutput != null) {
            deleteTemporaryFile(outdir);
        }
    }

    @Override
    protected String getJobOperationName() {
        return "export_" + outputFormat;
    }

    /**
     * Mapper to convert to Variant.
     * The output of this mapper should be connected directly to the {@link VariantOutputFormat}
     * This mapper can not work with a reduce step.
     * @see VariantOutputFormat
     */
    public static class VariantExporterDirectMapper extends VariantMapper<Variant, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            context.write(value, NullWritable.get());
        }
    }

    /**
     * Mapper to convert to VariantAvro.
     * The output of this mapper should be connected directly to the {@link AvroKeyOutputFormat}
     * This mapper can not work with a reduce step.
     * @see AvroKeyOutputFormat
     */
    public static class AvroVariantExporterDirectMapper extends VariantMapper<AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            context.write(new AvroKey<>(value.getImpl()), NullWritable.get());
        }
    }

    /**
     * Mapper to convert to VariantAvro.
     * This mapper should be connected to either {@link AvroKeyVariantExporterReducer} or {@link VariantExporterReducer}
     * @see VariantExporterReducer
     * @see AvroKeyVariantExporterReducer
     */
    public static class AvroVariantExporterMapper extends VariantMapper<NullWritable, AvroValue<VariantAvro>> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            removeNullsFromAvro(value.getImpl(), context);
            context.write(NullWritable.get(), new AvroValue<>(value.getImpl()));
        }
    }

    /**
     * Reducer to join all VariantAvro and generate Variants.
     * The output of this reducer should be connected to the {@link VariantOutputFormat}
     * @see AvroVariantExporterMapper
     * @see VariantOutputFormat
     */
    public static class VariantExporterReducer extends Reducer<NullWritable, AvroValue<VariantAvro>, Variant, NullWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<AvroValue<VariantAvro>> values, Context context)
                throws IOException, InterruptedException {
            for (AvroValue<VariantAvro> value : values) {
                context.write(new Variant(value.datum()), NullWritable.get());
            }
        }
    }

    /**
     * Reducer to join all VariantAvro.
     * The output of this reducer should be connected to the {@link AvroKeyOutputFormat}
     * @see AvroVariantExporterMapper
     * @see AvroKeyOutputFormat
     */
    public static class AvroKeyVariantExporterReducer
            extends Reducer<NullWritable, AvroValue<VariantAvro>, AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<AvroValue<VariantAvro>> values, Context context)
                throws IOException, InterruptedException {
            for (AvroValue<VariantAvro> value : values) {
                context.write(new AvroKey<>(value.datum()), NullWritable.get());
            }
        }
    }

    /**
     * Mapper to convert to Variant.
     * The output of this mapper should be connected directly to the {@link AvroParquetOutputFormat}
     * This mapper can not work with a reduce step. Void (null) key produces NPE.
     * @see AvroParquetOutputFormat
     */
    public static class ParquetVariantExporterDirectMapper extends VariantMapper<Void, VariantAvro> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
            silenceParquet();
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            removeNullsFromAvro(value.getImpl(), context);
            context.write(null, value.getImpl());
        }
    }

    /**
     * Mapper to convert to VariantAvro.
     * This mapper should be connected to {@link ParquetVariantExporterReducer}
     * This mapper can not be connected directly to the {@link AvroParquetOutputFormat}
     * @see ParquetVariantExporterReducer
     */
    public static class ParquetVariantExporterMapper extends VariantMapper<NullWritable, AvroValue<VariantAvro>> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
            silenceParquet();
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            removeNullsFromAvro(value.getImpl(), context);
            context.write(NullWritable.get(), new AvroValue<>(value.getImpl()));
        }
    }

    /**
     * Reducer to join all VariantAvro.
     * The output of this reducer should be connected directly to the {@link AvroParquetOutputFormat}
     * @see ParquetVariantExporterMapper
     * @see AvroParquetOutputFormat
     */
    public static class ParquetVariantExporterReducer extends Reducer<NullWritable, AvroValue<VariantAvro>, Void, VariantAvro> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            silenceParquet();
        }

        @Override
        protected void reduce(NullWritable key, Iterable<AvroValue<VariantAvro>> values, Context context)
                throws IOException, InterruptedException {
            for (AvroValue<VariantAvro> value : values) {
                context.write(null, value.datum());
            }
        }
    }

    private static void silenceParquet() {
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


    private static VariantAvro removeNullsFromAvro(VariantAvro avro, TaskAttemptContext context) {
        if (avro.getAnnotation() != null) {
            List<GeneCancerAssociation> geneCancerAssociations = avro.getAnnotation().getGeneCancerAssociations();
            if (geneCancerAssociations != null) {
                for (GeneCancerAssociation geneCancerAssociation : geneCancerAssociations) {
                    if (geneCancerAssociation.getMutationTypes() != null) {
                        if (geneCancerAssociation.getMutationTypes().removeIf(Objects::isNull)) {
                            context.getCounter(COUNTER_GROUP_NAME, "annotation.gca.mutationType_null").increment(1);
                        }
                    }
                    if (geneCancerAssociation.getTissues() != null) {
                        if (geneCancerAssociation.getTissues().removeIf(Objects::isNull)) {
                            context.getCounter(COUNTER_GROUP_NAME, "annotation.gca.tissues_null").increment(1);
                        }
                    }
                }
            }
        }
        return avro;
    }


    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }
}
