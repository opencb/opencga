package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.Log;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantFileOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantLocusKey;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantLocusKeyPartitioner;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.logging.Handler;
import java.util.logging.Level;

public class VariantExporterDriver extends VariantDriver {

    public static final String OUTPUT_FORMAT_PARAM = "of";
    private VariantWriterFactory.VariantOutputFormat outputFormat;
    private Class<? extends VariantMapper> mapperClass;
    private Class<? extends Reducer> reducerClass;
    private Class<? extends OutputFormat> outputFormatClass;
    private Class<? extends Partitioner> partitioner;

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        outputFormat = VariantWriterFactory.VariantOutputFormat.valueOf(getParam(OUTPUT_FORMAT_PARAM, "avro").toUpperCase());
    }

    @Override
    protected Class<? extends VariantMapper> getMapperClass() {
        return mapperClass;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    @Override
    protected Class<? extends Partitioner> getPartitioner() {
        return partitioner;
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    @Override
    protected void setupJob(Job job) throws IOException {
        job.getConfiguration().setBoolean(JobContext.MAP_OUTPUT_COMPRESS, true);
        job.getConfiguration().setClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC, DeflateCodec.class, CompressionCodec.class);
        switch (outputFormat) {
            case AVRO_GZ:
                FileOutputFormat.setCompressOutput(job, true);
                job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC);
                // do not break
            case AVRO:
                outputFormatClass = AvroKeyOutputFormat.class;
                if (useReduceStep) {
                    job.setMapOutputKeyClass(VariantLocusKey.class);
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
                outputFormatClass = AvroParquetOutputFormat.class;
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
                if (outputFormat.isBinary()) {
                    throw new IllegalArgumentException("Unexpected binary output format " + outputFormat);
                }
                if (useReduceStep) {
                    job.setMapOutputKeyClass(VariantLocusKey.class);
                    AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());
                    mapperClass = AvroVariantExporterMapper.class;
                    reducerClass = VariantExporterReducer.class;
                    partitioner = VariantLocusKeyPartitioner.class;
                    outputFormatClass = VariantFileOutputFormat.class;
                } else {
                    AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema());
                    mapperClass = VariantExporterDirectMultipleOutputsMapper.class;
//                    mapperClass = VariantExporterDirectMapper.class;

                    reducerClass = null;

//                    MultipleOutputs.setCountersEnabled(job, true);
                    MultipleOutputs.addNamedOutput(job, VariantExporterDirectMultipleOutputsMapper.NAMED_OUTPUT,
                            VariantFileOutputFormat.class, Variant.class, NullWritable.class);
                    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
                    outputFormatClass = LazyOutputFormat.class;
                }
                if (outputFormat.isGzip()) {
                    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
                } else if (outputFormat.isSnappy()) {
                    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); // compression
                }
                job.getConfiguration().set(VariantFileOutputFormat.VARIANT_OUTPUT_FORMAT, outputFormat.name());
                job.setOutputKeyClass(Variant.class);
                break;
        }
    }


    @Override
    protected String getJobOperationName() {
        return "export_" + outputFormat;
    }

    /**
     * Mapper to convert to Variant.
     * The output of this mapper should be connected directly to the {@link VariantWriterFactory.VariantOutputFormat}
     * This mapper can not work with a reduce step.
     * @see VariantWriterFactory.VariantOutputFormat
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
     * Mapper to convert to Variant.
     * The output of this mapper should be connected directly to the {@link VariantWriterFactory.VariantOutputFormat}
     * This mapper can not work with a reduce step.
     * The output is written to multiple outputs, ensuring that generated files are sorted by chromosome and position.
     */
    public static class VariantExporterDirectMultipleOutputsMapper extends VariantMapper<Variant, NullWritable> {

        public static final String NAMED_OUTPUT = "export";
        private String baseOutputPath;
        private String chromosome;

        public static String buildOutputKeyPrefix(String chromosome, Integer start) {
            // If it's a single digit chromosome, add a 0 at the beginning
            //       1 -> 01
            //       3 -> 03
            //      22 -> 22
            // If the first character is a digit, and the second is not, add a 0 at the beginning
            //      MT -> MT
            //      1_KI270712v1_random -> 01_KI270712v1_random
            if (VariantLocusKey.isSingleDigitChromosome(chromosome)) {
                chromosome = "0" + chromosome;
            }

            return String.format("%s.%s.%010d.", NAMED_OUTPUT, chromosome, start);
        }

        private MultipleOutputs<Variant, NullWritable> mos;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs<>(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            if (baseOutputPath == null || !consecutiveChromosomes(chromosome, value.getChromosome())) {
                baseOutputPath = buildOutputKeyPrefix(value.getChromosome(), value.getStart());
                chromosome = value.getChromosome();
            }
            mos.write(NAMED_OUTPUT, value, NullWritable.get(), baseOutputPath);
        }

        private static boolean consecutiveChromosomes(String prevChromosome, String newChromosome) {
            if (newChromosome.equals(prevChromosome)) {
                return true;
            }
            if (VariantLocusKey.isSingleDigitChromosome(prevChromosome)) {
                return VariantLocusKey.isSingleDigitChromosome(newChromosome);
            } else {
                return !VariantLocusKey.isSingleDigitChromosome(newChromosome);
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Variant, Variant, NullWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
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
    public static class AvroVariantExporterMapper extends VariantMapper<VariantLocusKey, AvroValue<VariantAvro>> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(0);
        }

        @Override
        protected void map(Object key, Variant value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            removeNullsFromAvro(value.getImpl(), context);
            context.write(new VariantLocusKey(value), new AvroValue<>(value.getImpl()));
        }
    }

    /**
     * Reducer to join all VariantAvro and generate Variants.
     * The output of this reducer should be connected to the {@link VariantWriterFactory.VariantOutputFormat}
     * @see AvroVariantExporterMapper
     * @see VariantWriterFactory.VariantOutputFormat
     */
    public static class VariantExporterReducer<T> extends Reducer<T, AvroValue<VariantAvro>, Variant, NullWritable> {
        @Override
        protected void reduce(T key, Iterable<AvroValue<VariantAvro>> values, Context context)
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
    public static class AvroKeyVariantExporterReducer<T>
            extends Reducer<T, AvroValue<VariantAvro>, AvroKey<VariantAvro>, NullWritable> {
        @Override
        protected void reduce(T key, Iterable<AvroValue<VariantAvro>> values, Context context)
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
