package org.opencb.opencga.storage.hadoop.variant.pending;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultithreadedTableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.search.VariantSearchSyncInfo;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.utils.MapReduceOutputFile;
import org.opencb.opencga.storage.hadoop.utils.ValueOnlyTextOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

/**
 * Created on 12/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DiscoverPendingVariantsDriver extends AbstractVariantsTableDriver {

    public static final String OVERWRITE = "overwrite";
    public static final String VARIANTS_COUNTER = "variants";
    public static final String PENDING_VARIANTS_COUNTER = "pending_variants";
    public static final String READY_VARIANTS_COUNTER = "ready_variants";
    private final Logger logger = LoggerFactory.getLogger(DiscoverPendingVariantsDriver.class);
    protected MapReduceOutputFile output;

    private PendingVariantsDescriptor<?> descriptor;

    @Override
    protected Class<? extends TableMapper<?, ?>> getMapperClass() {
        if (descriptor.getType() == PendingVariantsDescriptor.Type.FILE) {
            return DiscoverVariantsFileBasedMapper.class;
        } else {
            return DiscoverVariantsTableBasedMapper.class;
        }
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        this.descriptor = getDescriptor(getConf());
        if (descriptor.getType() == PendingVariantsDescriptor.Type.FILE) {
            output = initMapReduceOutputFile();
            if (output == null) {
                throw new IllegalArgumentException("Missing output file");
            }
        }
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        super.preExecution(variantTable);

        if (descriptor.getType() == PendingVariantsDescriptor.Type.TABLE) {
            PendingVariantsTableBasedDescriptor descriptor = (PendingVariantsTableBasedDescriptor) this.descriptor;
            HBaseManager hBaseManager = getHBaseManager();
            descriptor.createTableIfNeeded(descriptor.getTableName(getTableNameGenerator()), hBaseManager);
        }
    }

    private static PendingVariantsDescriptor<?> getDescriptor(Configuration conf) {
        try {
            return conf.getClass(PendingVariantsDescriptor.class.getName(), null, PendingVariantsDescriptor.class).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Missing valid PendingVariantsDescriptor", e);
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        Query query = VariantMapReduceUtil.getQueryFromConfig(getConf());

//        query.append(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
//        query.remove(VariantQueryParam.ANNOTATION_EXISTS.key());
//        VariantHBaseQueryParser parser = new VariantHBaseQueryParser(getHelper(), getMetadataManager());
//        Scan scan = parser.parseQuery(query,
//                new QueryOptions(QueryOptions.INCLUDE, VariantField.TYPE.fieldName()));

        Scan scan = new Scan();
        descriptor.configureScan(scan, getMetadataManager());
        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());
        logger.info("Scan variants table " + variantTable + " with scan " + scan.toString(50));

        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)) {
            Region region = new Region(query.getString(VariantQueryParam.REGION.key()));
            VariantHBaseQueryParser.addRegionFilter(scan, region);
        }

        boolean multiThread = getConf().getBoolean("annotation.pending.discover.MultithreadedTableMapper", false);
        final Class<? extends TableMapper> mapperClass;
        if (multiThread) {
            mapperClass = MultithreadedTableMapper.class;
            MultithreadedTableMapper.setMapperClass(job, (Class) getMapperClass());
//            MultithreadedTableMapper.setNumberOfThreads(job, 10); // default is 10
        } else {
            mapperClass = getMapperClass();
        }

        if (descriptor.getType() == PendingVariantsDescriptor.Type.TABLE) {
            PendingVariantsTableBasedDescriptor descriptor = (PendingVariantsTableBasedDescriptor) this.descriptor;
            VariantMapReduceUtil.initTableMapperJob(job, variantTable, descriptor.getTableName(getTableNameGenerator()), scan, mapperClass);
        } else {
            PendingVariantsFileBasedDescriptor descriptor = (PendingVariantsFileBasedDescriptor) this.descriptor;
            VariantMapReduceUtil.initTableMapperJob(job, variantTable, scan, mapperClass);
            try {
                Class<? extends InputFormat<?, ?>> delegatedInputFormatClass = job.getInputFormatClass();
                job.setInputFormatClass(VariantAlignedInputFormat.class);
                VariantAlignedInputFormat.setDelegatedInputFormat(job, delegatedInputFormatClass);
                VariantAlignedInputFormat.setBatchSize(job, descriptor.getFileBatchSize());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }



//        MultipleOutputs.addNamedOutput(job, "pending", ValueOnlyTextOutputFormat.class, VariantLocusKey.class, Text.class);
            LazyOutputFormat.setOutputFormatClass(job, ValueOnlyTextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, output.getOutdir());
            job.setOutputFormatClass(LazyOutputFormat.class);
            job.setOutputKeyClass(VariantLocusKey.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

            // Set DFS replication factor to 1. Replication factor is not really needed for these temporary files
            // Before using these files, need to check their integrity
            job.getConfiguration().set(DFSConfigKeys.DFS_REPLICATION_KEY, "1");
        }

        VariantMapReduceUtil.setNoneReduce(job);


        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "discover_" + descriptor.name() + "_pending_variants";
    }


    public static class DiscoverVariantsTableBasedMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        private int variants;
        private int readyVariants;
        private int pendingVariants;
        private PendingVariantsTableBasedDescriptor descriptor;
        private Function<Result, Mutation> pendingEvaluator;
        private VariantStorageMetadataManager metadataManager;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            descriptor = (PendingVariantsTableBasedDescriptor) getDescriptor(context.getConfiguration());
            descriptor.checkValidPendingTableName(context.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE));
            variants = 0;
            readyVariants = 0;
            pendingVariants = 0;
            boolean overwrite = context.getConfiguration().getBoolean(OVERWRITE, false);
            metadataManager = new VariantStorageMetadataManager(
                    new HBaseVariantStorageMetadataDBAdaptorFactory(
                            new VariantTableHelper(context.getConfiguration())));
            pendingEvaluator = descriptor.getPendingEvaluatorMapper(
                    metadataManager, overwrite);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Mutation mutation = pendingEvaluator.apply(value);

            variants++;
            if (mutation == null) {
                readyVariants++;
//                context.write(key, mutation);
            } else if (mutation instanceof Delete) {
                readyVariants++;
                context.write(key, mutation);
            } else {
                pendingVariants++;
                context.write(key, mutation);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            Counter counter = context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, VARIANTS_COUNTER);
            synchronized (counter) {
                counter.increment(variants);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, READY_VARIANTS_COUNTER).increment(readyVariants);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, PENDING_VARIANTS_COUNTER).increment(pendingVariants);
            }
            metadataManager.close();
        }
    }

    public static class DiscoverVariantsFileBasedMapper extends TableMapper<VariantLocusKey, Text> {

        private PendingVariantsFileBasedDescriptor descriptor;
        private Function<Result, Variant> pendingEvaluator;
        private VariantStorageMetadataManager metadataManager;
        private MultipleOutputs<VariantLocusKey, Text> mos;
        private ObjectMapper objectMapper;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs<>(context);
            descriptor = (PendingVariantsFileBasedDescriptor) getDescriptor(context.getConfiguration());

            boolean overwrite = context.getConfiguration().getBoolean(OVERWRITE, false);
            metadataManager = new VariantStorageMetadataManager(
                    new HBaseVariantStorageMetadataDBAdaptorFactory(
                            new VariantTableHelper(context.getConfiguration())));
            pendingEvaluator = descriptor.getPendingEvaluatorMapper(
                    metadataManager, overwrite);

            objectMapper = new ObjectMapper(new JsonFactory());
            objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
            JacksonUtils.addVariantMixIn(objectMapper);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, VARIANTS_COUNTER).increment(0);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, PENDING_VARIANTS_COUNTER).increment(0);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, READY_VARIANTS_COUNTER).increment(0);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Variant variant = pendingEvaluator.apply(value);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, VARIANTS_COUNTER).increment(1);
            if (variant == null) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, READY_VARIANTS_COUNTER).increment(1);
            } else {
                VariantSearchSyncInfo.Status syncStatus = VariantSearchSyncInfo.Status.from(variant.getAnnotation()
                        .getAdditionalAttributes().get(GROUP_NAME.key())
                        .getAttribute().get(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key()));
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, syncStatus.name()).increment(1);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, PENDING_VARIANTS_COUNTER).increment(1);
                mos.write(new VariantLocusKey(variant),
                        new Text(objectMapper.writeValueAsBytes(variant)),
                        descriptor.buildFileName(variant));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
            metadataManager.close();
        }
    }

    public static String[] buildArgs(String table, Class<? extends PendingVariantsDescriptor> descriptor, ObjectMap options) {
        options.put(PendingVariantsDescriptor.class.getName(), descriptor.getName());
        return buildArgs(table, options);
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = ToolRunner.run(new DiscoverPendingVariantsDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

}
