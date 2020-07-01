package org.opencb.opencga.storage.hadoop.variant.pending;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultithreadedTableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

/**
 * Created on 12/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DiscoverPendingVariantsDriver extends AbstractVariantsTableDriver {

    private final Logger logger = LoggerFactory.getLogger(DiscoverPendingVariantsDriver.class);

    private PendingVariantsDescriptor descriptor;

    @Override
    protected Class<DiscoverVariantsMapper> getMapperClass() {
        return DiscoverVariantsMapper.class;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        this.descriptor = getDescriptor(getConf());
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        super.preExecution(variantTable);


        HBaseManager hBaseManager = getHBaseManager();
        descriptor.createTableIfNeeded(descriptor.getTableName(getTableNameGenerator()), hBaseManager);
    }

    private static PendingVariantsDescriptor getDescriptor(Configuration conf) {
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
        logger.info("Scan variants table " + variantTable + " with scan " + scan.toString(50));

        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)) {
            Region region = new Region(query.getString(VariantQueryParam.REGION.key()));
            VariantHBaseQueryParser.addRegionFilter(scan, region);
        }

        int caching = getConf().getInt(HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.key(), 50);
        boolean multiThread = getConf().getBoolean("annotation.pending.discover.MultithreadedTableMapper", false);


        scan.setCaching(caching);
        scan.setCacheBlocks(false);
        logger.info("Set scan caching to " + caching);

        final Class<? extends TableMapper> mapperClass;
        if (multiThread) {
            logger.info("Run with MultithreadedTableMapper");
            mapperClass = MultithreadedTableMapper.class;
            MultithreadedTableMapper.setMapperClass(job, getMapperClass());
//            MultithreadedTableMapper.setNumberOfThreads(job, 10); // default is 10
        } else {
            mapperClass = getMapperClass();
        }
        VariantMapReduceUtil.initTableMapperJob(job, variantTable, descriptor.getTableName(getTableNameGenerator()), scan, mapperClass);


        VariantMapReduceUtil.setNoneReduce(job);


        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "discover_" + descriptor.name() + "_pending_variants";
    }


    public static class DiscoverVariantsMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        private int variants;
        private int readyVariants;
        private int pendingVariants;
        private PendingVariantsDescriptor descriptor;
        private Function<Result, Mutation> pendingEvaluator;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            descriptor = getDescriptor(context.getConfiguration());
            descriptor.checkValidPendingTableName(context.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE));
            variants = 0;
            readyVariants = 0;
            pendingVariants = 0;
            pendingEvaluator = descriptor.getPendingEvaluatorMapper(
                    new VariantStorageMetadataManager(
                            new HBaseVariantStorageMetadataDBAdaptorFactory(
                                    new VariantTableHelper(context.getConfiguration()))));
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

            Counter counter = context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants");
            synchronized (counter) {
                counter.increment(variants);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "ready_variants").increment(readyVariants);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "pending_variants").increment(pendingVariants);
            }
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
