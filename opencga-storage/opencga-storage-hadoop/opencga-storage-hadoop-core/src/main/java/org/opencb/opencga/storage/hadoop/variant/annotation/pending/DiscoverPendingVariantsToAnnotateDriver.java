package org.opencb.opencga.storage.hadoop.variant.annotation.pending;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultithreadedTableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.VariantColumn.SO;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.VariantColumn.TYPE;
import static org.opencb.opencga.storage.hadoop.variant.annotation.pending.PendingVariantsToAnnotateUtils.*;

/**
 * Created on 12/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DiscoverPendingVariantsToAnnotateDriver extends AbstractVariantsTableDriver {

    private final Logger logger = LoggerFactory.getLogger(DiscoverPendingVariantsToAnnotateDriver.class);

    @Override
    protected Class<DiscoverVariantsToAnnotateMapper> getMapperClass() {
        return DiscoverVariantsToAnnotateMapper.class;
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        super.preExecution(variantTable);

        HBaseManager hBaseManager = getHBaseManager();
        PendingVariantsToAnnotateUtils.createTableIfNeeded(getTableNameGenerator().getPendingAnnotationTableName(), hBaseManager);
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

        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)) {
            Region region = new Region(query.getString(VariantQueryParam.REGION.key()));
            VariantHBaseQueryParser.addRegionFilter(scan, region);
        }

        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, TYPE.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SO.bytes());

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
        VariantMapReduceUtil.initTableMapperJob(job, variantTable,
                getTableNameGenerator().getPendingAnnotationTableName(), scan, mapperClass);


        VariantMapReduceUtil.setNoneReduce(job);


        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "discover_variants_to_annotate";
    }


    public static class DiscoverVariantsToAnnotateMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        public static final byte[] SO_BYTES = SO.bytes();
        private int variants;
        private int annotatedVariants;
        private int pendingVariants;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            HBaseVariantTableNameGenerator
                    .checkValidPendingAnnotationTableName(context.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE));
            variants = 0;
            annotatedVariants = 0;
            pendingVariants = 0;
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            boolean annotated = false;
            for (Cell cell : value.rawCells()) {
                if (cell.getValueLength() > 0) {
                    if (Bytes.equals(
                            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            SO_BYTES, 0, SO_BYTES.length)) {
                        annotated = true;
                        break;
                    }
                }
            }

            variants++;
            if (annotated) {
                annotatedVariants++;
                Delete delete = new Delete(value.getRow());
                context.write(key, delete);
            } else {
                pendingVariants++;
                Put put = new Put(value.getRow());
                put.addColumn(FAMILY, COLUMN, VALUE);
                context.write(key, put);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            Counter counter = context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants");
            synchronized (counter) {
                counter.increment(variants);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "annotated_variants").increment(annotatedVariants);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "pending_variants").increment(pendingVariants);
            }
        }
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = ToolRunner.run(new DiscoverPendingVariantsToAnnotateDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

}
