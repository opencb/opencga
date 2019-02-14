package org.opencb.opencga.storage.hadoop.variant.annotation.pending;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
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
        createColumnFamilyIfNeeded(variantTable, hBaseManager);
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

        scan.addColumn(getHelper().getColumnFamily(), TYPE.bytes());
        scan.addColumn(getHelper().getColumnFamily(), SO.bytes());

        int caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);


        scan.setCaching(caching);
        scan.setCacheBlocks(false);
        logger.info("Set scan caching to " + caching);

        VariantMapReduceUtil.initTableMapperJob(job, variantTable, variantTable, scan, getMapperClass());
        VariantMapReduceUtil.setNoneReduce(job);


        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "discover_variants_to_annotate";
    }


    public static class DiscoverVariantsToAnnotateMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            boolean annotated = false;
            for (Cell cell : value.rawCells()) {
                if (cell.getValueLength() > 0) {
                    if (Bytes.equals(
                            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            SO.bytes(), 0, SO.bytes().length)) {
                        annotated = true;
                        break;
                    }
                }
            }

            if (annotated) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "annotated_variants").increment(1);
                Delete delete = new Delete(value.getRow());
                delete.addFamily(FAMILY);
                context.write(key, delete);
            } else {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "pending_variants").increment(1);
                Put put = new Put(value.getRow());
                put.addColumn(FAMILY, COLUMN, VALUE);
                context.write(key, put);
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
