package org.opencb.opencga.storage.hadoop.variant.mr;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.opencb.opencga.storage.hadoop.HBaseCompat;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created on 23/11/17.
 * FIXME: Most of the code here is copied from {@link org.apache.phoenix.mapreduce.PhoenixInputFormat}
 *        The only modification to the original code is disable transactions, as it produces compatibility
 *        issues with tephra guava that should be solved in the opencga-storage-hadoop-deps
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CustomPhoenixInputFormat<T extends DBWritable> extends InputFormat<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(CustomPhoenixInputFormat.class);
    private static Logger logger = LoggerFactory.getLogger(CustomPhoenixInputFormat.class);

    @Override
    public RecordReader<NullWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context, configuration);
        @SuppressWarnings("unchecked") final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);

        PhoenixRecordReader<T> phoenixRecordReader = HBaseCompat.getInstance()
                .getPhoenixCompat().newPhoenixRecordReader(inputClass, configuration, queryPlan);
        return new CloseValueRecordReader<>(phoenixRecordReader);
    }

    public static class CloseValueRecordReader<K, V> extends TransformInputFormat.RecordReaderTransform<K, V, V> {

        public CloseValueRecordReader(RecordReader<K, V> recordReader) {
            super(recordReader, v -> v);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(split, context);
            if (split instanceof PhoenixInputSplit) {
                PhoenixInputSplit phoenixInputSplit = (PhoenixInputSplit) split;
                KeyRange keyRange = phoenixInputSplit.getKeyRange();
                logger.info("Key range : " + keyRange);

                try {
                    Pair<String, Integer> chrPosStart = VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey(keyRange.getLowerRange());
                    Pair<String, Integer> chrPosEnd = VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey(keyRange.getUpperRange());
                    logger.info("Variants key range : "
                            + (keyRange.isLowerInclusive() ? "[" : "(")
                            + chrPosStart.getFirst() + ":" + chrPosStart.getSecond()
                            + " - "
                            + chrPosEnd.getFirst() + ":" + chrPosEnd.getSecond()
                            + (keyRange.isUpperInclusive() ? "]" : ")"));
                } catch (Exception e) {
                    logger.error("Error parsing key range: {}", e.getMessage());
                }

                logger.info("Split: " + phoenixInputSplit.getScans().size() + " scans");
                int i = 0;
                for (Scan scan : phoenixInputSplit.getScans()) {
                    logger.info("[{}] Scan: {}", ++i, scan);
                }
            }
        }

        @Override
        public void close() throws IOException {
            V currentValue;
            try {
                currentValue = getCurrentValue();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            if (currentValue instanceof Closeable) {
                ((Closeable) currentValue).close();
            }
            super.close();
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context, configuration);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(queryPlan, allSplits, configuration);
        return splits;
    }

    private List<InputSplit> generateSplits(final QueryPlan qplan, final List<KeyRange> splits, Configuration configuration)
            throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
        for (List<Scan> scans : qplan.getScans()) {
            if (scans.size() == 1) {
                // Split scans into multiple smaller scans
                int numScans = configuration.getInt(HadoopVariantStorageOptions.MR_HBASE_PHOENIX_SCAN_SPLIT.key(),
                        HadoopVariantStorageOptions.MR_HBASE_PHOENIX_SCAN_SPLIT.defaultValue());
                List<Scan> splitScans = new ArrayList<>(numScans);
                Scan scan = scans.get(0);
                byte[] startRow = scan.getStartRow();
                if (startRow == null || startRow.length == 0) {
                    startRow = Bytes.toBytesBinary("1\\x00\\x00\\x00\\x00\\x00");
                    logger.info("Scan with empty startRow. Set default start. "
                            + "[" + Bytes.toStringBinary(startRow) + "-" + Bytes.toStringBinary(scan.getStopRow()) + ")");
                }
                byte[] stopRow = scan.getStopRow();
                if (stopRow == null || stopRow.length == 0) {
                    stopRow = Bytes.toBytesBinary("Z\\x00\\x00\\x00\\x00\\x00");
                    logger.info("Scan with empty stopRow. Set default stop. "
                            + "[" + Bytes.toStringBinary(startRow) + "-" + Bytes.toStringBinary(stopRow) + ")");
                }
                byte[][] ranges = Bytes.split(startRow, stopRow, numScans - 1);
                for (int i = 1; i < ranges.length; i++) {
                    Scan splitScan = new Scan(scan);
                    splitScan.withStartRow(ranges[i - 1]);
                    splitScan.withStopRow(ranges[i], false);
                    splitScans.add(splitScan);
                }
                for (Scan splitScan : splitScans) {
                    psplits.add(new PhoenixInputSplit(Collections.singletonList(splitScan)));
                }
            } else {
                psplits.add(new PhoenixInputSplit(scans));
            }
        }
        return psplits;
    }

    /**
     * Returns the query plan associated with the select query.
     *
     * @param context context
     * @return QueryPlan
     */
    private QueryPlan getQueryPlan(final JobContext context, final Configuration configuration) {
        Preconditions.checkNotNull(context);
        try {
            final String currentScnValue = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            final Properties overridingProps = new Properties();
            if (currentScnValue != null) {
                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
            }
            overridingProps.put(QueryServices.TRANSACTIONS_ENABLED, "false");
            final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
            final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
            Preconditions.checkNotNull(selectStatement);
            final Statement statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);

            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        } catch (Exception exception) {
            LOG.error(String.format("Failed to get the query plan with error [%s]",
                    exception.getMessage()));
            throw Throwables.propagate(exception);
        }
    }

}
