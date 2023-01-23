package org.opencb.opencga.storage.hadoop.variant.mr;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Statement;
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

    @Override
    public RecordReader<NullWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context, configuration);
        @SuppressWarnings("unchecked") final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);

        PhoenixRecordReader<T> phoenixRecordReader;
        try {
            // hdp2.6
            Constructor<PhoenixRecordReader> constructor = PhoenixRecordReader.class
                    .getConstructor(Class.class, Configuration.class, QueryPlan.class, MapReduceParallelScanGrouper.class);
            constructor.setAccessible(true);
            phoenixRecordReader = constructor.newInstance(inputClass, configuration, queryPlan, MapReduceParallelScanGrouper.getInstance());
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        } catch (NoSuchMethodException ignore) {
            // Search other constructor
            try {
                // emg5.31
                Constructor<PhoenixRecordReader> constructor = PhoenixRecordReader.class
                        .getConstructor(Class.class, Configuration.class, QueryPlan.class);
                constructor.setAccessible(true);
                phoenixRecordReader = constructor.newInstance(inputClass, configuration, queryPlan);
            } catch (InstantiationException | InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
                throw new IOException(e);
            }
        }

        return new CloseValueRecordReader<>(phoenixRecordReader);
    }

    public static class CloseValueRecordReader<K, V> extends TransformInputFormat.RecordReaderTransform<K, V, V> {

        public CloseValueRecordReader(RecordReader<K, V> recordReader) {
            super(recordReader, v -> v);
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
        final List<InputSplit> splits = generateSplits(queryPlan, allSplits);
        return splits;
    }

    private List<InputSplit> generateSplits(final QueryPlan qplan, final List<KeyRange> splits) throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
        for (List<Scan> scans : qplan.getScans()) {
            psplits.add(new PhoenixInputSplit(scans));
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
