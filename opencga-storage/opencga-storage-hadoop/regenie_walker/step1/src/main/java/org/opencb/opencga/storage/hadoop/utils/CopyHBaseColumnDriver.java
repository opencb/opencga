package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CopyHBaseColumnDriver extends AbstractHBaseDriver {

    public static final String COLUMNS_TO_COPY = "columnsToCopy";
    public static final String COLUMNS_TO_INCLUDE = "columnsToInclude";
    public static final String DELETE_AFTER_COPY = "deleteAfterCopy";
    private List<String> columnsToInclude;
    private Map<String, String> columnsToCopyMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyHBaseColumnDriver.class);

    public CopyHBaseColumnDriver() {
    }

    public CopyHBaseColumnDriver(Configuration conf) {
        if (conf != null) {
            setConf(conf);
        }
    }

    @Override
    protected String getJobName() {
        return "opencga: copy columns from table '" + table + '\'';
    }

    @Override
    protected void setupJob(Job job, String table) throws IOException {
        Scan scan = new Scan();
        int caching = job.getConfiguration().getInt(HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.key(), 10);

        LOGGER.info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        for (String column : columnsToCopyMap.keySet()) {
            String[] split = column.split(":");
            scan.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        }
        for (String column : columnsToInclude) {
            String[] split = column.split(":");
            scan.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        }
        LOGGER.info("Scan " + scan.toString(50));

        // There is a maximum number of counters
        int newSize = Math.min(columnsToCopyMap.size(), 50);
        job.getConfiguration().setStrings(COLUMNS_TO_COUNT, new ArrayList<>(columnsToCopyMap.keySet())
                .subList(0, newSize).toArray(new String[newSize]));

        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, table, table, scan, CopyHBaseColumnMapper.class);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        columnsToCopyMap = getColumnsToCopy(getConf());
        String param = getParam(COLUMNS_TO_INCLUDE, "");
        if (param.isEmpty()) {
            columnsToInclude = Collections.emptyList();
        } else {
            columnsToInclude = Arrays.asList(param.split(","));
        }
    }

    public static String[] buildArgs(String table, Map<String, String> columnsToCopyMap, List<String> columnsToInclude, ObjectMap options) {
        options = options == null ? new ObjectMap() : new ObjectMap(options);
        if (columnsToCopyMap == null || columnsToCopyMap.isEmpty()) {
            throw new IllegalArgumentException("Invalid empty ColumnsToCopy");
        }
        if (columnsToInclude != null) {
            options.put(COLUMNS_TO_INCLUDE, String.join(",", columnsToInclude));
        }
        options.put(COLUMNS_TO_COPY, columnsToCopyMap.entrySet()
                .stream()
                .map(entry -> entry.getKey() + '=' + entry.getValue())
                .collect(Collectors.joining(",")));
        return AbstractHBaseDriver.buildArgs(table, options);
    }

    private static Map<String, String> getColumnsToCopy(Configuration conf) {
        final String malformedColumnsToCopyMessage = "Invalid list of src=target columns to copy.";

        String columnsToCopy = conf.get(COLUMNS_TO_COPY);
        Map<String, String> columnsToCopyMap = new HashMap<>();
        for (String pair : columnsToCopy.split(",")) {
            String[] split = pair.split("=");
            if (split.length != 2) {
                throw new IllegalArgumentException(malformedColumnsToCopyMessage
                        + " Missing target. '" + pair + '\'');
            }
            String src = split[0];
            String dest = split[1];
            if (!src.contains(":") || !dest.contains(":")) {
                throw new IllegalArgumentException(malformedColumnsToCopyMessage
                        + " Missing family. '" + pair + '\'');
            }
            if (src.equals(dest)) {
                throw new IllegalArgumentException(malformedColumnsToCopyMessage
                        + " Source and target can not be the same. '" + pair + '\'');
            }
            columnsToCopyMap.put(src, dest);
        }
        if (columnsToCopyMap.isEmpty()) {
            throw new IllegalArgumentException(malformedColumnsToCopyMessage + " Empty.");
        }
        return columnsToCopyMap;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }

    public static class CopyHBaseColumnMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        private Map<String, String> columnsToCopy;
        private Set<String> columnsToCount;
        private boolean deleteAfterCopy;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            columnsToCopy = getColumnsToCopy(context.getConfiguration());
            deleteAfterCopy = context.getConfiguration().getBoolean(DELETE_AFTER_COPY, false);
            columnsToCount = new HashSet<>(Arrays.asList(context.getConfiguration().getStrings(COLUMNS_TO_COUNT)));

        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_NAME, "results").increment(1);
            boolean anyCopy = false;
            for (Cell cell : result.rawCells()) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                String c = Bytes.toString(family) + ":" + Bytes.toString(qualifier);
                String target = columnsToCopy.get(c);
                if (target != null) {
                    anyCopy = true;
                    String[] split = target.split(":", 2);

                    context.write(key, new Put(result.getRow())
                            .addColumn(Bytes.toBytes(split[0]), ByteBuffer.wrap(Bytes.toBytes(split[1])), HConstants.LATEST_TIMESTAMP,
                                    CellUtil.getValueBufferShallowCopy(cell)));
                    context.getCounter(COUNTER_GROUP_NAME, "put").increment(1);

                    if (deleteAfterCopy) {
                        context.getCounter(COUNTER_GROUP_NAME, "delete").increment(1);
                        context.write(key, new Delete(result.getRow()).addColumns(family, qualifier));
                    }
                    if (columnsToCount.contains(c)) {
                        context.getCounter("CopyColumn", c).increment(1);
                    }
                } else {
                    context.getCounter(COUNTER_GROUP_NAME, "skip_column").increment(1);
                }
            }
            if (anyCopy) {
                context.getCounter(COUNTER_GROUP_NAME, "copy_row").increment(1);
            } else {
                context.getCounter(COUNTER_GROUP_NAME, "skip_row").increment(1);
            }

        }
    }

}
