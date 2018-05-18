package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created on 19/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DeleteHBaseColumnDriver extends AbstractHBaseDriver {

    public static final String COLUMNS_TO_DELETE = "columns_to_delete";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHBaseColumnDriver.class);
    public static final int FIXED_SIZE_ARGS = 2;

    private List<String> columns;

    public void setupJob(Job job, String table) throws IOException {
        job.getConfiguration().setStrings(COLUMNS_TO_DELETE, columns.toArray(new String[columns.size()]));
        // There is a maximum number of counters
        job.getConfiguration().setStrings(COLUMNS_TO_COUNT, columns.subList(0, Math.min(columns.size(), 80))
                .toArray(new String[columns.size()]));

        Scan scan = new Scan();
        int caching = job.getConfiguration().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 100);
        LOGGER.info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.setFilter(new KeyOnlyFilter());
        for (String column : columns) {
            String[] split = column.split(":");
            scan.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        }

        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, table, table, scan, DeleteHBaseColumnMapper.class);
    }

    @Override
    protected void parseAndValidateArgs(String[] args) {
        columns = Arrays.asList(args[1].split(","));

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("No columns specified!");
        }
        for (String column : columns) {
            if (!column.contains(":")) {
                throw new IllegalArgumentException("Malformed column '" + column + "'. Requires <family>:<column>");
            }
        }
    }

    @Override
    protected String getJobName() {
        return "opencga: delete columns from table '" + table + '\'';
    }

    @Override
    protected String getUsage() {
        return "Usage: " + getClass().getSimpleName()
                + " [generic options] <table> <family_1>:<column_1>(,<family_n>:<column_n>)* (<key> <value>)*";
    }

    @Override
    protected int getFixedSizeArgs() {
        return FIXED_SIZE_ARGS;
    }

    public static String[] buildArgs(String table, List<String> columns, ObjectMap options) {
        List<String> args = new ArrayList<>(FIXED_SIZE_ARGS + options.size() * 2);

        args.add(table);
        args.add(String.join(",", columns));
        for (String key : options.keySet()) {
            String value = options.getString(key);
            if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
                args.add(key);
                args.add(value);
            }
        }
        return args.toArray(new String[args.size()]);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new DeleteHBaseColumnDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + DeleteHBaseColumnDriver.class, e);
            System.exit(1);
        }
    }

    public int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf != null) {
            setConf(conf);
        }
        return ToolRunner.run(this, args);
    }

    public static class DeleteHBaseColumnMapper extends TableMapper<ImmutableBytesWritable, Delete> {

        private Set<String> columnsToCount;
        private Set<String> columnsToDelete;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            columnsToCount = new HashSet<>(Arrays.asList(context.getConfiguration().getStrings(COLUMNS_TO_COUNT)));
            columnsToDelete = new HashSet<>(Arrays.asList(context.getConfiguration().getStrings(COLUMNS_TO_DELETE)));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            for (Cell cell : result.rawCells()) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                String c = Bytes.toString(family) + ':' + Bytes.toString(qualifier);
                if (columnsToDelete.contains(c)) {
                    context.write(key, new Delete(result.getRow()).addColumn(family, qualifier));
                    if (columnsToCount.contains(c)) {
                        context.getCounter("DeleteColumn", c).increment(1);
                    }
                }
            }
        }
    }

}
