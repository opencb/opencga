package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
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
public class DeleteHBaseColumnDriver extends Configured implements Tool {

    public static final String COLUMNS_TO_DELETE = "columns_to_delete";
    public static final String COLUMNS_TO_COUNT = "columns_to_count";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHBaseColumnDriver.class);
    private String table;
    private List<String> columns;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HBaseConfiguration.addHbaseResources(conf);
        getConf().setClassLoader(DeleteHBaseColumnDriver.class.getClassLoader());
        configFromArgs(args);

//        int maxKeyValueSize = conf.getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_KEYVALUE_SIZE_MAX, 10485760); // 10MB
//        logger.info("HBASE: set " + ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY + " to " + maxKeyValueSize);
//        conf.setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, maxKeyValueSize); // always overwrite server default (usually 1MB)

        /* -------------------------------*/
        // JOB setup
        Job job = Job.getInstance(getConf(), "opencga: delete columns from table '" + table + '\'');
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(DeleteHBaseColumnMapper.class);    // class that contains mapper

//        // Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions
//        // See opencb/opencga#352 for more info.
//        int scannerTimeout = getConf().getInt(MAPREDUCE_HBASE_SCANNER_TIMEOUT,
//                getConf().getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
//        logger.info("Set Scanner timeout to " + scannerTimeout + " ...");
//        job.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, scannerTimeout);

        setupJob(job, table, columns);

        boolean succeed = executeJob(job);
        if (!succeed) {
            LOGGER.error("error with job!");
        }

        return succeed ? 0 : 1;
    }

    public static void setupJob(Job job, String table, List<String> columns) throws IOException {
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

    private void configFromArgs(String[] args) {
        int fixedSizeArgs = 2;

        System.out.println(Arrays.toString(args));
        if (args.length < fixedSizeArgs || (args.length - fixedSizeArgs) % 2 != 0) {
            System.err.println("Usage: " + getClass().getSimpleName()
                    + " [generic options] <table> <family_1>:<column_1>(,<family_n>:<column_n>)* (<key> <value>)*");
            System.err.println("Found " + Arrays.toString(args));
            ToolRunner.printGenericCommandUsage(System.err);
            throw new IllegalArgumentException("Wrong number of arguments!");
        }

        // Get first other args to avoid overwrite the fixed position args.
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }

        table = args[0];
        columns = Arrays.asList(args[1].split(","));

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("No table specified!");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("No columns specified!");
        }
        for (String column : columns) {
            if (!column.contains(":")) {
                throw new IllegalArgumentException("Malformed column '" + column + "'. Requires <family>:<column>");
            }
        }
    }

    public static String[] buildArgs(String table, List<String> columns, ObjectMap options) {
        List<String> args = new ArrayList<>(1 + columns.size() + options.size() * 2);

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

    private boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
//                onError();
            } catch (IOException e) {
                LOGGER.error("Error", e);
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);
        return succeed;
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

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            columnsToCount = new HashSet<>(Arrays.asList(context.getConfiguration().getStrings(COLUMNS_TO_COUNT)));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            for (Cell cell : result.rawCells()) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                context.write(key, new Delete(result.getRow()).addColumn(family, qualifier));
                String c = Bytes.toString(family) + ":" + Bytes.toString(qualifier);
                if (columnsToCount.contains(c)) {
                    context.getCounter("DeleteColumn", c).increment(1);
                }
            }
        }
    }

}
