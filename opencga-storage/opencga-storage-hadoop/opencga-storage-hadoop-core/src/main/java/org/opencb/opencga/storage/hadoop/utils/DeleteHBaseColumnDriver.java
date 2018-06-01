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
import org.apache.hadoop.hbase.util.Pair;
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
    public static final String DELETE_ALL_COLUMNS = "delete_all_columns";
    public static final String REGIONS_TO_DELETE = "regions_to_delete";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHBaseColumnDriver.class);
    // This separator is not valid at Bytes.toStringBinary
    private static final String REGION_SEPARATOR = "\\\\";

    private List<String> columns;
    private List<Pair<byte[], byte[]>> regions;

    public void setupJob(Job job, String table) throws IOException {
        job.getConfiguration().setStrings(COLUMNS_TO_DELETE, columns.toArray(new String[columns.size()]));
        // There is a maximum number of counters
        job.getConfiguration().setStrings(COLUMNS_TO_COUNT, columns.subList(0, Math.min(columns.size(), 80))
                .toArray(new String[columns.size()]));

        Scan templateScan = new Scan();
        int caching = job.getConfiguration().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 100);
        LOGGER.info("Scan set Caching to " + caching);
        templateScan.setCaching(caching);        // 1 is the default in Scan
        templateScan.setCacheBlocks(false);  // don't set to true for MR jobs
        templateScan.setFilter(new KeyOnlyFilter());
        for (String column : columns) {
            String[] split = column.split(":");
            templateScan.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        }

        List<Scan> scans;
        if (!regions.isEmpty()) {
            scans = new ArrayList<>(regions.size() / 2);
            for (Pair<byte[], byte[]> region : regions) {
                Scan scan = new Scan(templateScan);
                scans.add(scan);
                if (region.getFirst() != null && region.getFirst().length != 0) {
                    scan.setStartRow(region.getFirst());
                }
                if (region.getSecond() != null && region.getSecond().length != 0) {
                    scan.setStopRow(region.getSecond());
                }
            }
        } else {
            scans = Collections.singletonList(templateScan);
        }

        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, table, table, scans, DeleteHBaseColumnMapper.class);
    }

    @Override
    protected void parseAndValidateArgs(String[] args) {
        columns = getColumnsToDelete(getConf());

        if (columns.isEmpty()) {
            if (getConf().getBoolean(DELETE_ALL_COLUMNS, false)) {
                LOGGER.info("Delete ALL columns");
            } else {
                throw new IllegalArgumentException("No columns specified!");
            }
        }
        for (String column : columns) {
            if (!column.contains(":")) {
                throw new IllegalArgumentException("Malformed column '" + column + "'. Requires <family>:<column>");
            }
        }

        regions = new ArrayList<>();
        String regionsStr = getConf().get(REGIONS_TO_DELETE);
        if (regionsStr != null && !regionsStr.isEmpty()) {
            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(regionsStr, REGION_SEPARATOR);
            if (split.length % 2 != 0) {
                throw new IllegalArgumentException("Expected pair number of elements in region.  Got " + split.length);
            }
            for (int i = 0; i < split.length; i += 2) {
                regions.add(new Pair<>(Bytes.toBytesBinary(split[i]), Bytes.toBytesBinary(split[i + 1])));
            }
        }
    }

    private static List<String> getColumnsToDelete(Configuration conf) {
        List<String> columns;
        String[] columnStrings = conf.getStrings(COLUMNS_TO_DELETE);
        if (columnStrings == null) {
            columns = Collections.emptyList();
        } else {
            columns = Arrays.asList(columnStrings);
        }
        return columns;
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

    public static String[] buildArgs(String table, List<String> columns, ObjectMap options) {
        return buildArgs(table, columns, false, null, options);
    }

    public static String[] buildArgs(String table, List<String> columns, boolean deleteAllColumns, List<Pair<byte[], byte[]>> regions,
                                     ObjectMap options) {
        ObjectMap args = new ObjectMap();
        if (deleteAllColumns) {
            args.append(DELETE_ALL_COLUMNS, true);
        } else {
            args.append(COLUMNS_TO_DELETE, String.join(",", columns));
        }

        if (regions != null) {
            StringBuilder sb = new StringBuilder();
            Iterator<Pair<byte[], byte[]>> iterator = regions.iterator();
            while (iterator.hasNext()) {
                Pair<byte[], byte[]> region = iterator.next();
                String start = Bytes.toStringBinary(region.getFirst());
                String end = Bytes.toStringBinary(region.getSecond());

                sb.append(start);
                sb.append(REGION_SEPARATOR);
                sb.append(end);
                if (iterator.hasNext()) {
                    sb.append(REGION_SEPARATOR);
                }
            }
            args.append(REGIONS_TO_DELETE, sb.toString());
        }

        args.putAll(options);

        return buildArgs(table, args);
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
        private boolean deleteAllColumns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            deleteAllColumns = context.getConfiguration().getBoolean(DELETE_ALL_COLUMNS, false);
            columnsToCount = new HashSet<>(context.getConfiguration().get(COLUMNS_TO_COUNT) == null
                    ? Collections.emptyList()
                    : Arrays.asList(context.getConfiguration().getStrings(COLUMNS_TO_COUNT)));
            columnsToDelete = new HashSet<>(getColumnsToDelete(context.getConfiguration()));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            Delete delete = new Delete(result.getRow());
            if (deleteAllColumns) {
                context.getCounter("DeleteColumn", "delete").increment(1);
                context.write(key, delete);
            } else {
                for (Cell cell : result.rawCells()) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualifier = CellUtil.cloneQualifier(cell);
                    String c = Bytes.toString(family) + ':' + Bytes.toString(qualifier);
                    if (columnsToDelete.contains(c)) {
                        delete.addColumn(family, qualifier);
                        if (columnsToCount.contains(c)) {
                            context.getCounter("DeleteColumn", c).increment(1);
                        }
                    }
                }
                if (!delete.isEmpty()) {
                    context.getCounter("DeleteColumn", "delete").increment(1);
                    context.write(key, delete);
                }
            }
        }
    }

}
